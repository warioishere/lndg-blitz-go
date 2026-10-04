package rebalancer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// Probe constants.
const (
	probeSteps     = 5
	minProbeAmount = 69420
)

// sourceLightningClient is the LightningClient subset required by trySingleSource.
type sourceLightningClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error)
	AddInvoice(ctx context.Context, in *lnrpc.Invoice, opts ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error)
}

// sourceRouterClient is the RouterClient subset required for probing and sending.
type sourceRouterClient interface {
	BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error)
	SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error)
}

// sourceQuerier bundles the DB access required by updateRoute and updateNodeReputations.
type sourceQuerier interface {
	routeQuerier
	reputationQuerier
}

// engine holds rebalancer-wide caches and is the receiver for probing and send methods.
type engine struct {
	mc       *missionControl
	alias    *aliasCache
	selfPK   *selfPubkeyCache
	chanInfo *chanInfoCache
}

func newEngine() *engine {
	return &engine{
		mc:       newMissionControl(),
		alias:    newAliasCache(),
		selfPK:   newSelfPubkeyCache(),
		chanInfo: newChanInfoCache(),
	}
}

// rebalanceResult carries the outcome of a successful trySingleSource attempt.
// scaledFeeLimit is set only when a probe fallback succeeds.
type rebalanceResult struct {
	status         int32
	feesPaid       float64
	successfulOut  uint64
	successfulIn   uint64
	paymentHash    string
	value          int64
	scaledFeeLimit *float64
}

// probeRouteAmount performs a binary search for the maximum routable amount using
// fake payments. Returns satoshis, or 0 if the best found value is below minProbeAmount.
func probeRouteAmount(ctx context.Context, routerstub sourceRouterClient, hopKeys [][]byte, outgoingChanID string, cltvDelta int32, originalAmount int64) int64 {
	var good int64
	bad := originalAmount
search:
	for step := 0; step < probeSteps; step++ {
		probe := (good + bad) / 2
		gap := good / 20
		if gap < 1000 {
			gap = 1000
		}
		if probe < minProbeAmount || bad-good < gap {
			break
		}
		outChanID, perr := strconv.ParseUint(outgoingChanID, 10, 64)
		if perr != nil {
			break
		}
		build, err := routerstub.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
			OutgoingChanId: outChanID,
			AmtMsat:        probe * 1000,
			HopPubkeys:     hopKeys,
			FinalCltvDelta: cltvDelta,
		})
		if err != nil {
			break
		}
		fakeHash := make([]byte, 32)
		if _, e := rand.Read(fakeHash); e != nil {
			break
		}
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		result, serr := routerstub.SendToRouteV2(cctx, &routerrpc.SendToRouteRequest{PaymentHash: fakeHash, Route: build.GetRoute()})
		cancel()
		if serr != nil {
			break
		}
		failure := result.GetFailure()
		if failure == nil {
			break
		}
		switch int(failure.GetCode()) {
		case 1: // INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS -> can succeed
			good = probe
		case 15: // TEMPORARY_CHANNEL_FAILURE -> too much
			bad = probe
		case 12: // FEE_INSUFFICIENT -> retry (don't update bounds)
			continue
		default:
			break search
		}
	}
	if good >= minProbeAmount {
		return good
	}
	return 0
}

// failureLogParts returns the failure code name and failure source index as strings.
// When no failure object is present, both values are "None".
func failureLogParts(failureSet bool, codeNum, fsi int) (string, string) {
	if !failureSet {
		return "None", "None"
	}
	return failureCodeName(codeNum), strconv.Itoa(fsi)
}

// trySingleSource attempts a rebalance via a single outbound source channel.
// Returns a rebalanceResult on success, nil otherwise.
// A GetInfo error (fetching self pubkey) is propagated.
func (e *engine) trySingleSource(
	ctx context.Context,
	stub sourceLightningClient,
	routerstub sourceRouterClient,
	q sourceQuerier,
	rebalance db.GuiRebalancer,
	sourceChanID string,
	sourceFeeMap map[string]int,
	targetFeeRate, arMaxCost, maxFeeRate int,
	invoiceResponse *lnrpc.AddInvoiceResponse,
	feeLimitMsat int64,
	timeout time.Duration,
	now time.Time,
) (*rebalanceResult, error) {
	srcFee := lookupSourceFee(sourceFeeMap, sourceChanID)
	// Per-source budget: how many ppm of route fee are affordable.
	perSourceRoutePpm := int(float64(targetFeeRate)*(float64(arMaxCost)/100)) - srcFee
	if maxFeeRate < perSourceRoutePpm {
		perSourceRoutePpm = maxFeeRate
	}
	if perSourceRoutePpm <= 0 {
		return nil, nil
	}
	perSourceFeeLimitSat := int64(float64(perSourceRoutePpm) * float64(rebalance.Value) / 1_000_000)
	if perSourceFeeLimitSat <= 0 {
		return nil, nil
	}
	srcLabel := e.alias.label(sourceChanID)

	// QueryRoutes as a self-payment rebalance: destination is ourselves, last_hop is the target peer.
	selfPubkey, err := e.selfPK.get(ctx, stub)
	if err != nil {
		return nil, err
	}
	outChanID, perr := strconv.ParseUint(sourceChanID, 10, 64)
	lastHopBytes, herr := hex.DecodeString(rebalance.LastHopPubkey)
	if perr != nil || herr != nil {
		e2 := perr
		if e2 == nil {
			e2 = herr
		}
		rebalLog(fmt.Sprintf("QueryRoutes via %s failed: %s", srcLabel, e2))
		return nil, nil
	}
	qr, qerr := stub.QueryRoutes(ctx, &lnrpc.QueryRoutesRequest{
		PubKey:            selfPubkey,
		Amt:               int64(rebalance.Value),
		OutgoingChanIds:   []uint64{outChanID},
		LastHopPubkey:     lastHopBytes,
		FeeLimit:          &lnrpc.FeeLimit{Limit: &lnrpc.FeeLimit_Fixed{Fixed: perSourceFeeLimitSat}},
		UseMissionControl: true,
	})
	if qerr != nil {
		rebalLog(fmt.Sprintf("QueryRoutes via %s failed: %s", srcLabel, qerr))
		return nil, nil
	}
	if qr == nil || len(qr.GetRoutes()) == 0 {
		return nil, nil
	}

	for _, queryRoute := range qr.GetRoutes() {
		if len(queryRoute.GetHops()) == 0 {
			continue
		}
		// Skip routes that reuse a cached failed (prev -> next) edge at a similar amount.
		if !e.mc.validateRoute(queryRoute) {
			rebalLog(fmt.Sprintf("Route via %s skipped — reuses a cached failed edge", srcLabel))
			continue
		}
		// Decode hop pubkeys for BuildRoute.
		hops := queryRoute.GetHops()
		hopKeys := make([][]byte, 0, len(hops))
		for _, h := range hops {
			b, decErr := hex.DecodeString(h.GetPubKey())
			if decErr != nil {
				return nil, decErr
			}
			hopKeys = append(hopKeys, b)
		}
		var cltvDelta int32
		if len(hops) >= 2 {
			cltvDelta = int32(hops[len(hops)-1].GetExpiry()) - int32(hops[len(hops)-2].GetExpiry())
		} else {
			cltvDelta = 144
		}

		// BuildRoute with payment_addr so the payment can actually be sent over this route.
		build, berr := routerstub.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
			OutgoingChanId: outChanID,
			AmtMsat:        int64(rebalance.Value) * 1000,
			HopPubkeys:     hopKeys,
			FinalCltvDelta: cltvDelta,
			PaymentAddr:    invoiceResponse.GetPaymentAddr(),
		})
		if berr != nil {
			rebalLog(fmt.Sprintf("BuildRoute via %s failed: %s", srcLabel, berr))
			continue
		}

		// Opportunity-cost check (total_fees_msat already includes inbound discounts).
		allowed, routeFeePpm, maxRoutePpm := checkOpportunityCost(
			build.GetRoute().GetTotalFeesMsat(), int(rebalance.Value),
			srcFee, targetFeeRate, arMaxCost, maxFeeRate,
		)
		if !allowed {
			rebalLog(fmt.Sprintf("Route via %s rejected: route %d ppm > max %d ppm (target %d*%d%% - src %d)",
				srcLabel, routeFeePpm, maxRoutePpm, targetFeeRate, arMaxCost, srcFee))
			continue
		}

		// Global fee_limit safety check.
		if build.GetRoute().GetTotalFeesMsat() > feeLimitMsat {
			actualPpm := int((float64(build.GetRoute().GetTotalFeesMsat()) / (float64(rebalance.Value) * 1000)) * 1000000)
			limitPpm := int(float64(feeLimitMsat) / float64(rebalance.Value) * 1000)
			rebalLog(fmt.Sprintf("Route via %s exceeds rebalance fee_limit (%d ppm > %d ppm)", srcLabel, actualPpm, limitPpm))
			continue
		}

		routeMsg := build.GetRoute()
		rebuiltBytes, merr := proto.Marshal(routeMsg)
		if merr != nil {
			return nil, merr
		}
		rebuiltHex := hex.EncodeToString(rebuiltBytes)

		rebalLog(fmt.Sprintf("Sending via %s: %d hops, %d ppm route + %d ppm source",
			srcLabel, len(routeMsg.GetHops()), routeFeePpm, srcFee))

		cctx, cancel := context.WithTimeout(ctx, timeout)
		sendResp, serr := routerstub.SendToRouteV2(cctx, &routerrpc.SendToRouteRequest{
			PaymentHash: invoiceResponse.GetRHash(),
			Route:       routeMsg,
		})
		cancel()
		if serr != nil {
			rebalLog(fmt.Sprintf("SendToRouteV2 via %s error: %s", srcLabel, serr))
			return nil, nil
		}

		if sendResp.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED { // status == 1
			feesMsat := sendResp.GetRoute().GetTotalFeesMsat()
			updateRoute(ctx, q, rebalance.LastHopPubkey, sourceChanID, rebuiltHex, true, false, now)
			updateNodeReputations(ctx, q, rebuiltHex, true, 0, false, now)
			feesPaid := 0.0
			if feesMsat != 0 {
				feesPaid = float64(feesMsat) / 1000
			}
			respHops := sendResp.GetRoute().GetHops()
			return &rebalanceResult{
				status:        2,
				feesPaid:      feesPaid,
				successfulOut: respHops[0].GetChanId(),
				successfulIn:  respHops[len(respHops)-1].GetChanId(),
				paymentHash:   hex.EncodeToString(invoiceResponse.GetRHash()),
				value:         int64(rebalance.Value),
			}, nil
		}

		// Handle failure.
		failure := sendResp.GetFailure()
		failureSet := failure != nil
		var codeNum, fsi int
		if failureSet {
			codeNum = int(failure.GetCode())
			fsi = int(failure.GetFailureSourceIndex())
		}
		reason, fsiStr := failureLogParts(failureSet, codeNum, fsi)
		rebalLog(fmt.Sprintf("Route via %s failed: code=%s hop=%s", srcLabel, reason, fsiStr))
		e.mc.recordRouteFailure(routeMsg, failure)
		updateRoute(ctx, q, rebalance.LastHopPubkey, sourceChanID, rebuiltHex, false, false, now)
		updateNodeReputations(ctx, q, rebuiltHex, false, fsi, failureSet, now)

		// TEMPORARY_CHANNEL_FAILURE at the target channel — try a smaller amount via probe.
		totalHops := len(routeMsg.GetHops())
		if failureSet && codeNum == 15 && fsi == totalHops-2 {
			probed := probeRouteAmount(ctx, routerstub, hopKeys, sourceChanID, cltvDelta, int64(rebalance.Value))
			if probed > 0 {
				rebalLog(fmt.Sprintf("Probe found max %d sats via %s", probed, srcLabel))
				probeInvoice, aerr := stub.AddInvoice(ctx, &lnrpc.Invoice{Value: probed, Expiry: int64(timeout / time.Second)})
				if aerr != nil {
					return nil, aerr
				}
				probeBuild, pberr := routerstub.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
					OutgoingChanId: outChanID,
					AmtMsat:        probed * 1000,
					HopPubkeys:     hopKeys,
					FinalCltvDelta: cltvDelta,
					PaymentAddr:    probeInvoice.GetPaymentAddr(),
				})
				if pberr != nil {
					continue
				}
				pAllowed, pPpm, pMax := checkOpportunityCost(
					probeBuild.GetRoute().GetTotalFeesMsat(), int(probed),
					srcFee, targetFeeRate, arMaxCost, maxFeeRate,
				)
				if !pAllowed {
					rebalLog(fmt.Sprintf("Probed route via %s rejected: %d ppm > %d ppm", srcLabel, pPpm, pMax))
					continue
				}
				pcctx, pcancel := context.WithTimeout(ctx, timeout)
				probeResp, prerr := routerstub.SendToRouteV2(pcctx, &routerrpc.SendToRouteRequest{
					PaymentHash: probeInvoice.GetRHash(),
					Route:       probeBuild.GetRoute(),
				})
				pcancel()
				if prerr != nil {
					continue
				}
				if probeResp.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED {
					feesMsat := probeResp.GetRoute().GetTotalFeesMsat()
					pBytes, pmerr := proto.Marshal(probeBuild.GetRoute())
					if pmerr != nil {
						return nil, pmerr
					}
					probeHex := hex.EncodeToString(pBytes)
					updateRoute(ctx, q, rebalance.LastHopPubkey, sourceChanID, probeHex, true, true, now)
					updateNodeReputations(ctx, q, probeHex, true, 0, false, now)
					// Scale fee_limit so RapidFire children maintain the same ppm.
					scaled := pyround.Round(rebalance.FeeLimit*(float64(probed)/float64(rebalance.Value)), 3)
					feesPaid := 0.0
					if feesMsat != 0 {
						feesPaid = float64(feesMsat) / 1000
					}
					respHops := probeResp.GetRoute().GetHops()
					return &rebalanceResult{
						status:         2,
						feesPaid:       feesPaid,
						successfulOut:  respHops[0].GetChanId(),
						successfulIn:   respHops[len(respHops)-1].GetChanId(),
						paymentHash:    hex.EncodeToString(probeInvoice.GetRHash()),
						value:          probed,
						scaledFeeLimit: &scaled,
					}, nil
				}
			}
		}
		// Route failed; caller should try the next source.
		return nil, nil
	}

	return nil, nil
}
