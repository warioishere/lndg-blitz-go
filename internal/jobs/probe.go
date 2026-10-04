package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// probeLightningClient is the LightningClient subset required by the probe subsystem.
type probeLightningClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error)
}

// probeRouterClient is the RouterClient (routerrpc) subset used for binary search probing.
type probeRouterClient interface {
	SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error)
	BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error)
}

// probeQuerier bundles all DB access required by the probe subsystem.
type probeQuerier interface {
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
	UpsertLocalSetting(ctx context.Context, arg db.UpsertLocalSettingParams) error
	ListOpenAutoRebalanceChannels(ctx context.Context) ([]db.GuiChannel, error)
	ListOutboundCandidates(ctx context.Context) ([]db.ListOutboundCandidatesRow, error)
	ListChanIDsByPubkey(ctx context.Context, remotePubkey string) ([]string, error)
	GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error)
	InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error
	UpdateRebalanceRouteHex(ctx context.Context, arg db.UpdateRebalanceRouteHexParams) error
	InsertProbeLog(ctx context.Context, arg db.InsertProbeLogParams) error
	DeleteOldProbeLogs(ctx context.Context) error
	GetPeerAlias(ctx context.Context, pubkey string) (pgtype.Text, error)
}

// probeTargetDetail holds per-target probe statistics written to the ProbeLog details JSON.
type probeTargetDetail struct {
	Pubkey        string `json:"pubkey"`
	Alias         string `json:"alias"`
	New           int    `json:"new"`
	Existing      int    `json:"existing"`
	Errors        int    `json:"errors"`
	Verified      int    `json:"verified"`
	OutChansTried int    `json:"out_chans_tried"`
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// probeRouteSync sends a probe HTLC with a random payment hash and classifies the result:
//
//	'verified'  — code==1 (INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS) and fsi==len(hops)
//	'liquidity' — code==15 (TEMPORARY_CHANNEL_FAILURE)
//	'fee'       — code==12 (FEE_INSUFFICIENT)
//	'invalid'   — any other failure code
//	'error'     — gRPC error or unexpected status
func probeRouteSync(ctx context.Context, routerstub probeRouterClient, route *lnrpc.Route, timeout time.Duration) string {
	fakeHash := make([]byte, 32)
	if _, err := rand.Read(fakeHash); err != nil {
		return "error"
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := routerstub.SendToRouteV2(cctx, &routerrpc.SendToRouteRequest{PaymentHash: fakeHash, Route: route})
	if err != nil {
		return "error"
	}
	if result.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED { // cannot succeed with a fake hash
		return "error"
	}
	failure := result.GetFailure()
	code := int(failure.GetCode())
	fsi := int(failure.GetFailureSourceIndex())
	nHops := len(route.GetHops())
	if code == 1 && fsi == nHops {
		return "verified"
	}
	if code == 15 {
		return "liquidity"
	}
	if code == 12 {
		return "fee"
	}
	return "invalid"
}

// probeWithBinarySearch finds the maximum amount that can flow over the route
// (source_chan_id, hop_pubkeys) using binary search. Returns (route_hex|nil,
// max_amount_sat, fee_ppm|nil). Search parameters: max_steps=8, gap threshold
// max(good/20, 1000), fee budget scaled proportionally to amount.
func probeWithBinarySearch(ctx context.Context, routerstub probeRouterClient, sourceChanID string, hopPubkeys [][]byte, cltvDelta int32, targetAmountSat, feeLimitSat int64) (*string, int64, *float64) {
	const maxSteps = 8
	var good int64 = 0
	bad := targetAmountSat + 1
	amount := targetAmountSat
	var bestHex *string
	var bestFeePpm *float64
	feeRetried := false

search:
	for step := 0; step < maxSteps; step++ {
		if amount <= 0 {
			break
		}
		outChanID, perr := strconv.ParseUint(sourceChanID, 10, 64)
		if perr != nil {
			break
		}
		bctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		built, berr := routerstub.BuildRoute(bctx, &routerrpc.BuildRouteRequest{
			OutgoingChanId: outChanID,
			AmtMsat:        amount * 1000,
			HopPubkeys:     hopPubkeys,
			FinalCltvDelta: cltvDelta,
		})
		cancel()
		if berr != nil {
			break
		}
		routeMsg := built.GetRoute()
		if len(routeMsg.GetHops()) == 0 {
			break
		}
		// Scale the fee budget proportionally to the current probe amount; if the
		// route is already too expensive at this amount (due to constant base fees),
		// halving will not help so we abort.
		amountFeeLimitSat := int64(float64(feeLimitSat*amount) / float64(targetAmountSat))
		if amountFeeLimitSat < 1 {
			amountFeeLimitSat = 1
		}
		if routeMsg.GetTotalFeesMsat()/1000 > amountFeeLimitSat {
			break
		}
		var routeFeePpm *float64
		totalAmt := routeMsg.GetTotalAmtMsat()
		totalFees := routeMsg.GetTotalFeesMsat()
		if totalAmt != 0 && totalFees != 0 && totalAmt > totalFees {
			v := (float64(totalFees) / float64(totalAmt-totalFees)) * 1000000
			routeFeePpm = &v
		}
		status := probeRouteSync(ctx, routerstub, routeMsg, 30*time.Second)
		switch status {
		case "verified":
			b, _ := proto.Marshal(routeMsg)
			h := hex.EncodeToString(b)
			bestHex = &h
			bestFeePpm = routeFeePpm
			good = amount
			if bad-good <= maxInt64(good/20, 1000) {
				break search
			}
			amount = (good + bad) / 2
		case "liquidity":
			bad = amount
			if bad-good <= maxInt64(good/20, 1000) {
				break search
			}
			amount = (good + bad) / 2
		case "fee":
			// Retry the same amount once; if the fee failure persists, abort.
			if feeRetried {
				break search
			}
			feeRetried = true
			continue
		default:
			break search
		}
	}
	return bestHex, good, bestFeePpm
}

// Exported type aliases so the graph_watcher can reuse the probe interfaces.
type (
	ProbeLightningClient = probeLightningClient
	ProbeRouterClient    = probeRouterClient
	ProbeQuerier         = probeQuerier
)

// ProbeTargets is the exported entry point for probeTargets, returning only the
// new/existing/errors counters without the per-target detail list.
func ProbeTargets(ctx context.Context, stub ProbeLightningClient, routerstub ProbeRouterClient, q ProbeQuerier, selfPubkey string, targets []db.GuiChannel, outboundCans []string, sourceFeeMap map[string]int, maxFeeRate, maxPerTarget int) (int, int, int, error) {
	n, e, er, _, err := probeTargets(ctx, stub, routerstub, q, selfPubkey, targets, outboundCans, sourceFeeMap, maxFeeRate, maxPerTarget)
	return n, e, er, err
}

// probeTargets probes auto-rebalance targets using random-preimage HTLCs and
// stores only verified routes. Returns new/existing/error counts and per-target details.
func probeTargets(ctx context.Context, stub probeLightningClient, routerstub probeRouterClient, q probeQuerier, selfPubkey string, targets []db.GuiChannel, outboundCans []string, sourceFeeMap map[string]int, maxFeeRate, maxPerTarget int) (int, int, int, []probeTargetDetail, error) {
	const probeBufferPpm = 100
	probedPubkeys := map[string]bool{}
	totalNew, totalExisting, totalErrors := 0, 0, 0
	targetDetails := []probeTargetDetail{}

	for _, chn := range targets {
		if probedPubkeys[chn.RemotePubkey] {
			continue
		}
		probedPubkeys[chn.RemotePubkey] = true
		targetNew, targetExisting, targetErrors, targetVerified := 0, 0, 0, 0

		peerChanIDs, err := q.ListChanIDsByPubkey(ctx, chn.RemotePubkey)
		if err != nil {
			return 0, 0, 0, nil, err
		}
		peerSet := map[string]bool{}
		for _, c := range peerChanIDs {
			peerSet[c] = true
		}
		var outChans []string
		for _, c := range outboundCans {
			if !peerSet[c] {
				outChans = append(outChans, c)
			}
		}
		limit := maxPerTarget
		if limit > len(outChans) {
			limit = len(outChans)
		}

		for _, outChan := range outChans[:limit] {
			sourceFeeRate := sourceFeeMap[outChan]
			inner := int(float64(chn.LocalFeeRate)*(float64(chn.ArMaxCost)/100)) - sourceFeeRate
			budgetPpm := maxFeeRate
			if inner < budgetPpm {
				budgetPpm = inner
			}
			feeLimitSat := int64(float64(int64(budgetPpm+probeBufferPpm)*chn.ArAmtTarget) / 1000000)
			if feeLimitSat <= 0 {
				continue
			}

			outChanU, perr := strconv.ParseUint(outChan, 10, 64)
			lastHop, herr := hex.DecodeString(chn.RemotePubkey)
			var response *lnrpc.QueryRoutesResponse
			var qerr error
			if perr != nil || herr != nil {
				qerr = errors.New("parse error")
			} else {
				response, qerr = stub.QueryRoutes(ctx, &lnrpc.QueryRoutesRequest{
					PubKey:            selfPubkey,
					Amt:               chn.ArAmtTarget,
					OutgoingChanIds:   []uint64{outChanU},
					LastHopPubkey:     lastHop,
					FeeLimit:          &lnrpc.FeeLimit{Limit: &lnrpc.FeeLimit_Fixed{Fixed: feeLimitSat}},
					UseMissionControl: true,
				})
			}
			if qerr != nil {
				targetErrors++
				continue
			}
			if response == nil || len(response.GetRoutes()) == 0 {
				continue
			}
			route := response.GetRoutes()[0]
			hops := route.GetHops()
			if len(hops) == 0 {
				continue
			}
			hopPubkeys := make([][]byte, 0, len(hops))
			for _, h := range hops {
				b, e := hex.DecodeString(h.GetPubKey())
				if e != nil {
					return 0, 0, 0, nil, e
				}
				hopPubkeys = append(hopPubkeys, b)
			}
			cltv := 144
			if len(hops) >= 2 {
				cltv = int(hops[len(hops)-1].GetExpiry()) - int(hops[len(hops)-2].GetExpiry())
			}
			verifiedHex, maxAmountSat, verifiedFeePpm := probeWithBinarySearch(ctx, routerstub, outChan, hopPubkeys, int32(cltv), chn.ArAmtTarget, feeLimitSat)
			if verifiedHex == nil || maxAmountSat <= 0 {
				targetErrors++
				continue
			}
			pathParts := make([]string, 0, len(hops))
			for _, h := range hops {
				pathParts = append(pathParts, h.GetPubKey())
			}
			path := strings.Join(pathParts, "-")
			routeOutChan := formatChanID(hops[0].GetChanId())

			existing, gerr := q.GetRebalanceRoute(ctx, db.GetRebalanceRouteParams{
				TargetPubkey: chn.RemotePubkey, OutgoingChanID: routeOutChan, Route: path,
			})
			switch {
			case isNoRows(gerr):
				var feePpm pgtype.Float8
				if verifiedFeePpm != nil {
					feePpm = pgtype.Float8{Float64: *verifiedFeePpm, Valid: true}
				}
				if e := q.InsertRebalanceRoute(ctx, db.InsertRebalanceRouteParams{
					TargetPubkey: chn.RemotePubkey, OutgoingChanID: routeOutChan, Route: path,
					FinalCltvDelta: int32(cltv),
					RouteHex:       pgtype.Text{String: *verifiedHex, Valid: true},
					LastFeePpm:     feePpm,
				}); e != nil {
					return 0, 0, 0, nil, e
				}
				targetNew++
			case gerr != nil:
				return 0, 0, 0, nil, gerr
			default:
				newFeePpm := existing.LastFeePpm
				if verifiedFeePpm != nil {
					newFeePpm = pgtype.Float8{Float64: *verifiedFeePpm, Valid: true}
				}
				if e := q.UpdateRebalanceRouteHex(ctx, db.UpdateRebalanceRouteHexParams{
					ID: existing.ID, RouteHex: pgtype.Text{String: *verifiedHex, Valid: true}, LastFeePpm: newFeePpm,
				}); e != nil {
					return 0, 0, 0, nil, e
				}
				targetExisting++
			}
			targetVerified++
		}

		totalNew += targetNew
		totalExisting += targetExisting
		totalErrors += targetErrors
		alias, isNone, err := peerAliasOpt(ctx, q, chn.RemotePubkey)
		if err != nil {
			return 0, 0, 0, nil, err
		}
		if isNone {
			alias = ""
		}
		targetDetails = append(targetDetails, probeTargetDetail{
			Pubkey:        chn.RemotePubkey,
			Alias:         alias,
			New:           targetNew,
			Existing:      targetExisting,
			Errors:        targetErrors,
			Verified:      targetVerified,
			OutChansTried: limit,
		})
	}
	return totalNew, totalExisting, totalErrors, targetDetails, nil
}

// isoLayout is the timestamp format used for QR-LastProbe storage.
// parseISO accepts both this format and RFC3339Nano.
const isoLayout = "2006-01-02T15:04:05.000000"

func parseISO(s string) (time.Time, error) {
	for _, l := range []string{
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse isoformat %q", s)
}

// ProbeRoutesJob runs the route probe when QR-Enabled is set and enough time has
// elapsed since QR-LastProbe. It probes auto-rebalance targets and writes a
// ProbeLog entry (keeping at most 100 records).
func ProbeRoutesJob(ctx context.Context, q probeQuerier, stub probeLightningClient, routerstub probeRouterClient) error {
	enabled, err := settingGateEnabled(ctx, q, "QR-Enabled")
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	updateHours, err := getOrCreateFloat(ctx, q, "QR-UpdateHours", "6")
	if err != nil {
		return err
	}
	maxPerTarget, err := getOrCreateInt(ctx, q, "QR-MaxPerTarget", "5")
	if err != nil {
		return err
	}

	// Check whether enough time has passed since the last probe run.
	lastProbe := time.Time{}
	if row, e := q.GetLocalSetting(ctx, "QR-LastProbe"); e == nil {
		if t, perr := parseISO(row.Value); perr == nil {
			lastProbe = t
		}
	} else if !isNoRows(e) {
		return e
	}
	if time.Since(lastProbe) < time.Duration(updateHours*float64(time.Hour)) {
		return nil
	}

	probeStart := time.Now()
	targets, err := q.ListOpenAutoRebalanceChannels(ctx)
	if err != nil {
		return err
	}
	outRows, err := q.ListOutboundCandidates(ctx)
	if err != nil {
		return err
	}
	outboundCans := make([]string, 0, len(outRows))
	sourceFeeMap := make(map[string]int, len(outRows))
	for _, r := range outRows {
		outboundCans = append(outboundCans, r.ChanID)
		sourceFeeMap[r.ChanID] = int(r.LocalFeeRate)
	}

	// Read AR-MaxFeeRate; use 500 as default when the key is absent.
	maxFeeRate := 500
	if row, e := q.GetLocalSetting(ctx, "AR-MaxFeeRate"); e == nil {
		if row.Value != "" {
			v, perr := strconv.Atoi(row.Value)
			if perr != nil {
				return perr
			}
			maxFeeRate = v
		}
	} else if !isNoRows(e) {
		return e
	}

	selfInfo, err := stub.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	selfPubkey := selfInfo.GetIdentityPubkey()

	totalNew, totalExisting, totalErrors, details, err := probeTargets(ctx, stub, routerstub, q, selfPubkey, targets, outboundCans, sourceFeeMap, maxFeeRate, maxPerTarget)
	if err != nil {
		return err
	}
	durationMs := int(time.Since(probeStart).Milliseconds())

	if err := q.UpsertLocalSetting(ctx, db.UpsertLocalSettingParams{Key: "QR-LastProbe", Value: time.Now().Format(isoLayout)}); err != nil {
		return err
	}
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return err
	}
	if err := q.InsertProbeLog(ctx, db.InsertProbeLogParams{
		Timestamp:      ts(time.Now()),
		TargetsScanned: int32(len(details)),
		RoutesFound:    int32(totalNew),
		RoutesExisting: int32(totalExisting),
		Errors:         int32(totalErrors),
		DurationMs:     int32(durationMs),
		Details:        detailsJSON,
	}); err != nil {
		return err
	}
	if err := q.DeleteOldProbeLogs(ctx); err != nil {
		return err
	}

	if totalNew != 0 {
		dataLog(fmt.Sprintf("QueryRoutes probe discovered %d new route(s) for %d target(s)", totalNew, len(details)))
	} else {
		dataLog(fmt.Sprintf("QueryRoutes probe completed: %d targets, %d existing routes, %d errors (%dms)", len(details), totalExisting, totalErrors, durationMs))
	}
	return nil
}
