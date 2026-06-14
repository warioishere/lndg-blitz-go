package jobs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// closuresClient is the LND subset required by UpdateClosures.
type closuresClient interface {
	ClosedChannels(ctx context.Context, in *lnrpc.ClosedChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ClosedChannelsResponse, error)
}

// closuresQuerier is the DB subset required by UpdateClosures and GetTxFees.
type closuresQuerier interface {
	netLinksQuerier
	CountClosures(ctx context.Context) (int64, error)
	InsertClosure(ctx context.Context, arg db.InsertClosureParams) error
	DeleteClosureByFunding(ctx context.Context, arg db.DeleteClosureByFundingParams) error
	UpdateClosureCosts(ctx context.Context, arg db.UpdateClosureCostsParams) error
	DeleteResolutionsByChan(ctx context.Context, chanID string) error
	ExistsResolutionBySweep(ctx context.Context, sweepTxid string) (bool, error)
	InsertResolution(ctx context.Context, arg db.InsertResolutionParams) error
}

// UpdateClosures syncs closed channel records from LND into the database.
// Only channels beyond the already-stored count are processed.
func UpdateClosures(ctx context.Context, q closuresQuerier, client closuresClient) error {
	resp, err := client.ClosedChannels(ctx, &lnrpc.ClosedChannelsRequest{})
	if err != nil {
		return err
	}
	closures := resp.Channels
	existing, err := q.CountClosures(ctx)
	if err != nil {
		return err
	}
	if int64(len(closures)) <= existing {
		return nil
	}

	network := config.Get().LND_NETWORK
	skip := existing
	var counter int64
	for _, closure := range closures {
		counter++
		if counter <= skip {
			continue
		}
		resolutionCount := len(closure.Resolutions)
		parts := strings.SplitN(closure.ChannelPoint, ":", 2)
		txid := parts[0]
		var fundingIndex int32
		if len(parts) > 1 {
			n, _ := strconv.Atoi(parts[1])
			fundingIndex = int32(n)
		}

		closingCosts := 0
		if int(closure.OpenInitiator) != 2 && closure.CloseType != 4 && closure.CloseType != 5 {
			fee, e := GetTxFees(ctx, q, network, closure.ClosingTxHash)
			if e != nil {
				return e
			}
			closingCosts = fee
		}

		if e := q.InsertClosure(ctx, db.InsertClosureParams{
			ChanID: formatChanID(closure.ChanId), FundingTxid: txid, FundingIndex: fundingIndex,
			ClosingTx: closure.ClosingTxHash, RemotePubkey: closure.RemotePubkey, Capacity: closure.Capacity,
			CloseHeight: int32(closure.CloseHeight), SettledBalance: closure.SettledBalance,
			TimeLockedBalance: closure.TimeLockedBalance, CloseType: int32(closure.CloseType),
			OpenInitiator: int32(closure.OpenInitiator), CloseInitiator: int32(closure.CloseInitiator),
			ResolutionCount: int32(resolutionCount),
		}); e != nil {
			dataLog(fmt.Sprintf("Error inserting closure: %s", e))
			_ = q.DeleteClosureByFunding(ctx, db.DeleteClosureByFundingParams{FundingTxid: txid, FundingIndex: fundingIndex})
			return nil
		}

		if resolutionCount > 0 {
			if e := q.DeleteResolutionsByChan(ctx, formatChanID(closure.ChanId)); e != nil {
				return e
			}
			for _, resolution := range closure.Resolutions {
				if int(resolution.ResolutionType) != 2 {
					exists, e := q.ExistsResolutionBySweep(ctx, resolution.SweepTxid)
					if e != nil {
						return e
					}
					if !exists {
						fee, e := GetTxFees(ctx, q, network, resolution.SweepTxid)
						if e != nil {
							return e
						}
						closingCosts += fee
					}
				}
				if e := q.InsertResolution(ctx, db.InsertResolutionParams{
					ChanID: formatChanID(closure.ChanId), ResolutionType: int32(resolution.ResolutionType),
					Outcome: int32(resolution.Outcome), OutpointTx: resolution.GetOutpoint().GetTxidStr(),
					OutpointIndex: int32(resolution.GetOutpoint().GetOutputIndex()), AmountSat: int64(resolution.AmountSat),
					SweepTxid: resolution.SweepTxid,
				}); e != nil {
					return e
				}
			}
		}
		if e := q.UpdateClosureCosts(ctx, db.UpdateClosureCostsParams{
			FundingTxid: txid, FundingIndex: fundingIndex, ClosingCosts: int32(closingCosts),
		}); e != nil {
			return e
		}
	}
	return nil
}
