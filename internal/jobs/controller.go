package jobs

import (
	"context"
	"fmt"
	"time"
)

// DataQuerier combines the querier subsets required by all Data jobs. *db.Queries
// satisfies this interface (all methods are sqlc-generated). Overlapping methods
// (e.g. GetLocalSetting) are valid in Go interface embedding as long as the
// signatures are identical.
type DataQuerier interface {
	peerQuerier
	updateChannelsQuerier
	emergencyQuerier
	invoicesQuerier
	paymentsQuerier
	forwardsQuerier
	onchainQuerier
	closuresQuerier
	cleanQuerier
	autoFeesQuerier
	inboundOffsetsQuerier
	maxhtlcQuerier
	boostQuerier
	probeQuerier
	aggQuerier
}

// DataLightningClient combines the LightningClient subsets required by all Data jobs.
// lnrpc.LightningClient satisfies this interface.
type DataLightningClient interface {
	peerClient
	updateChannelsClient
	policyClient
	invoicesClient
	paymentsClient
	forwardsClient
	onchainClient
	closuresClient
	cleanClient
	probeLightningClient
}

// RunDataJobs runs all 17 Data jobs in a fixed order. The first error aborts
// the run; remaining jobs are skipped.
func RunDataJobs(ctx context.Context, q DataQuerier, stub DataLightningClient, routerstub probeRouterClient) error {
	if err := UpdatePeers(ctx, q, stub); err != nil {
		return err
	}
	if err := RefreshPeerAliases(ctx, q, stub); err != nil {
		return err
	}
	if err := UpdateChannels(ctx, q, stub); err != nil {
		return err
	}
	if err := EmergencyFeeJob(ctx, q, stub); err != nil {
		return err
	}
	if err := UpdateInvoices(ctx, q, stub); err != nil {
		return err
	}
	if err := UpdatePayments(ctx, q, stub); err != nil {
		return err
	}
	if err := UpdateForwards(ctx, q, stub); err != nil {
		return err
	}
	if err := UpdateOnchain(ctx, q, stub); err != nil {
		return err
	}
	if err := UpdateClosures(ctx, q, stub); err != nil {
		return err
	}
	if err := ReconnectPeers(ctx, q, stub); err != nil {
		return err
	}
	if err := CleanPayments(ctx, q, stub); err != nil {
		return err
	}
	if err := AutoFees(ctx, q, stub); err != nil {
		return err
	}
	if err := InboundOffsets(ctx, q, stub); err != nil {
		return err
	}
	if err := AutoMaxhtlcJob(ctx, q, stub); err != nil {
		return err
	}
	if err := FailedHtlcBoostJob(ctx, q, stub); err != nil {
		return err
	}
	if err := ProbeRoutesJob(ctx, q, stub, routerstub); err != nil {
		return err
	}
	return AggFailedHtlcs(ctx, q)
}

// DataLoopDeps bundles the side-effecting dependencies of the Data loop so that
// DataLoop can be tested without a real LND or database connection. The concrete
// wiring to lnd/pgxpool lives in cmd/controller.
type DataLoopDeps struct {
	// RunOnce builds the stub and router stub from the current shared channel
	// and calls RunDataJobs.
	RunOnce func(ctx context.Context) error
	// ResetChannel closes and reopens the shared gRPC channel.
	ResetChannel func()
	// ResetPool resets the database connection pool.
	ResetPool func()
	// Sleep is the interval between loop iterations.
	Sleep time.Duration
}

// DataLoop runs continuously, calling RunOnce on each iteration. On error it
// resets the gRPC channel and DB pool before sleeping. The loop exits cleanly
// when ctx is cancelled.
func DataLoop(ctx context.Context, d DataLoopDeps) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		dataLog("Starting data execution...")
		if err := d.RunOnce(ctx); err != nil {
			dataLog(fmt.Sprintf("Error processing background data: %s", err))
			d.ResetChannel()
			d.ResetPool()
		}
		dataLog("Data execution completed...sleeping for 20 seconds")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.Sleep):
		}
	}
}
