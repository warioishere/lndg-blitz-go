package jobs

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/signrpc"
)

// dataClient satisfies the invoicesClient interface. update_invoices uses
// signrpc.VerifyMessage for WhatSat sender verification; its signature conflicts
// with lnrpc.VerifyMessage. The embedded LightningClient provides all other RPCs;
// the VerifyMessage method here shadows the lnrpc variant and delegates to the
// signer (both share the same underlying connection).
type dataClient struct {
	lnrpc.LightningClient
	signer signrpc.SignerClient
}

func (c dataClient) VerifyMessage(ctx context.Context, in *signrpc.VerifyMessageReq, opts ...grpc.CallOption) (*signrpc.VerifyMessageResp, error) {
	return c.signer.VerifyMessage(ctx, in, opts...)
}

// BuildDataDeps constructs the DataLoopDeps for the [Data] loop from a querier
// and a connection source. getConn returns the current LND ClientConn (RunOnce
// builds the stubs from it), resetConn closes and reopens the connection,
// resetPool releases idle DB connections. Used by both cmd/jobs (shared channel)
// and cmd/controller (dedicated connection).
func BuildDataDeps(q DataQuerier, getConn func() (*grpc.ClientConn, error), resetConn, resetPool func(), sleep time.Duration) DataLoopDeps {
	return DataLoopDeps{
		RunOnce: func(ctx context.Context) error {
			conn, err := getConn()
			if err != nil {
				return err
			}
			stub := dataClient{
				LightningClient: lnrpc.NewLightningClient(conn),
				signer:          signrpc.NewSignerClient(conn),
			}
			routerstub := routerrpc.NewRouterClient(conn)
			return RunDataJobs(ctx, q, stub, routerstub)
		},
		ResetChannel: resetConn,
		ResetPool:    resetPool,
		Sleep:        sleep,
	}
}
