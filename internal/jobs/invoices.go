package jobs

import (
	"context"
	"encoding/hex"
	"time"

	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/signrpc"
)

// signerClient is the signrpc subset used for WhatSat sender verification.
type signerClient interface {
	VerifyMessage(ctx context.Context, in *signrpc.VerifyMessageReq, opts ...grpc.CallOption) (*signrpc.VerifyMessageResp, error)
}

// invoicesClient is the LND subset required by UpdateInvoices.
type invoicesClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	ListInvoices(ctx context.Context, in *lnrpc.ListInvoiceRequest, opts ...grpc.CallOption) (*lnrpc.ListInvoiceResponse, error)
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
	signerClient
}

// invoicesQuerier bundles node cache and invoice DB access.
type invoicesQuerier interface {
	nodeCacheQ
	ListOpenInvoices(ctx context.Context) ([]db.ListOpenInvoicesRow, error)
	MaxInvoiceIndex(ctx context.Context) (int32, error)
	InsertInvoice(ctx context.Context, arg db.InsertInvoiceParams) error
	SetInvoiceState(ctx context.Context, arg db.SetInvoiceStateParams) error
	UpdateInvoiceSettled(ctx context.Context, arg db.UpdateInvoiceSettledParams) error
	GetChannelAlias(ctx context.Context, chanID string) (string, error)
}

// UpdateInvoices syncs open invoices and fetches new invoices from LND.
func UpdateInvoices(ctx context.Context, q invoicesQuerier, client invoicesClient) error {
	open, err := q.ListOpenInvoices(ctx)
	if err != nil {
		return err
	}
	for _, openInvoice := range open {
		resp, e := client.ListInvoices(ctx, &lnrpc.ListInvoiceRequest{
			IndexOffset: uint64(openInvoice.Index - 1), NumMaxInvoices: 1,
		})
		if e != nil {
			return e
		}
		inv := resp.Invoices
		if len(inv) > 0 && openInvoice.RHash == hex.EncodeToString(inv[0].RHash) {
			if e := updateInvoice(ctx, q, client, inv[0], openInvoice.RHash); e != nil {
				return e
			}
		} else {
			if e := q.SetInvoiceState(ctx, db.SetInvoiceStateParams{RHash: openInvoice.RHash, State: 2}); e != nil {
				return e
			}
		}
	}

	maxIdx, err := q.MaxInvoiceIndex(ctx)
	if err != nil {
		return err
	}
	resp, err := client.ListInvoices(ctx, &lnrpc.ListInvoiceRequest{
		IndexOffset: uint64(maxIdx), NumMaxInvoices: 100,
	})
	if err != nil {
		return err
	}
	for _, invoice := range resp.Invoices {
		rHash := hex.EncodeToString(invoice.RHash)
		if e := q.InsertInvoice(ctx, db.InsertInvoiceParams{
			CreationDate: ts(time.Unix(invoice.CreationDate, 0)),
			RHash:        rHash,
			Value:        roundTo(float64(invoice.ValueMsat)/1000, 3),
			AmtPaid:      invoice.AmtPaidSat,
			State:        int32(invoice.State),
			Index:        int32(invoice.AddIndex),
		}); e != nil {
			return e
		}
		if e := updateInvoice(ctx, q, client, invoice, rHash); e != nil {
			return e
		}
	}
	return nil
}

// updateInvoice processes a single invoice: updates its state, and for settled
// invoices extracts HTLC metadata including keysend preimage, message, and
// verified WhatSat sender.
func updateInvoice(ctx context.Context, q invoicesQuerier, client invoicesClient, invoice *lnrpc.Invoice, rHash string) error {
	if int32(invoice.State) != 1 { // only SETTLED invoices carry detail fields
		return q.SetInvoiceState(ctx, db.SetInvoiceStateParams{RHash: rHash, State: int32(invoice.State)})
	}

	var chanIn, chanInAlias, keysendPreimage, message, sender, senderAlias *string
	if len(invoice.Htlcs) > 0 {
		htlc := invoice.Htlcs[0]
		ci := formatChanID(htlc.ChanId)
		chanIn = &ci
		if alias, err := q.GetChannelAlias(ctx, ci); err == nil {
			a := alias
			chanInAlias = &a
		} else if !isNoRows(err) {
			return err
		}
		records := htlc.CustomRecords
		if p, ok := records[5482373484]; ok {
			ph := hex.EncodeToString(p)
			keysendPreimage = &ph
		}
		if m, ok := records[34349334]; ok {
			msg := truncRunes(string(m), 1000)
			message = &msg
		}
		_, has37 := records[34349337]
		_, has39 := records[34349339]
		_, has43 := records[34349343]
		_, has34 := records[34349334]
		if has37 && has39 && has43 && has34 {
			info, ierr := client.GetInfo(ctx, &lnrpc.GetInfoRequest{})
			valid := false
			if ierr == nil {
				selfPub, _ := hex.DecodeString(info.IdentityPubkey)
				msg := append(append(append(append([]byte{}, records[34349339]...), selfPub...), records[34349343]...), records[34349334]...)
				resp, verr := client.VerifyMessage(ctx, &signrpc.VerifyMessageReq{
					Msg: msg, Signature: records[34349337], Pubkey: records[34349339],
				})
				if verr != nil {
					dataLog("Unable to validate signature on invoice: " + rHash)
					valid = false
				} else {
					valid = resp.Valid
				}
			}
			if valid {
				s := hex.EncodeToString(records[34349339])
				sender = &s
				a := nodeAlias(ctx, q, client, s)
				senderAlias = &a
			}
		}
	}

	return q.UpdateInvoiceSettled(ctx, db.UpdateInvoiceSettledParams{
		RHash:           rHash,
		State:           int32(invoice.State),
		AmtPaid:         invoice.AmtPaidSat,
		SettleDate:      ts(time.Unix(invoice.SettleDate, 0)),
		ChanIn:          textPtr(chanIn),
		ChanInAlias:     textPtr(chanInAlias),
		KeysendPreimage: textPtr(keysendPreimage),
		Message:         textPtr(message),
		Sender:          textPtr(sender),
		SenderAlias:     textPtr(senderAlias),
	})
}
