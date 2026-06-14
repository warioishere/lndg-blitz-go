package jobs

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"time"

	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// cleanClient is the LND subset required by CleanPayments.
type cleanClient interface {
	DeletePayment(ctx context.Context, in *lnrpc.DeletePaymentRequest, opts ...grpc.CallOption) (*lnrpc.DeletePaymentResponse, error)
}

// cleanQuerier is the DB subset required by CleanPayments.
type cleanQuerier interface {
	settingsQuerier
	ListPaymentsToClean(ctx context.Context, creationDate pgtype.Timestamptz) ([]db.ListPaymentsToCleanRow, error)
	SetPaymentCleaned(ctx context.Context, paymentHash string) error
}

// CleanPayments deletes old failed payments from LND according to the configured
// retention period. For succeeded payments only the failed HTLCs are removed.
func CleanPayments(ctx context.Context, q cleanQuerier, client cleanClient) error {
	enabled, err := getOrCreateInt(ctx, q, "LND-CleanPayments", "0")
	if err != nil {
		return err
	}
	if enabled != 1 {
		return nil
	}
	retentionDays, err := getOrCreateInt(ctx, q, "LND-RetentionDays", "30")
	if err != nil {
		return err
	}
	timeFilter := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour)
	payments, err := q.ListPaymentsToClean(ctx, ts(timeFilter))
	if err != nil {
		return err
	}
	for _, payment := range payments {
		hash, herr := hex.DecodeString(payment.PaymentHash)
		if herr != nil {
			return herr
		}
		htlcsOnly := payment.Status == 2
		// Log deletion errors but always mark the payment as cleaned.
		if _, derr := client.DeletePayment(ctx, &lnrpc.DeletePaymentRequest{
			PaymentHash: hash, FailedHtlcsOnly: htlcsOnly,
		}); derr != nil {
			dataLog(fmt.Sprintf("Error cleaning payment %s at index %d with payment status %d: %s",
				payment.PaymentHash, payment.Index, payment.Status, grpcErrMsg(derr)))
		}
		if e := q.SetPaymentCleaned(ctx, payment.PaymentHash); e != nil {
			return e
		}
	}
	return nil
}
