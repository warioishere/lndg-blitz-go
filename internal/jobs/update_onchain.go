package jobs

import (
	"context"
	"time"

	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// onchainClient is the LND subset required by UpdateOnchain.
type onchainClient interface {
	GetTransactions(ctx context.Context, in *lnrpc.GetTransactionsRequest, opts ...grpc.CallOption) (*lnrpc.TransactionDetails, error)
}

// onchainQuerier is the DB subset required by UpdateOnchain.
type onchainQuerier interface {
	DeleteOnchainZeroBlock(ctx context.Context) error
	NextOnchainBlockHeight(ctx context.Context) (int32, error)
	InsertOnchain(ctx context.Context, arg db.InsertOnchainParams) error
}

// truncRunes truncates s to at most n Unicode code points.
func truncRunes(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

// UpdateOnchain fetches on-chain transactions from LND starting at the last
// known block height and inserts them into the DB.
func UpdateOnchain(ctx context.Context, q onchainQuerier, client onchainClient) error {
	if err := q.DeleteOnchainZeroBlock(ctx); err != nil {
		return err
	}
	lastBlock, err := q.NextOnchainBlockHeight(ctx)
	if err != nil {
		return err
	}
	resp, err := client.GetTransactions(ctx, &lnrpc.GetTransactionsRequest{StartHeight: lastBlock})
	if err != nil {
		return err
	}
	for _, tx := range resp.Transactions {
		if err := q.InsertOnchain(ctx, db.InsertOnchainParams{
			TxHash:      tx.TxHash,
			TimeStamp:   ts(time.Unix(tx.TimeStamp, 0)),
			Amount:      tx.Amount,
			Fee:         int32(tx.TotalFees),
			BlockHash:   tx.BlockHash,
			BlockHeight: tx.BlockHeight,
			Label:       truncRunes(tx.Label, 100),
		}); err != nil {
			return err
		}
	}
	return nil
}
