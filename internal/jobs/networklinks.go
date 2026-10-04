package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// netLinksQuerier is the narrow DB subset required by NetworkLinks and GetTxFees.
type netLinksQuerier interface {
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
}

// NetworkLinks returns the configured GUI-NetLinks setting, creating it with the
// default value "https://mempool.space" if it does not yet exist.
func NetworkLinks(ctx context.Context, q netLinksQuerier) (string, error) {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{
		Key:   "GUI-NetLinks",
		Value: "https://mempool.space",
	})
	if err != nil {
		return "", err
	}
	return row.Value, nil
}

// GetTxFees fetches the on-chain fee for txid from the configured block explorer.
// HTTP or parse errors are logged and silently return 0; errors from NetworkLinks
// are propagated.
func GetTxFees(ctx context.Context, q netLinksQuerier, network, txid string) (int, error) {
	links, err := NetworkLinks(ctx, q)
	if err != nil {
		return 0, err
	}
	suffix := ""
	if network == "testnet" {
		suffix = "/testnet"
	}
	baseURL := links + suffix + "/api/tx/"

	fee, ferr := fetchTxFee(baseURL + txid)
	if ferr != nil {
		dataLog(fmt.Sprintf("Error getting closure fees for %s: %s", txid, ferr))
		return 0, nil
	}
	return fee, nil
}

// txFeeClient bounds the explorer request so an unresponsive server cannot
// stall the data loop.
var txFeeClient = &http.Client{Timeout: 10 * time.Second}

// fetchTxFee fetches a transaction JSON from url and extracts the "fee" field.
// Returns an error if the field is absent or cannot be decoded.
func fetchTxFee(url string) (int, error) {
	resp, err := txFeeClient.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var data map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, err
	}
	raw, ok := data["fee"]
	if !ok {
		return 0, fmt.Errorf("'fee'")
	}
	var fee int
	if err := json.Unmarshal(raw, &fee); err != nil {
		return 0, err
	}
	return fee, nil
}
