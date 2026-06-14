package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// txFeeFetcher fetches transaction fees from a block explorer API.
// The interface is mockable for tests.
type txFeeFetcher interface {
	GetTxFee(ctx context.Context, url string) (int64, error)
}

type httpTxFeeFetcher struct{ client *http.Client }

func newHTTPTxFeeFetcher() *httpTxFeeFetcher { return &httpTxFeeFetcher{client: &http.Client{}} }

func (h *httpTxFeeFetcher) GetTxFee(ctx context.Context, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var body struct {
		Fee int64 `json:"fee"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	return body.Fee, nil
}

// handleGetFees fetches missing closure costs via the mempool API (closing-tx fee
// plus sweep fees, avoiding double-counting fees from earlier closures). On fetch
// error it flashes a message and redirects, preserving any partial work.
func (s *Server) handleGetFees(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}

	baseURL := s.networkLinks(ctx)
	if s.cfg.LND_NETWORK == "testnet" {
		baseURL += "/testnet"
	}
	baseURL += "/api/tx/"

	type missing struct {
		id            int64
		chanID        string
		closingTx     string
		openInitiator int32
	}
	rows, err := s.queryMaps(ctx, `SELECT id, chan_id, closing_tx, open_initiator FROM gui_closures
		WHERE close_type NOT IN (4,5) AND NOT (open_initiator=2 AND resolution_count=0) AND closing_costs=0
		ORDER BY id`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	missingFees := make([]missing, 0, len(rows))
	for _, row := range rows {
		id, _ := toInt64(row["id"])
		oi, _ := toInt64(row["open_initiator"])
		cid, _ := row["chan_id"].(string)
		cltx, _ := row["closing_tx"].(string)
		missingFees = append(missingFees, missing{id: id, chanID: cid, closingTx: cltx, openInitiator: int32(oi)})
	}

	for _, m := range missingFees {
		// Sweep TXIDs from earlier closures (id < m.id) — do not count them again.
		swept := map[string]bool{}
		sweptRows, err := s.queryMaps(ctx, `SELECT sweep_txid FROM gui_resolutions
			WHERE chan_id IN (SELECT chan_id FROM gui_closures WHERE id < $1)`, m.id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, sr := range sweptRows {
			if tx, ok := sr["sweep_txid"].(string); ok {
				swept[tx] = true
			}
		}

		txid := m.closingTx // captured for the error message
		var closingCosts int64
		if m.openInitiator == 1 {
			fee, ferr := s.txFees.GetTxFee(ctx, baseURL+txid)
			if ferr != nil {
				f.add(fmt.Sprintf("Error getting closure fees: txid='%s' error=%s", txid, ferr.Error()))
				s.redirect(w, r, refererOr(r, "/"), f)
				return
			}
			closingCosts = fee
		}
		resRows, err := s.queryMaps(ctx, `SELECT sweep_txid FROM gui_resolutions
			WHERE chan_id=$1 AND resolution_type <> 2 ORDER BY id`, m.chanID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, rr := range resRows {
			sweepTxid, _ := rr["sweep_txid"].(string)
			if swept[sweepTxid] {
				continue
			}
			fee, ferr := s.txFees.GetTxFee(ctx, baseURL+sweepTxid)
			if ferr != nil {
				f.add(fmt.Sprintf("Error getting closure fees: txid='%s' error=%s", txid, ferr.Error()))
				s.redirect(w, r, refererOr(r, "/"), f)
				return
			}
			closingCosts += fee
			swept[sweepTxid] = true
		}
		if _, err := s.db.Exec(ctx, `UPDATE gui_closures SET closing_costs=$2 WHERE id=$1`, m.id, closingCosts); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	s.redirect(w, r, refererOr(r, "/"), f)
}
