package web

import (
	"net/http"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// opensSQL selects suggested peers from PaymentHops over the last 60 days of
// successful payments, excluding self, current peers, and avoided nodes.
// Computes ppm/score/count/amount/fees/sum_cost_to/alias; filters score != 0
// (numeric ROUND = half away from zero, as Django generates it);
// ordered by -score then ppm; top 21.
const opensSQL = `SELECT node_pubkey,
        (sum(fee) / sum(amt)) * 1000000 AS ppm,
        ROUND((ROUND(count(id) / 1, 0) + ROUND((sum(amt) / 100000)::numeric, 0)) / 10, 0)::bigint AS score,
        count(id) AS "count",
        sum(amt) AS amount,
        sum(fee) AS fees,
        sum(cost_to) / (sum(amt) / 1000000) AS sum_cost_to,
        max(alias) AS alias
    FROM gui_paymenthops
    WHERE payment_hash_id IN (
            SELECT payment_hash FROM gui_payments WHERE creation_date >= $1 AND status = 2
        )
        AND node_pubkey <> $2
        AND node_pubkey NOT IN (SELECT remote_pubkey FROM gui_channels WHERE is_open = true)
        AND node_pubkey NOT IN (SELECT pubkey FROM gui_avoidnodes)
    GROUP BY node_pubkey
    HAVING ROUND((ROUND(count(id) / 1, 0) + ROUND((sum(amt) / 100000)::numeric, 0)) / 10, 0) <> 0
    ORDER BY score DESC, ppm
    LIMIT 21`

// handleOpens renders the suggested peers page (routing heuristic) plus the
// avoid list. LND/DB errors return 500.
func (s *Server) handleOpens(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	openList, err := s.queryMaps(ctx, opensSQL, time.Now().UTC().AddDate(0, 0, -60), info.GetIdentityPubkey())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	avoidList, err := s.queryMaps(ctx, `SELECT * FROM gui_avoidnodes`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "open_list.html", map[string]any{
		"open_list":  openList,
		"avoid_list": avoidList,
	})
}
