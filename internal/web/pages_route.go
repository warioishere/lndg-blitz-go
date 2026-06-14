package web

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// stringSlicesEqual compares two string slices element-by-element.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// handleRebalanceRouteDetail renders the saved route detail page: the hops of a
// stored route (with self/peer aliases), up to 5 successful rebalances whose
// PaymentHops path exactly matches the route, and associated invoices. RPC errors
// render error.html.
func (s *Server) handleRebalanceRouteDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, _ := strconv.Atoi(chi.URLParam(r, "id")) // route pattern enforces [0-9]+

	var targetPubkey, outgoingChanID, route string
	err := s.db.QueryRow(ctx,
		`SELECT target_pubkey, outgoing_chan_id, route FROM gui_rebalanceroute WHERE id = $1`, id).
		Scan(&targetPubkey, &outgoingChanID, &route)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.renderError(w, r, "No RebalanceRoute matches the given query.")
			return
		}
		s.renderError(w, r, err.Error())
		return
	}

	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	selfPubkey := info.GetIdentityPubkey()
	selfAlias := info.GetAlias()

	routeList := strings.Split(route, "-")
	peerAlias := map[string]string{}
	if peers, e := s.queryMaps(ctx, `SELECT pubkey, alias FROM gui_peers WHERE pubkey = ANY($1)`, routeList); e == nil {
		for _, p := range peers {
			pk, _ := p["pubkey"].(string)
			al, _ := p["alias"].(string)
			peerAlias[pk] = al
		}
	}
	routeHops := make([]map[string]any, 0, len(routeList))
	for i, pub := range routeList {
		alias := peerAlias[pub]
		if pub == selfPubkey {
			alias = selfAlias
		}
		routeHops = append(routeHops, map[string]any{
			"attempt_id": 1, "step": i + 1, "alias": alias, "pubkey": pub,
		})
	}

	potential, err := s.queryMaps(ctx,
		`SELECT id, payment_hash, requested, start, stop, value, fee_limit, fees_paid
            FROM gui_rebalancer
            WHERE last_hop_pubkey = $1 AND status = 2 AND payment_hash IS NOT NULL
              AND strpos(outgoing_chan_ids, $2) > 0
            ORDER BY id DESC`, targetPubkey, outgoingChanID)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}

	var rebalances []map[string]any
	var paymentHashes []string
	for _, reb := range potential {
		ph, _ := reb["payment_hash"].(string)
		hops, e := s.queryMaps(ctx,
			`SELECT attempt_id, node_pubkey FROM gui_paymenthops WHERE payment_hash_id = $1 ORDER BY attempt_id, step`, ph)
		if e != nil {
			s.renderError(w, r, e.Error())
			return
		}
		if len(hops) == 0 {
			continue
		}
		attemptNodes := map[int64][]string{}
		for _, h := range hops {
			aid, _ := toInt64(h["attempt_id"])
			np, _ := h["node_pubkey"].(string)
			attemptNodes[aid] = append(attemptNodes[aid], np)
		}
		for _, nodes := range attemptNodes {
			if stringSlicesEqual(nodes, routeList) {
				rebalances = append(rebalances, reb)
				paymentHashes = append(paymentHashes, ph)
				break
			}
		}
		if len(rebalances) >= 5 {
			break
		}
	}

	var invoices []map[string]any
	if len(paymentHashes) > 0 {
		invoices, err = s.queryMaps(ctx, `SELECT * FROM gui_invoices WHERE r_hash = ANY($1)`, paymentHashes)
		if err != nil {
			s.renderError(w, r, err.Error())
			return
		}
	}

	s.renderTemplate(w, r, "saved_route.html", map[string]any{
		"route":         routeHops,
		"target_pubkey": targetPubkey,
		"rebalances":    rebalances,
		"invoices":      invoices,
	})
}

// htlcsByHashLock returns pending HTLCs for a hash_lock with expiration annotations
// relative to the given block height.
func (s *Server) htlcsByHashLock(ctx context.Context, blockHeight int64, incoming bool, hashLock string) ([]map[string]any, error) {
	return s.queryMaps(ctx,
		`SELECT *, (expiration_height - $1) AS blocks_til_expiration,
            ((expiration_height - $1)*10)/60 AS hours_til_expiration
        FROM gui_pendinghtlcs WHERE incoming = $2 AND hash_lock = $3 ORDER BY hash_lock`,
		blockHeight, incoming, hashLock)
}

// paymentHopsPPMSelect is the ppm annotation used by route/routes:
// ppm = ROUND((fee/amt)*1000000) — float division with banker's rounding in Postgres.
const paymentHopsPPMSelect = `SELECT *, ROUND((fee/amt)*1000000)::bigint AS ppm FROM gui_paymenthops`

// handleRoute renders the route detail page: hops for a payment_hash (with ppm,
// total_cost/total_ppm), associated invoice, and pending HTLCs. RPC errors render
// error.html.
func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	blockHeight := int64(info.GetBlockHeight())
	paymentHash := singleQueryParam(r)

	route, err := s.queryMaps(ctx,
		paymentHopsPPMSelect+` WHERE payment_hash_id = $1 ORDER BY attempt_id, step`, paymentHash)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}

	var totalCost float64
	var totalPpm int64
	if len(route) > 0 {
		var sumFee, sumAmtStep1 float64
		for _, h := range route {
			fee, _ := toFloat64(h["fee"])
			sumFee += fee
			if step, _ := toInt64(h["step"]); step == 1 {
				amt, _ := toFloat64(h["amt"])
				sumAmtStep1 += amt
			}
		}
		totalCost = math.RoundToEven(sumFee*1000) / 1000 // round(sum_fee, 3)
		if sumAmtStep1 != 0 {
			totalPpm = int64(totalCost * 1000000 / sumAmtStep1)
		}
	}

	invoices, err := s.queryMaps(ctx, `SELECT * FROM gui_invoices WHERE r_hash = $1`, paymentHash)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	incoming, err := s.htlcsByHashLock(ctx, blockHeight, true, paymentHash)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	outgoing, err := s.htlcsByHashLock(ctx, blockHeight, false, paymentHash)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}

	s.renderTemplate(w, r, "route.html", map[string]any{
		"payment_hash":    paymentHash,
		"total_cost":      totalCost,
		"total_ppm":       totalPpm,
		"route":           route,
		"invoices":        invoices,
		"incoming_htlcs":  incoming,
		"outgoing_htlcs":  outgoing,
		"last_hop_pubkey": r.URL.Query().Get("last_hop_pubkey"),
	})
}

// handleRoutes renders the hops of the last 69 payments that passed through a
// given node_pubkey. RPC errors render error.html.
func (s *Server) handleRoutes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := singleQueryParam(r)
	route, err := s.queryMaps(ctx,
		paymentHopsPPMSelect+` WHERE payment_hash_id IN (
            SELECT payment_hash_id FROM gui_paymenthops WHERE node_pubkey = $1 ORDER BY id DESC LIMIT 69
        ) ORDER BY attempt_id, step`, pubkey)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	s.renderTemplate(w, r, "route.html", map[string]any{
		"payment_hash":    pubkey,
		"route":           route,
		"last_hop_pubkey": r.URL.Query().Get("last_hop_pubkey"),
	})
}
