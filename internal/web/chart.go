package web

import (
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// chartSQL aggregates daily cost, revenue, and on-chain fees from payments,
// invoices, forwards, on-chain transactions, and channel closures. Each source
// is unioned into a single result set, then grouped by day (truncated to UTC).
// $1 = offset (timestamptz), $2 = time_interval_per_block in seconds (float8);
// both NULL without on-chain records, which leaves the closures out.
const chartSQL = `
SELECT dt, sum(cost) AS cost, sum(revenue) AS revenue, sum(onchain) AS onchain
FROM (
  SELECT date_trunc('day', creation_date AT TIME ZONE 'UTC') AS dt,
         sum(fee)::float8 AS cost, 0.0::float8 AS revenue, 0 AS onchain
    FROM gui_payments WHERE status = 2 GROUP BY 1
  UNION
  SELECT date_trunc('day', settle_date AT TIME ZONE 'UTC'),
         0.0::float8, sum(amt_paid)::float8, 0
    FROM gui_invoices WHERE state = 1 AND is_revenue = true GROUP BY 1
  UNION
  SELECT date_trunc('day', forward_date AT TIME ZONE 'UTC'),
         0.0::float8, sum(fee)::float8, 0
    FROM gui_forwards GROUP BY 1
  UNION
  SELECT date_trunc('day', time_stamp AT TIME ZONE 'UTC'),
         0.0::float8, 0.0::float8, COALESCE(sum(fee), 0)
    FROM gui_onchain GROUP BY 1
  UNION
  SELECT date_trunc('day', ($1::timestamptz + (close_height * ($2 * interval '1 second'))) AT TIME ZONE 'UTC'),
         0.0::float8, 0.0::float8, COALESCE(sum(closing_costs), 0)
    FROM gui_closures WHERE $1::timestamptz IS NOT NULL GROUP BY 1
) t
GROUP BY dt ORDER BY dt`

// handleChart returns a list of {dt, cost, revenue, onchain} per day.
// Closure dates are estimated from the block timing of the first and last
// on-chain records, so closures are only included when there is one.
func (s *Server) handleChart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var offset, intervalSecs any
	var firstDate, lastDate time.Time
	var firstBlock, lastBlock int32
	err := s.db.QueryRow(ctx,
		`SELECT time_stamp, block_height FROM gui_onchain ORDER BY time_stamp ASC LIMIT 1`).
		Scan(&firstDate, &firstBlock)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	default:
		if err := s.db.QueryRow(ctx,
			`SELECT time_stamp, block_height FROM gui_onchain ORDER BY time_stamp DESC LIMIT 1`).
			Scan(&lastDate, &lastBlock); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		interval := 10 * time.Minute
		if lastBlock > firstBlock {
			interval = lastDate.Sub(firstDate) / time.Duration(lastBlock-firstBlock)
		}
		offset = firstDate.Add(-time.Duration(firstBlock) * interval)
		intervalSecs = interval.Seconds()
	}

	rows, err := s.db.Query(ctx, chartSQL, offset, intervalSecs)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	defer rows.Close()

	results := make([]any, 0)
	for rows.Next() {
		var dt time.Time
		var cost, revenue float64
		var onchain int64
		if err := rows.Scan(&dt, &cost, &revenue, &onchain); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		results = append(results, newOrderedMap().
			Set("dt", isoformatUTC(dt)).
			Set("cost", cost).
			Set("revenue", revenue).
			Set("onchain", onchain))
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, results)
}
