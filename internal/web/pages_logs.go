package web

import (
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// controllerLogPath is the path of the controller log file.
// Declared as a var so tests can redirect it.
var controllerLogPath = "/var/log/lndg-controller.log"

// handlePeerEvents renders peerevents.html (the table is populated via JS from
// the API).
func (s *Server) handlePeerEvents(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, r, "peerevents.html", nil)
}

// splitKeepNewline splits a byte slice on '\n', keeping the newline on each line;
// a trailing partial line without '\n' is retained as-is.
func splitKeepNewline(b []byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			out = append(out, string(b[start:i+1]))
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, string(b[start:]))
	}
	return out
}

// readControllerLog reads the last ~200*count bytes of the controller log,
// optionally filters lines by substring, and returns the last count lines.
// Returns (size, lines, error).
func readControllerLog(count int, grep string) (int64, []string, error) {
	info, err := os.Stat(controllerLogPath)
	if err != nil {
		return 0, nil, err
	}
	size := info.Size()
	if size == 0 {
		return size, []string{"Logs are empty...."}, nil
	}
	readSize := int64(200 * count)
	if readSize > size {
		readSize = size
	}
	f, err := os.Open(controllerLogPath)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	if _, err := f.Seek(size-readSize, io.SeekStart); err != nil {
		return 0, nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return 0, nil, err
	}
	logs := []string{}
	for _, line := range splitKeepNewline(data) {
		if grep != "" {
			if strings.Contains(line, grep) {
				logs = append(logs, line)
			}
		} else {
			logs = append(logs, line)
		}
	}
	if count >= 0 && count < len(logs) {
		logs = logs[len(logs)-count:]
	}
	return size, logs, nil
}

// handleLogs renders the logs HTML page, or (format=json) returns JSON
// {size, lines} for live polling.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	jsonFmt := q.Get("format") == "json"

	count := 200
	if t := q.Get("tail"); t != "" {
		n, err := strconv.Atoi(t)
		if err != nil {
			s.logsError(w, r, jsonFmt, err)
			return
		}
		count = n
	}
	grep := q.Get("grep")

	size, logs, err := readControllerLog(count, grep)
	if err != nil {
		s.logsError(w, r, jsonFmt, err)
		return
	}
	if jsonFmt {
		writeJSON(w, http.StatusOK, newOrderedMap().Set("size", size).Set("lines", logs))
		return
	}
	s.renderTemplate(w, r, "logs.html", map[string]any{"logs": logs, "tail": count, "grep": grep})
}

// logsError handles log errors: returns JSON 500 or renders error.html.
func (s *Server) logsError(w http.ResponseWriter, r *http.Request, jsonFmt bool, err error) {
	if jsonFmt {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.renderError(w, r, err.Error())
}

// handleForwards renders forwards.html (the table is populated via JS from the API).
func (s *Server) handleForwards(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, r, "forwards.html", nil)
}

// handleFailedHtlcs renders the top 21 aggregated failed downstream routes from
// the last 7 days (wire_failure=99), grouped by chan_id_in/out. RPC errors render
// error.html. The second table is populated via JS.
func (s *Server) handleFailedHtlcs(w http.ResponseWriter, r *http.Request) {
	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	rows, err := s.queryMaps(r.Context(),
		`SELECT chan_id_in, chan_id_out, count(id) AS count, sum(amount)::bigint AS volume,
                max(chan_in_alias) AS chan_in_alias, max(chan_out_alias) AS chan_out_alias
            FROM gui_failedhtlcs WHERE "timestamp" >= $1 AND wire_failure = 99
            GROUP BY chan_id_in, chan_id_out ORDER BY count DESC LIMIT 21`, cutoff)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	s.renderTemplate(w, r, "failed_htlcs.html", map[string]any{"agg_failed_htlcs": rows})
}

// handleKeysends renders received keysends (invoices with a keysend_preimage set),
// newest settle time first.
func (s *Server) handleKeysends(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(),
		`SELECT * FROM gui_invoices WHERE keysend_preimage IS NOT NULL ORDER BY settle_date DESC`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "keysends.html", map[string]any{"keysends": rows})
}

// queryFeeLog is the shared implementation for autopilot/outbound/inbound fee log
// views: entries from a log table newer than cutoff, optionally filtered by
// chan_id (from "?=<chan_id>"), ordered by -id.
func (s *Server) queryFeeLog(r *http.Request, table string, cutoff time.Time) ([]map[string]any, error) {
	chanID := singleQueryParam(r)
	if chanID == "" {
		return s.queryMaps(r.Context(),
			`SELECT * FROM `+table+` WHERE "timestamp" >= $1 ORDER BY id DESC`, cutoff)
	}
	return s.queryMaps(r.Context(),
		`SELECT * FROM `+table+` WHERE chan_id = $1 AND "timestamp" >= $2 ORDER BY id DESC`, chanID, cutoff)
}

// handleAutopilot renders the autopilot log for the last 21 days.
func (s *Server) handleAutopilot(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryFeeLog(r, "gui_autopilot", time.Now().Add(-21*24*time.Hour))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "autopilot.html", map[string]any{"autopilot": rows})
}

// handleOutboundFeeLog renders the autofees log for the last 7 days.
func (s *Server) handleOutboundFeeLog(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryFeeLog(r, "gui_autofees", time.Now().Add(-7*24*time.Hour))
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	s.renderTemplate(w, r, "outbound_fee_log.html", map[string]any{"outbound_fee_log": rows})
}

// handleInboundFeeLog renders the inbound fee log for the last 7 days.
func (s *Server) handleInboundFeeLog(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryFeeLog(r, "gui_inboundfeelog", time.Now().Add(-7*24*time.Hour))
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	s.renderTemplate(w, r, "inbound_fee_log.html", map[string]any{"inbound_fee_log": rows})
}
