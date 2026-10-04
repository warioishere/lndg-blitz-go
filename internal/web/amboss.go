package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// handleAmbossChannelFeeHistory fetches fee history for a channel from the Amboss API.
// Uses proper HTTP status codes (400/500/200).
func (s *Server) handleAmbossChannelFeeHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Only GET method allowed"})
		return
	}
	ctx := r.Context()
	q := r.URL.Query()
	channelID := strings.TrimSpace(q.Get("channel_id"))
	timePeriod := q.Get("time_period")
	if timePeriod == "" {
		timePeriod = "1w"
	}
	if channelID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "channel_id parameter required"})
		return
	}
	if !isDigits(channelID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "channel_id must be numeric"})
		return
	}

	apiKey := ""
	if ls, err := s.queries.GetLocalSetting(ctx, "AMB-ApiKey"); err == nil {
		apiKey = ls.Value
	}
	if apiKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Amboss API key not configured"})
		return
	}

	result, err := s.amboss.FetchChannelFeeHistory(ctx, channelID, apiKey, timePeriod)
	if err != nil {
		var apiErr *ambossAPIError
		if errors.As(err, &apiErr) {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": apiErr.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "Internal server error"})
		}
		return
	}

	labels := make([]any, 0, len(result.FeeHistory))
	data := make([]any, 0, len(result.FeeHistory))
	for _, p := range result.FeeHistory {
		labels = append(labels, p.Timestamp)
		data = append(data, p.FeeRateMilliMsat)
	}
	writeJSON(w, http.StatusOK, newOrderedMap().
		Set("channel_id", channelID).
		Set("short_channel_id", result.ShortChannelID).
		Set("labels", labels).
		Set("data", data))
}

// isDigits returns true if s is non-empty and contains only ASCII digits 0-9.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ambossURL is the Amboss GraphQL endpoint.
const ambossURL = "https://api.amboss.space/graphql"

// ambossFeePoint is a single entry in the combined fee history. Fields are kept
// as any to pass through the JSON types returned by Amboss without conversion.
type ambossFeePoint struct {
	Timestamp        any
	FeeRateMilliMsat any
	FeeBaseMsat      any
}

// ambossResult holds the return value of FetchChannelFeeHistory.
type ambossResult struct {
	ChannelID      string
	ShortChannelID any
	FeeHistory     []ambossFeePoint
}

// ambossAPIError represents an HTTP or GraphQL error from the Amboss API.
// Mapped to HTTP 500 with the error message; other errors produce a generic 500.
type ambossAPIError struct{ msg string }

func (e *ambossAPIError) Error() string { return e.msg }

// ambossFetcher encapsulates the external Amboss call (mockable in tests).
type ambossFetcher interface {
	FetchChannelFeeHistory(ctx context.Context, channelID, apiKey, timePeriod string) (ambossResult, error)
}

// httpAmbossFetcher is the real HTTP implementation of ambossFetcher.
type httpAmbossFetcher struct {
	client *http.Client
	url    string
}

func newHTTPAmbossFetcher() *httpAmbossFetcher {
	return &httpAmbossFetcher{client: &http.Client{Timeout: 10 * time.Second}, url: ambossURL}
}

const ambossQuery = `
        query GetEdgePolicy($id: String!, $from: String) {
            getEdge(id: $id) {
                short_channel_id
                graph {
                    policy_history(from: $from) {
                        node1 {
                            list {
                                updated_at
                                fee_base_msat
                                fee_rate_milli_msat
                                max_htlc
                                min_htlc
                            }
                        }
                        node2 {
                            list {
                                updated_at
                                fee_base_msat
                                fee_rate_milli_msat
                                max_htlc
                                min_htlc
                            }
                        }
                    }
                }
            }
        }
    `

type ambossPolicyItem struct {
	UpdatedAt        any `json:"updated_at"`
	FeeBaseMsat      any `json:"fee_base_msat"`
	FeeRateMilliMsat any `json:"fee_rate_milli_msat"`
}

type ambossResponse struct {
	Errors json.RawMessage `json:"errors"`
	Data   struct {
		GetEdge *struct {
			ShortChannelID any `json:"short_channel_id"`
			Graph          struct {
				PolicyHistory struct {
					Node1 struct {
						List []ambossPolicyItem `json:"list"`
					} `json:"node1"`
					Node2 struct {
						List []ambossPolicyItem `json:"list"`
					} `json:"node2"`
				} `json:"policy_history"`
			} `json:"graph"`
		} `json:"getEdge"`
	} `json:"data"`
}

// FetchChannelFeeHistory fetches fee policy history for a channel from Amboss.
func (f *httpAmbossFetcher) FetchChannelFeeHistory(ctx context.Context, channelID, apiKey, timePeriod string) (ambossResult, error) {
	now := time.Now().UTC()
	var fromDate time.Time
	switch timePeriod {
	case "1d":
		fromDate = now.Add(-24 * time.Hour)
	case "1m":
		fromDate = now.Add(-30 * 24 * time.Hour)
	default: // 1w
		fromDate = now.Add(-7 * 24 * time.Hour)
	}
	fromTimestamp := isoformatUTC(fromDate) + "Z"

	payload, _ := json.Marshal(map[string]any{
		"query": ambossQuery,
		"variables": map[string]any{
			"id":   channelID,
			"from": fromTimestamp,
		},
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(payload))
	if err != nil {
		return ambossResult{}, &ambossAPIError{msg: "Error fetching Amboss channel fee history: " + err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		return ambossResult{}, &ambossAPIError{msg: "Error fetching Amboss channel fee history: " + err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		// same text as requests' raise_for_status()
		kind := "Client"
		if resp.StatusCode >= 500 {
			kind = "Server"
		}
		reason := strings.TrimSpace(strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode)))
		return ambossResult{}, &ambossAPIError{msg: fmt.Sprintf("Error fetching Amboss channel fee history: %d %s Error: %s for url: %s",
			resp.StatusCode, kind, reason, f.url)}
	}

	var data ambossResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		// Unexpected decode error — not an Amboss API error, results in a generic 500.
		return ambossResult{}, err
	}

	if hasGraphQLErrors(data.Errors) {
		return ambossResult{}, &ambossAPIError{msg: "Amboss API error: " + string(data.Errors)}
	}

	if data.Data.GetEdge == nil {
		return ambossResult{ChannelID: channelID, ShortChannelID: nil, FeeHistory: []ambossFeePoint{}}, nil
	}

	edge := data.Data.GetEdge
	history := make([]ambossFeePoint, 0)
	for _, item := range edge.Graph.PolicyHistory.Node1.List {
		history = append(history, ambossFeePoint{Timestamp: item.UpdatedAt, FeeRateMilliMsat: item.FeeRateMilliMsat, FeeBaseMsat: item.FeeBaseMsat})
	}
	for _, item := range edge.Graph.PolicyHistory.Node2.List {
		history = append(history, ambossFeePoint{Timestamp: item.UpdatedAt, FeeRateMilliMsat: item.FeeRateMilliMsat, FeeBaseMsat: item.FeeBaseMsat})
	}
	// Sort by timestamp string.
	sort.SliceStable(history, func(i, j int) bool {
		return tsString(history[i].Timestamp) < tsString(history[j].Timestamp)
	})

	return ambossResult{ChannelID: channelID, ShortChannelID: edge.ShortChannelID, FeeHistory: history}, nil
}

func tsString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// hasGraphQLErrors returns true when the errors field is a non-empty, non-null value.
func hasGraphQLErrors(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return false
	}
	switch t := v.(type) {
	case nil:
		return false
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	case string:
		return t != ""
	default:
		return true
	}
}
