package rebalancer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Combined routes: two saved routes that pass through the same node X give a
// new route made of the first one up to X and the second one from X on. The
// part up to X comes from any saved route (to any target) that worked up to X:
// paid, verified by the prober, or failed only after X. The part from X on
// comes from a paid or verified route to the target. A path the route scout
// verified to a node X of the network is a part up to X as well (any node of
// it, X included, can be the joint). Parts come from routes with evidence
// within partWindow; a combination needs a success chance of at least
// minRouteProb (stats.go). The index is rebuilt every comboRefresh. The fee of
// a combination is estimated from the fees stored with its parts; BuildRoute
// prices it live before it is sent.
const (
	comboRefresh   = 30 * time.Minute
	partWindow     = 7 * 24 * time.Hour
	comboMaxHops   = 10
	comboPerTarget = 200
	combinedLimit  = 20
	// comboSlack lets a combination whose estimated fee is up to 20% over the
	// budget through to the live price check: stored fees can be out of date.
	comboSlack = 1.2
)

type comboQuerier interface {
	ListCombinableRoutes(ctx context.Context) ([]db.GuiRebalanceroute, error)
	ListScoutPaths(ctx context.Context, lastVerified pgtype.Timestamptz) ([]db.ScoutPath, error)
}

// pathEdge is a directed edge between two route nodes.
type pathEdge struct{ from, to string }

// partRoute is a saved route split into what the combination needs: nodes are
// the hop pubkeys (the last one is our node), edgePpm[i] the stored fee of the
// edge nodes[i] -> nodes[i+1], good the number of leading nodes that were
// reached in the latest attempt. A scout part ends at a node of the network
// instead of our node; it is only ever a head.
type partRoute struct {
	id       int64
	target   string
	outChan  string
	nodes    []string
	edgePpm  []float64
	good     int
	evidence time.Time
	scout    bool
}

// comboRoute is a combined route: nodes end with our node.
type comboRoute struct {
	outChan  string
	nodes    []string
	estPpm   float64
	evidence time.Time
}

// comboIndex holds the combined routes per target, rebuilt every comboRefresh.
type comboIndex struct {
	mu       sync.Mutex
	built    time.Time
	byTarget map[string][]comboRoute
}

func newComboIndex() *comboIndex { return &comboIndex{byTarget: map[string][]comboRoute{}} }

// refresh rebuilds the index from the saved routes when it is older than
// comboRefresh. On error it logs and keeps the previous index.
func (c *comboIndex) refresh(ctx context.Context, q comboQuerier, prob func(path string) float64, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.built.IsZero() && now.Sub(c.built) < comboRefresh {
		return
	}
	rows, err := q.ListCombinableRoutes(ctx)
	if err != nil {
		rebalLog(fmt.Sprintf("Error combining routes: %s", err))
		return
	}
	scouts, err := q.ListScoutPaths(ctx, pgtype.Timestamptz{Time: now.Add(-partWindow), Valid: true})
	if err != nil {
		rebalLog(fmt.Sprintf("Error combining routes: %s", err))
		return
	}
	c.byTarget = combineRoutes(rows, scouts, prob, now)
	c.built = now
	n := 0
	for _, rs := range c.byTarget {
		n += len(rs)
	}
	rebalLog(fmt.Sprintf("Combined %d routes for %d targets from %d saved routes and %d scout paths", n, len(c.byTarget), len(rows), len(scouts)))
}

// candidates returns the combined routes of a target over the given sources
// whose estimated fee fits budget(source) (with comboSlack), cheapest first.
func (c *comboIndex) candidates(target string, chanIDs []string, budget func(string) int) []routeCandidate {
	c.mu.Lock()
	defer c.mu.Unlock()
	allowed := make(map[string]bool, len(chanIDs))
	for _, id := range chanIDs {
		allowed[id] = true
	}
	var out []routeCandidate
	for _, cr := range c.byTarget[target] {
		if !allowed[cr.outChan] {
			continue
		}
		b := budget(cr.outChan)
		if b <= 0 || cr.estPpm > float64(b)*comboSlack {
			continue
		}
		out = append(out, routeCandidate{
			ranked: rankedRoute{
				row: db.GuiRebalanceroute{
					TargetPubkey: target, OutgoingChanID: cr.outChan, Route: strings.Join(cr.nodes, "-"),
					LastFeePpm: pgtype.Float8{Float64: cr.estPpm, Valid: true},
				},
				list:     listCombined,
				evidence: cr.evidence,
			},
			carries: true, fee: cr.estPpm, rep: 0.5,
		})
	}
	return out
}

// routeNodes returns the hop pubkeys of a route and the stored fee ppm of each
// edge between them.
func routeNodes(hops []*lnrpc.Hop) (nodes []string, edgePpm []float64) {
	for i, h := range hops {
		nodes = append(nodes, h.GetPubKey())
		if i < len(hops)-1 {
			ppm := 0.0
			if amt := h.GetAmtToForwardMsat(); amt > 0 {
				ppm = float64(h.GetFeeMsat()) * 1e6 / float64(amt)
			}
			edgePpm = append(edgePpm, ppm)
		}
	}
	return nodes, edgePpm
}

// splitScout parses a scout path; ok is false when it holds nothing usable.
func splitScout(s db.ScoutPath) (partRoute, bool) {
	parsed, err := parseRouteHex(s.RouteHex)
	if err != nil || len(parsed.GetHops()) < 2 {
		return partRoute{}, false
	}
	p := partRoute{target: s.TargetPubkey, outChan: s.OutgoingChanID, evidence: tsTime(s.LastVerified), scout: true}
	p.nodes, p.edgePpm = routeNodes(parsed.GetHops())
	p.good = len(p.nodes)
	return p, true
}

// splitRoute parses a saved route; ok is false when it holds nothing usable.
func splitRoute(r db.GuiRebalanceroute, now time.Time) (partRoute, bool) {
	parsed, err := parseRouteHex(r.RouteHex.String)
	if err != nil || len(parsed.GetHops()) < 2 {
		return partRoute{}, false
	}
	p := partRoute{id: r.ID, target: r.TargetPubkey, outChan: r.OutgoingChanID}
	p.nodes, p.edgePpm = routeNodes(parsed.GetHops())
	list, evidence := classifyRoute(r, now)
	if evidence.IsZero() || now.Sub(evidence) > partWindow {
		return partRoute{}, false
	}
	p.evidence = evidence
	switch {
	case list != listFailed:
		p.good = len(p.nodes)
	case r.LastFailureHop.Valid && int(r.LastFailureHop.Int32) >= len(p.nodes):
		p.good = len(p.nodes) // the final node (ours) failed: every edge carried it
	case r.LastFailureHop.Valid && r.LastFailureHop.Int32 >= 1:
		p.good = int(r.LastFailureHop.Int32) // reached the failing node
	default:
		return partRoute{}, false
	}
	return p, true
}

// combineRoutes builds the combined routes per target from saved routes and
// scout paths, at most comboPerTarget per target, cheapest estimated fee first.
// prob is the success chance of a path.
func combineRoutes(rows []db.GuiRebalanceroute, scouts []db.ScoutPath, prob func(path string) float64, now time.Time) map[string][]comboRoute {
	var parts []partRoute
	existing := map[string]bool{} // target|outChan|path of every saved route
	for _, r := range rows {
		existing[r.TargetPubkey+"|"+r.OutgoingChanID+"|"+r.Route] = true
		p, ok := splitRoute(r, now)
		if !ok {
			continue
		}
		parts = append(parts, p)
	}
	for _, s := range scouts {
		if p, ok := splitScout(s); ok {
			parts = append(parts, p)
		}
	}

	// Tails: the part of a fully working route to its target from node X on.
	type tail struct {
		part *partRoute
		from int // index of X in part.nodes
	}
	tails := map[string]map[string][]tail{} // target -> X -> tails
	for i := range parts {
		p := &parts[i]
		if p.scout || p.good < len(p.nodes) {
			continue
		}
		for x := 0; x < len(p.nodes)-1; x++ {
			if tails[p.target] == nil {
				tails[p.target] = map[string][]tail{}
			}
			tails[p.target][p.nodes[x]] = append(tails[p.target][p.nodes[x]], tail{p, x})
		}
	}

	out := map[string][]comboRoute{}
	seen := map[string]bool{}
	for i := range parts {
		head := &parts[i]
		// Heads end at a node the route reached and that is not ours (a scout
		// path does not end at our node).
		last := len(head.nodes) - 1
		if head.scout {
			last = len(head.nodes)
		}
		for x := 0; x < head.good && x < last; x++ {
			joint := head.nodes[x]
			for target, byNode := range tails {
				for _, t := range byNode[joint] {
					if !head.scout && t.part.id == head.id {
						continue
					}
					nodes := make([]string, 0, x+len(t.part.nodes)-t.from)
					nodes = append(nodes, head.nodes[:x+1]...)
					nodes = append(nodes, t.part.nodes[t.from+1:]...)
					if len(nodes) > comboMaxHops || !uniqueNodes(nodes) {
						continue
					}
					path := strings.Join(nodes, "-")
					key := target + "|" + head.outChan + "|" + path
					if existing[key] || seen[key] {
						continue
					}
					if prob(path) < minRouteProb {
						continue
					}
					seen[key] = true
					est := 0.0
					for j := 0; j < x; j++ {
						est += head.edgePpm[j]
					}
					for j := t.from; j < len(t.part.edgePpm); j++ {
						est += t.part.edgePpm[j]
					}
					evidence := head.evidence
					if t.part.evidence.Before(evidence) {
						evidence = t.part.evidence
					}
					out[target] = append(out[target], comboRoute{outChan: head.outChan, nodes: nodes, estPpm: est, evidence: evidence})
				}
			}
		}
	}
	for target, rs := range out {
		sort.SliceStable(rs, func(i, j int) bool {
			if rs[i].estPpm != rs[j].estPpm {
				return rs[i].estPpm < rs[j].estPpm
			}
			return rs[i].evidence.After(rs[j].evidence)
		})
		if len(rs) > comboPerTarget {
			rs = rs[:comboPerTarget]
		}
		out[target] = rs
	}
	return out
}

func uniqueNodes(nodes []string) bool {
	seen := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if seen[n] {
			return false
		}
		seen[n] = true
	}
	return true
}
