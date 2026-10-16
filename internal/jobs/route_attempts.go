package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// routeAttemptRetention is how long route_attempt_log keeps a route attempt
// of the rebalancer; routeAttemptPurgeEvery is how often older rows go.
const (
	routeAttemptRetention  = 30 * 24 * time.Hour
	routeAttemptPurgeEvery = time.Hour
)

type routeAttemptQuerier interface {
	DeleteOldRouteAttempts(ctx context.Context, timestamp pgtype.Timestamptz) error
}

// routeAttemptPurgedAt is the time of the latest purge; the data loop runs the
// jobs one after another, so it needs no lock.
var routeAttemptPurgedAt time.Time

// PurgeRouteAttempts deletes route attempts older than routeAttemptRetention,
// at most once per routeAttemptPurgeEvery. A failed delete is logged and does
// not stop the data run.
func PurgeRouteAttempts(ctx context.Context, q routeAttemptQuerier) {
	purgeRouteAttempts(ctx, q, time.Now())
}

func purgeRouteAttempts(ctx context.Context, q routeAttemptQuerier, now time.Time) {
	if now.Sub(routeAttemptPurgedAt) < routeAttemptPurgeEvery {
		return
	}
	routeAttemptPurgedAt = now
	if err := q.DeleteOldRouteAttempts(ctx, ts(now.Add(-routeAttemptRetention))); err != nil {
		dataLog(fmt.Sprintf("Error deleting old route attempts: %s", err))
	}
}
