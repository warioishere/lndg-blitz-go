package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRouteAttemptQ struct{ before []time.Time }

func (f *fakeRouteAttemptQ) DeleteOldRouteAttempts(ctx context.Context, timestamp pgtype.Timestamptz) error {
	f.before = append(f.before, timestamp.Time)
	return nil
}

func TestPurgeRouteAttemptsHourly(t *testing.T) {
	routeAttemptPurgedAt = time.Time{}
	t.Cleanup(func() { routeAttemptPurgedAt = time.Time{} })
	q := &fakeRouteAttemptQ{}
	now := time.Unix(1_700_000_000, 0)
	purgeRouteAttempts(context.Background(), q, now)
	purgeRouteAttempts(context.Background(), q, now.Add(59*time.Minute))
	purgeRouteAttempts(context.Background(), q, now.Add(time.Hour))
	require.Len(t, q.before, 2, "once per hour")
	assert.Equal(t, now.Add(-30*24*time.Hour), q.before[0], "rows older than 30 days go")
	assert.Equal(t, now.Add(time.Hour-30*24*time.Hour), q.before[1])
}
