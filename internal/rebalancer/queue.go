package rebalancer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// queueQuerier bundles all DB access needed by the queue orchestration.
type queueQuerier interface {
	rebalancerQuerier
	scheduleQuerier
	autoEnableQuerier
	ListPendingRebalances(ctx context.Context) ([]db.GuiRebalancer, error)
	MarkInFlightRebalancesError(ctx context.Context, stop pgtype.Timestamptz) error
}

// QueueDeps bundles the dependencies of the rebalancer queue.
// Sleep is injectable for tests.
type QueueDeps struct {
	Engine *engine
	Stub   rebalLightningClient
	Router rebalRouterClient
	Q      queueQuerier
	Now    func() time.Time
	Sleep  func(time.Duration)
}

// coordinator manages the pending queue and active-job tracking.
// A mutex ensures that take() (pop + mark active) is atomic with respect to idleAndEmpty().
type coordinator struct {
	mu        sync.Mutex
	queue     []*db.GuiRebalancer
	scheduled map[int64]struct{}
	active    map[int64]struct{}
	shutdown  bool
}

func newCoordinator() *coordinator {
	return &coordinator{scheduled: map[int64]struct{}{}, active: map[int64]struct{}{}}
}

// reset clears all queue and tracking state and resets the shutdown flag.
func (c *coordinator) reset() {
	c.mu.Lock()
	c.queue = nil
	c.scheduled = map[int64]struct{}{}
	c.active = map[int64]struct{}{}
	c.shutdown = false
	c.mu.Unlock()
}

// tryEnqueue adds a rebalance to the queue if it is not already scheduled or active.
func (c *coordinator) tryEnqueue(r *db.GuiRebalancer) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.scheduled[r.ID]; ok {
		return false
	}
	if _, ok := c.active[r.ID]; ok {
		return false
	}
	c.scheduled[r.ID] = struct{}{}
	c.queue = append(c.queue, r)
	return true
}

// enqueueScheduled unconditionally enqueues an auto-schedule result.
func (c *coordinator) enqueueScheduled(r *db.GuiRebalancer) {
	c.mu.Lock()
	c.scheduled[r.ID] = struct{}{}
	c.queue = append(c.queue, r)
	c.mu.Unlock()
}

// take atomically pops the next item from the queue and marks it as active.
func (c *coordinator) take() (*db.GuiRebalancer, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return nil, false
	}
	r := c.queue[0]
	c.queue = c.queue[1:]
	delete(c.scheduled, r.ID)
	c.active[r.ID] = struct{}{}
	return r, true
}

func (c *coordinator) done(id int64) {
	c.mu.Lock()
	delete(c.active, id)
	c.mu.Unlock()
}

// idleAndEmpty returns true when both the queue and the active set are empty.
func (c *coordinator) idleAndEmpty() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue) == 0 && len(c.active) == 0
}

func (c *coordinator) setShutdown() {
	c.mu.Lock()
	c.shutdown = true
	c.mu.Unlock()
}

func (c *coordinator) isShutdown() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.shutdown
}

// getDispatchInterval reads AR-DispatchInterval (default 5).
func getDispatchInterval(ctx context.Context, q settingsQuerier) int {
	v, _ := getLocalSettingInt(ctx, q, "AR-DispatchInterval", 5)
	return v
}

// getWorkerCount reads AR-Workers (default 1).
func getWorkerCount(ctx context.Context, q settingsQuerier) int {
	v, _ := getLocalSettingInt(ctx, q, "AR-Workers", 1)
	return v
}

// runManager is the queue manager loop: it polls for pending rebalances,
// runs auto-enable and auto-schedule, and shuts down when the queue is empty.
func (d QueueDeps) runManager(ctx context.Context, c *coordinator) {
	rebalLog("Queue manager is starting...")
	for {
		if c.isShutdown() || ctx.Err() != nil {
			rebalLog("Queue manager has shut down...")
			return
		}
		c.mu.Lock()
		qsize := len(c.queue)
		acount := len(c.active)
		c.mu.Unlock()
		rebalLog(fmt.Sprintf("Queue currently has %d items...", qsize))
		rebalLog(fmt.Sprintf("There are currently %d tasks in progress...", acount))
		rebalLog("Queue manager is checking for more work...")
		dispatch := getDispatchInterval(ctx, d.Q)

		pending, err := d.Q.ListPendingRebalances(ctx)
		if err != nil {
			rebalLog(fmt.Sprintf("Queue manager exception: %s", err))
			c.setShutdown()
			rebalLog("Queue manager has shut down...")
			return
		}
		for i := range pending {
			r := pending[i]
			if c.tryEnqueue(&r) {
				rebalLog(fmt.Sprintf("Found a pending job to schedule with id: %d", r.ID))
				if dispatch > 0 {
					d.Sleep(time.Duration(dispatch) * time.Second)
				}
			}
		}

		autoEnable(ctx, d.Q, d.Now)
		scheduled := d.Engine.autoSchedule(ctx, d.Q, d.Now)
		if len(scheduled) > 0 {
			rebalLog(fmt.Sprintf("Scheduling %d more jobs...", len(scheduled)))
			for _, r := range scheduled {
				c.enqueueScheduled(r)
				if dispatch > 0 {
					d.Sleep(time.Duration(dispatch) * time.Second)
				}
			}
		} else if c.idleAndEmpty() {
			rebalLog("Queue is still empty, stopping the rebalancer...")
			c.setShutdown()
			rebalLog("Queue manager has shut down...")
			return
		}
		d.Sleep(30 * time.Second)
	}
}

// runWorker is a single rebalancer worker: it dequeues jobs and runs them until shutdown.
func (d QueueDeps) runWorker(ctx context.Context, c *coordinator, worker string) {
	for {
		if c.isShutdown() || ctx.Err() != nil {
			return
		}
		r, ok := c.take()
		if ok {
			rebalLog(fmt.Sprintf("%s is starting a new request...", worker))
			cur := r
			for cur != nil && ctx.Err() == nil {
				cur = d.Engine.runRebalancer(ctx, d.Stub, d.Router, d.Q, cur, worker, d.Now)
			}
			c.done(r.ID)
			rebalLog(fmt.Sprintf("%s completed its request...", worker))
		} else if c.isShutdown() {
			return
		}
		d.Sleep(3 * time.Second)
	}
}

// runSession starts the manager and N workers, then waits until all have stopped.
func (d QueueDeps) runSession(ctx context.Context, c *coordinator, workerCount int) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); d.runManager(ctx, c) }()
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(n int) { defer wg.Done(); d.runWorker(ctx, c, fmt.Sprintf("Worker %d", n+1)) }(i)
	}
	wg.Wait()
	rebalLog("Manager and workers have stopped...")
}

// Run is the outer rebalancer loop. It monitors the AR-Workers setting and
// restarts the session when the worker count changes. Runs until ctx is cancelled.
func (d QueueDeps) Run(ctx context.Context) {
	rebalLog("Rebalancer initializing...")
	c := newCoordinator()
	var desired atomic.Int64
	desired.Store(int64(getWorkerCount(ctx, d.Q)))

	// Monitor AR-Workers and trigger a session restart when the count changes.
	go func() {
		for ctx.Err() == nil {
			d.Sleep(20 * time.Second)
			if ctx.Err() != nil {
				return
			}
			wc := getWorkerCount(ctx, d.Q)
			if int64(wc) != desired.Load() {
				desired.Store(int64(wc))
				c.setShutdown()
				rebalLog("New worker count detected...restarting rebalancer")
			}
		}
	}()

	for ctx.Err() == nil {
		c.reset()
		if err := d.Q.MarkInFlightRebalancesError(ctx, tsNow(d.Now)); err != nil {
			rebalLog(fmt.Sprintf("Rebalancer loop error: %s", err))
		}
		d.runSession(ctx, c, int(desired.Load()))
		rebalLog("Rebalancer successfully exited...sleeping for 20 seconds")
		d.Sleep(20 * time.Second)
	}
	rebalLog("Rebalancer loop has been terminated")
}

// NewQueueDeps constructs a QueueDeps from concrete clients and a querier for
// the cmd/rebalancer entry point. sleep is injectable.
func NewQueueDeps(q queueQuerier, stub lnrpc.LightningClient, router routerrpc.RouterClient, now func() time.Time, sleep func(time.Duration)) QueueDeps {
	return QueueDeps{
		Engine: newEngine(),
		Stub:   stub,
		Router: router,
		Q:      q,
		Now:    now,
		Sleep:  sleep,
	}
}
