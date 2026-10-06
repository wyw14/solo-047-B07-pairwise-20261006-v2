package switchover

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
)

// ErrRetryable wraps the interruption that left a failover in StateRetryable.
// Callers distinguish it from a hard failure (StateFailed) with errors.Is.
var ErrRetryable = errors.New("failover interrupted before protection activation; retry is safe")

// routeDrainer and routeActivator are the narrow route collaborators the
// session orchestrates. The concrete *route.Drainer / *route.Activator satisfy
// them; the interfaces also let tests inject device-level faults.
type routeDrainer interface {
	Drain(ctx context.Context, route *route.Route, at time.Time) (route.Snapshot, error)
	Delay() time.Duration
}

type routeActivator interface {
	Protection(ctx context.Context, route *route.Route, at time.Time) (route.Snapshot, error)
}

type SessionService struct {
	drainer      routeDrainer
	activator    routeActivator
	ids          platform.IDGenerator
	now          func() time.Time
	drainTimeout time.Duration
}

func NewSessionService(drainer routeDrainer, activator routeActivator, ids platform.IDGenerator, now func() time.Time, drainTimeout time.Duration) *SessionService {
	return &SessionService{drainer: drainer, activator: activator, ids: ids, now: now, drainTimeout: drainTimeout}
}

func (s *SessionService) Failover(ctx context.Context, target *route.Route) (Snapshot, error) {
	operation := NewOperation(s.ids.New("switch"), target.Snapshot().ID, s.now())
	operation.record(StateDrainingPrimary, "primary-drain", "draining failed primary path", s.now())

	drainTimeout := s.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = s.drainer.Delay()
	}
	// The sentinel cause lets us tell our own grace-window timeout apart from
	// caller cancellation or an inherited deadline, which share context
	// DeadlineExceeded/Canceled at the error level. A zero-delay drain is
	// synchronous, so no deadline is needed.
	drainCtx := ctx
	if drainTimeout > 0 {
		var cancelDrain context.CancelFunc
		drainCtx, cancelDrain = context.WithTimeoutCause(ctx, drainTimeout, route.ErrDrainTimeout)
		defer cancelDrain()
	}
	_, drainErr := s.drainer.Drain(drainCtx, target, s.now())

	switch {
	case drainErr == nil:
		// Fully drained: nothing special to record.
	case ctx.Err() != nil:
		// The caller cancelled or an outer deadline expired. Inspect the
		// parent context rather than the wrapped error: a caller may supply a
		// custom cancel cause, but either way the operation was externally
		// interrupted. The drainer restored the pre-drain route state; do not
		// touch the topology.
		opErr := fmt.Errorf("%w: %v", ErrRetryable, drainErr)
		result := operation.record(StateRetryable, "primary-drain-interrupted", opErr.Error(), s.now())
		return result, opErr
	case errors.Is(drainErr, route.ErrDrainTimeout):
		// Acceptable end of the grace window — the only error condition under
		// which protection activation may proceed.
		operation.record(StateDrainingPrimary, "primary-drain", drainErr.Error(), s.now())
	default:
		// Device-level or otherwise non-recoverable fault on the primary path.
		// A hard failure must never unconditionally move traffic onto another
		// path; leave the topology as-is for operator handling.
		opErr := fmt.Errorf("drain primary path: %w", drainErr)
		result := operation.record(StateFailed, "primary-drain-failed", opErr.Error(), s.now())
		return result, opErr
	}

	// Defence in depth: even when the drain itself tolerated its grace window,
	// an already-cancelled caller must not trigger a topology change.
	if err := ctx.Err(); err != nil {
		opErr := fmt.Errorf("%w: %v", ErrRetryable, err)
		result := operation.record(StateRetryable, "primary-drain-interrupted", opErr.Error(), s.now())
		return result, opErr
	}

	operation.record(StateActivating, "protection-activate", "activating protection path", s.now())
	if _, err := s.activator.Protection(ctx, target, s.now()); err != nil {
		if ctx.Err() != nil {
			opErr := fmt.Errorf("%w: %v", ErrRetryable, err)
			result := operation.record(StateRetryable, "protection-activate-interrupted", opErr.Error(), s.now())
			return result, opErr
		}
		opErr := fmt.Errorf("activate protection: %w", err)
		result := operation.record(StateFailed, "protection-activate", opErr.Error(), s.now())
		return result, opErr
	}
	message := "protection path carries traffic"
	if drainErr != nil {
		message = "protection path carries traffic after primary drain timeout"
	}
	return operation.record(StateProtected, "complete", message, s.now()), nil
}
