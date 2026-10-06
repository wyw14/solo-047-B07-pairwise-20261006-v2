package switchover

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/route"
)

// Drainer is the drain behavior a switchover depends on; *route.Drainer
// satisfies it.
type Drainer interface {
	Drain(ctx context.Context, target *route.Route, at time.Time) (route.Snapshot, error)
	Delay() time.Duration
}

type SessionService struct {
	drainer      Drainer
	activator    *route.Activator
	ids          platform.IDGenerator
	now          func() time.Time
	drainTimeout time.Duration
}

func NewSessionService(drainer Drainer, activator *route.Activator, ids platform.IDGenerator, now func() time.Time, drainTimeout time.Duration) *SessionService {
	return &SessionService{drainer: drainer, activator: activator, ids: ids, now: now, drainTimeout: drainTimeout}
}

// drainFailure classifies how a primary-drain attempt ended, so the
// switchover only crosses the topology boundary when it is safe.
type drainFailure int

const (
	drainFailureNone drainFailure = iota
	// drainFailureTimeout is the acceptable case: our own drain deadline
	// elapsed while the caller is still waiting, so failing over to the
	// protection path is still warranted.
	drainFailureTimeout
	// drainFailureCanceled means the caller went away (cancel or the
	// caller's own deadline); nothing may change on their behalf.
	drainFailureCanceled
	// drainFailureDevice is a hard failure from the drain itself; topology
	// must stay untouched and the operation may be retried.
	drainFailureDevice
)

func classifyDrainError(err error, parent context.Context) drainFailure {
	switch {
	case err == nil:
		return drainFailureNone
	case errors.Is(err, context.Canceled):
		return drainFailureCanceled
	case errors.Is(err, context.DeadlineExceeded) && parent.Err() != nil:
		// The caller's deadline fired, not the drain timeout we imposed.
		return drainFailureCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return drainFailureTimeout
	default:
		return drainFailureDevice
	}
}

func (s *SessionService) Failover(ctx context.Context, target *route.Route) (Snapshot, error) {
	operation := NewOperation(s.ids.New("switch"), target.Snapshot().ID, s.now())
	operation.record(StateDrainingPrimary, "primary-drain", "draining failed primary path", s.now())
	drainTimeout := s.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = s.drainer.Delay()
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, drainTimeout)
	_, drainErr := s.drainer.Drain(drainCtx, target, s.now())
	cancelDrain()
	switch classifyDrainError(drainErr, ctx) {
	case drainFailureCanceled:
		result := operation.record(StateAborted, "primary-drain", "drain aborted by caller, topology unchanged: "+drainErr.Error(), s.now())
		return result, fmt.Errorf("failover aborted: %w", drainErr)
	case drainFailureDevice:
		result := operation.record(StateRetryable, "primary-drain", "drain failed before any topology change, safe to retry: "+drainErr.Error(), s.now())
		return result, fmt.Errorf("drain primary: %w", drainErr)
	case drainFailureTimeout:
		operation.record(StateDrainingPrimary, "primary-drain", "drain timeout elapsed, proceeding to protection: "+drainErr.Error(), s.now())
	}
	operation.record(StateActivating, "protection-activate", "activating protection path", s.now())
	if _, err := s.activator.Protection(ctx, target, s.now()); err != nil {
		result := operation.record(StateFailed, "protection-activate", err.Error(), s.now())
		return result, fmt.Errorf("activate protection: %w", err)
	}
	message := "protection path carries traffic"
	if drainErr != nil {
		message = "protection path carries traffic after primary drain timeout"
	}
	return operation.record(StateProtected, "complete", message, s.now()), nil
}
