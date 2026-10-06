package route

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrDrainTimeout marks the expected end of the drain grace window. It is the
// only drain outcome a failover may treat as safe to continue from: traffic has
// been held off long enough and the route is left draining. Cancellation and
// inherited deadlines are interruptions, not this condition.
var ErrDrainTimeout = errors.New("drain grace window timed out")

type Drainer struct {
	delay time.Duration
}

func NewDrainer(delay time.Duration) *Drainer {
	return &Drainer{delay: delay}
}

// Drain moves the route into the draining state and waits out the grace window.
//
// Outcomes:
//   - nil: the window elapsed on schedule and the route is fully drained.
//   - an error wrapping ErrDrainTimeout: the configured window elapsed while the
//     caller bounded the wait with a deadline; the route stays draining and a
//     failover may proceed to activate the protection path.
//   - any other error: the wait was interrupted (cancellation, an inherited
//     deadline, or a device-level fault). The route is restored to its pre-drain
//     state so an interrupted drain cannot silently change traffic topology.
func (d *Drainer) Drain(ctx context.Context, route *Route, at time.Time) (Snapshot, error) {
	before := route.Snapshot()
	select {
	case <-ctx.Done():
		cause := context.Cause(ctx)
		if errors.Is(cause, ErrDrainTimeout) {
			return route.update(StateDraining, before.ActivePath, 0, at),
				fmt.Errorf("route drain grace window elapsed: %w", ErrDrainTimeout)
		}
		if cause == nil {
			cause = ctx.Err()
		}
		return before, fmt.Errorf("route drain interrupted before it started: %w", cause)
	default:
	}
	route.update(StateDraining, before.ActivePath, before.TrafficBPS, at)
	if d.delay > 0 {
		timer := time.NewTimer(d.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			cause := context.Cause(ctx)
			at = at.Add(d.delay)
			if errors.Is(cause, ErrDrainTimeout) {
				// The grace window elapsed under the caller's deadline: keep the
				// draining posture, which is the safe basis for protection.
				return route.update(StateDraining, before.ActivePath, 0, at),
					fmt.Errorf("route drain grace window elapsed: %w", ErrDrainTimeout)
			}
			// Interrupted by cancellation, an inherited deadline, or a device
			// fault: restore the pre-drain posture so traffic topology is not
			// silently changed by an incomplete drain.
			if cause == nil {
				cause = ctx.Err()
			}
			return route.update(before.State, before.ActivePath, before.TrafficBPS, at),
				fmt.Errorf("route drain interrupted: %w", cause)
		case <-timer.C:
		}
	}
	return route.update(StateDraining, before.ActivePath, 0, at.Add(d.delay)), nil
}

func (d *Drainer) Delay() time.Duration {
	return d.delay
}
