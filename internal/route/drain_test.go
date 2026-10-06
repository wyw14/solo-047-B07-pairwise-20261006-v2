package route

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

func TestDrainTimeoutLeavesRouteDraining(t *testing.T) {
	drainer := NewDrainer(time.Hour)
	target, err := New("R1", "S1", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	// The caller bounds the wait with a deadline carrying the drain-timeout
	// cause: the grace window elapsed, which is an acceptable drain outcome.
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Millisecond, ErrDrainTimeout)
	defer cancel()

	snapshot, err := drainer.Drain(ctx, target, time.Unix(1, 0))
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("expected ErrDrainTimeout, got %v", err)
	}
	if snapshot.State != StateDraining {
		t.Fatalf("expected draining state, got %q", snapshot.State)
	}
	if snapshot.ActivePath != topology.PathPrimary || snapshot.TrafficBPS != 0 {
		t.Fatalf("drained route should keep primary path with zero traffic, got %+v", snapshot)
	}
}

func TestDrainCancellationRestoresPreDrainState(t *testing.T) {
	drainer := NewDrainer(time.Hour)
	target, err := New("R1", "S1", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	snapshot, err := drainer.Drain(ctx, target, time.Unix(1, 0))
	if err == nil {
		t.Fatal("expected interruption error")
	}
	if errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("cancellation must not be classified as a drain timeout: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if snapshot.State != StatePrimary {
		t.Fatalf("interrupted drain must restore pre-drain state, got %q", snapshot.State)
	}
	if snapshot.ActivePath != topology.PathPrimary || snapshot.TrafficBPS != 1_000_000 {
		t.Fatalf("interrupted drain must restore pre-drain traffic, got %+v", snapshot)
	}
}

func TestDrainInheritedDeadlineRestoresPreDrainState(t *testing.T) {
	drainer := NewDrainer(time.Hour)
	target, err := New("R1", "S1", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	// An outer deadline (no drain-timeout cause) must read as an interruption.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	snapshot, err := drainer.Drain(ctx, target, time.Unix(1, 0))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("inherited deadline must not look like an acceptable timeout: %v", err)
	}
	if snapshot.State != StatePrimary {
		t.Fatalf("expected restored primary state, got %q", snapshot.State)
	}
}
