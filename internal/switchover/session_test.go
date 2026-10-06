package switchover

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

type stubIDs struct{}

func (stubIDs) New(prefix string) string { return prefix + "-stub" }

func newSession(t *testing.T, drainer routeDrainer, activator routeActivator) (*SessionService, *route.Route, *topology.Registry) {
	t.Helper()
	registry := topology.NewRegistry()
	if _, err := registry.Register("S1", topology.PathPrimary, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	target, err := route.New("R1", "S1", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	service := NewSessionService(drainer, activator, stubIDs{}, func() time.Time { return time.Unix(10, 0) }, 0)
	return service, target, registry
}

func topologyGeneration(t *testing.T, registry *topology.Registry) uint64 {
	t.Helper()
	generation, err := registry.Generation("S1")
	if err != nil {
		t.Fatal(err)
	}
	return generation
}

func TestFailoverProceedsAfterDrainGraceTimeout(t *testing.T) {
	drainer := route.NewDrainer(time.Hour)
	registry := topology.NewRegistry()
	if _, err := registry.Register("S1", topology.PathPrimary, time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	target, err := route.New("R1", "S1", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	activator := route.NewActivator(registry)
	service := NewSessionService(drainer, activator, stubIDs{}, func() time.Time { return time.Unix(10, 0) }, 5*time.Millisecond)

	result, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatalf("grace-window timeout should be tolerated, got %v", err)
	}
	if result.State != StateProtected {
		t.Fatalf("expected protected, got %q", result.State)
	}
	if target.Snapshot().ActivePath != topology.PathProtection {
		t.Fatalf("traffic should move to protection, got %q", target.Snapshot().ActivePath)
	}
	stages := eventStages(result)
	if len(stages) < 3 || stages[0] != "primary-drain" || stages[1] != "primary-drain" {
		t.Fatalf("expected drain start + timeout events before activation, got %v", stages)
	}
}

func TestFailoverCancellationDoesNotChangeTopology(t *testing.T) {
	drainer := route.NewDrainer(time.Hour)
	activator := route.NewActivator(nil) // must never be invoked; a nil registry panics if it is
	service, target, registry := newSession(t, drainer, activator)
	before := topologyGeneration(t, registry)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := service.Failover(ctx, target)

	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("expected ErrRetryable, got %v", err)
	}
	if result.State != StateRetryable {
		t.Fatalf("expected retryable state, got %q", result.State)
	}
	if target.Snapshot().State != route.StatePrimary {
		t.Fatalf("interrupted drain must leave route on primary, got %q", target.Snapshot().State)
	}
	if target.Snapshot().ActivePath != topology.PathPrimary {
		t.Fatalf("interrupted failover must not move traffic, got %q", target.Snapshot().ActivePath)
	}
	if got := topologyGeneration(t, registry); got != before {
		t.Fatalf("topology generation changed %d -> %d", before, got)
	}
	if len(result.Events) != 2 {
		t.Fatalf("expected drain-start and interruption events, got %+v", result.Events)
	}
	if result.Events[1].Stage != "primary-drain-interrupted" {
		t.Fatalf("expected interruption event, got %q", result.Events[1].Stage)
	}
}

type deviceFaultDrainer struct {
	err error
}

func (d deviceFaultDrainer) Drain(context.Context, *route.Route, time.Time) (route.Snapshot, error) {
	return route.Snapshot{}, d.err
}

func (d deviceFaultDrainer) Delay() time.Duration { return 0 }

func TestFailoverDeviceFaultIsHardFailure(t *testing.T) {
	deviceErr := errors.New("optical switch hardware fault")
	service, target, registry := newSession(t, deviceFaultDrainer{err: deviceErr}, route.NewActivator(nil))
	before := topologyGeneration(t, registry)

	result, err := service.Failover(context.Background(), target)
	if err == nil {
		t.Fatal("expected hard failure error")
	}
	if errors.Is(err, ErrRetryable) {
		t.Fatalf("device fault must not be reported as retryable: %v", err)
	}
	if result.State != StateFailed {
		t.Fatalf("expected failed state, got %q", result.State)
	}
	if target.Snapshot().State != route.StatePrimary || target.Snapshot().ActivePath != topology.PathPrimary {
		t.Fatalf("hard failure must leave primary path untouched, got %+v", target.Snapshot())
	}
	if got := topologyGeneration(t, registry); got != before {
		t.Fatalf("hard failure changed topology generation %d -> %d", before, got)
	}
	if result.Events[1].Stage != "primary-drain-failed" {
		t.Fatalf("expected hard-failure event, got %q", result.Events[1].Stage)
	}
}

type faultActivator struct{}

func (faultActivator) Protection(_ context.Context, _ *route.Route, _ time.Time) (route.Snapshot, error) {
	return route.Snapshot{}, errors.New("protection cross-connect refused")
}

func TestFailoverProtectionFaultFailsWithoutClaimingProtected(t *testing.T) {
	// Drain succeeds immediately; activation reports a device fault.
	service, target, registry := newSession(t, route.NewDrainer(0), faultActivator{})
	before := topologyGeneration(t, registry)

	result, err := service.Failover(context.Background(), target)
	if err == nil {
		t.Fatal("expected activation failure")
	}
	if errors.Is(err, ErrRetryable) {
		t.Fatalf("device fault during activation is not retryable: %v", err)
	}
	if result.State != StateFailed {
		t.Fatalf("expected failed state, got %q", result.State)
	}
	if got := topologyGeneration(t, registry); got != before {
		t.Fatalf("generation changed %d -> %d despite failed activation", before, got)
	}
	// Events must tell the complete story: drain -> activating -> failure.
	if len(result.Events) != 3 {
		t.Fatalf("expected three events, got %+v", result.Events)
	}
}

func eventStages(snapshot Snapshot) []string {
	stages := make([]string, len(snapshot.Events))
	for i, event := range snapshot.Events {
		stages[i] = event.Stage
	}
	return stages
}
