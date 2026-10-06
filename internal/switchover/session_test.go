package switchover

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/topology"
)

var fixedNow = time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)

func now() time.Time { return fixedNow }

type sequenceIDs struct{ n int }

func (s *sequenceIDs) New(prefix string) string {
	s.n++
	return fmt.Sprintf("%s-%d", prefix, s.n)
}

type fakeDrainer struct {
	err   error
	delay time.Duration
	calls int
}

func (f *fakeDrainer) Drain(_ context.Context, target *route.Route, _ time.Time) (route.Snapshot, error) {
	f.calls++
	if f.err != nil {
		return target.Snapshot(), f.err
	}
	return target.Snapshot(), nil
}

func (f *fakeDrainer) Delay() time.Duration { return f.delay }

func newTestWorld(t *testing.T) (*topology.Registry, *route.Route) {
	t.Helper()
	registry := topology.NewRegistry()
	if _, err := registry.Register("S1", topology.PathPrimary, fixedNow); err != nil {
		t.Fatal(err)
	}
	target, err := route.New("R1", "S1", fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	return registry, target
}

func assertTopologyUntouched(t *testing.T, registry *topology.Registry, target *route.Route) {
	t.Helper()
	span, err := registry.Snapshot("S1")
	if err != nil {
		t.Fatal(err)
	}
	if span.ActivePath != topology.PathPrimary {
		t.Fatalf("active path=%s, want primary", span.ActivePath)
	}
	if span.Generation != 1 {
		t.Fatalf("generation=%d, want 1 (no topology mutation)", span.Generation)
	}
	routeSnap := target.Snapshot()
	if routeSnap.State != route.StatePrimary || routeSnap.ActivePath != topology.PathPrimary || routeSnap.TrafficBPS != 1_000_000 {
		t.Fatalf("route=%+v, want untouched primary carrying traffic", routeSnap)
	}
}

func TestFailoverSuccessActivatesProtection(t *testing.T) {
	registry, target := newTestWorld(t)
	service := NewSessionService(route.NewDrainer(0), route.NewActivator(registry), &sequenceIDs{}, now, time.Second)

	result, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}
	if result.State != StateProtected {
		t.Fatalf("state=%s, want %s", result.State, StateProtected)
	}
	wantStages := []string{"primary-drain", "protection-activate", "complete"}
	if len(result.Events) != len(wantStages) {
		t.Fatalf("events=%v, want %d stages", result.Events, len(wantStages))
	}
	for i, stage := range wantStages {
		if result.Events[i].Stage != stage {
			t.Fatalf("event %d stage=%s, want %s", i, result.Events[i].Stage, stage)
		}
	}
	span, err := registry.Snapshot("S1")
	if err != nil {
		t.Fatal(err)
	}
	if span.ActivePath != topology.PathProtection {
		t.Fatalf("active path=%s, want protection", span.ActivePath)
	}
	if got := target.Snapshot().State; got != route.StateProtectionActive {
		t.Fatalf("route state=%s, want %s", got, route.StateProtectionActive)
	}
}

func TestFailoverDrainTimeoutStillProtects(t *testing.T) {
	registry, target := newTestWorld(t)
	service := NewSessionService(route.NewDrainer(50*time.Millisecond), route.NewActivator(registry), &sequenceIDs{}, now, 5*time.Millisecond)

	result, err := service.Failover(context.Background(), target)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}
	if result.State != StateProtected {
		t.Fatalf("state=%s, want %s", result.State, StateProtected)
	}
	last := result.Events[len(result.Events)-1]
	if !strings.Contains(last.Message, "after primary drain timeout") {
		t.Fatalf("final message=%q, want timeout note", last.Message)
	}
	span, err := registry.Snapshot("S1")
	if err != nil {
		t.Fatal(err)
	}
	if span.ActivePath != topology.PathProtection {
		t.Fatalf("active path=%s, want protection", span.ActivePath)
	}
}

func TestFailoverCallerCancelAbortsWithoutTopologyChange(t *testing.T) {
	registry, target := newTestWorld(t)
	service := NewSessionService(route.NewDrainer(time.Minute), route.NewActivator(registry), &sequenceIDs{}, now, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := service.Failover(ctx, target)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
	if result.State != StateAborted {
		t.Fatalf("state=%s, want %s", result.State, StateAborted)
	}
	assertTopologyUntouched(t, registry, target)
}

func TestFailoverParentDeadlineAborts(t *testing.T) {
	registry, target := newTestWorld(t)
	service := NewSessionService(route.NewDrainer(time.Minute), route.NewActivator(registry), &sequenceIDs{}, now, time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	result, err := service.Failover(ctx, target)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want context.DeadlineExceeded", err)
	}
	if result.State != StateAborted {
		t.Fatalf("state=%s, want %s", result.State, StateAborted)
	}
	assertTopologyUntouched(t, registry, target)
}

func TestFailoverDeviceFailureIsRetryableAndLeavesTopologyUntouched(t *testing.T) {
	registry, target := newTestWorld(t)
	drainer := &fakeDrainer{err: errors.New("line card rejected drain command")}
	service := NewSessionService(drainer, route.NewActivator(registry), &sequenceIDs{}, now, time.Second)

	result, err := service.Failover(context.Background(), target)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "drain primary") {
		t.Fatalf("err=%v, want drain primary context", err)
	}
	if result.State != StateRetryable {
		t.Fatalf("state=%s, want %s", result.State, StateRetryable)
	}
	assertTopologyUntouched(t, registry, target)
	if drainer.calls != 1 {
		t.Fatalf("drain calls=%d, want 1", drainer.calls)
	}
	last := result.Events[len(result.Events)-1]
	if !strings.Contains(last.Message, "line card rejected drain command") {
		t.Fatalf("last event=%q, want device error recorded", last.Message)
	}
}

func TestOperationSnapshotsKeepEventsIntact(t *testing.T) {
	operation := NewOperation("op-1", "R1", fixedNow)
	first := operation.record(StateDrainingPrimary, "primary-drain", "draining", fixedNow)
	first.Events[0].Message = "corrupted"
	second := operation.record(StateProtected, "complete", "done", fixedNow)

	if got := operation.Snapshot().Events[0].Message; got != "draining" {
		t.Fatalf("stored event mutated to %q", got)
	}
	if len(first.Events) != 1 {
		t.Fatalf("first snapshot has %d events, want 1", len(first.Events))
	}
	if len(second.Events) != 2 {
		t.Fatalf("second snapshot has %d events, want 2", len(second.Events))
	}
	second.Events[1].Message = "corrupted"
	if got := operation.Snapshot().Events[1].Message; got != "done" {
		t.Fatalf("stored event mutated to %q", got)
	}
}

func TestRestoreSuccessCompletes(t *testing.T) {
	registry, target := newTestWorld(t)
	restorer := NewRestorer(route.NewDrainer(0), route.NewActivator(registry), registry, &sequenceIDs{}, now)

	result, err := restorer.Restore(context.Background(), target)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if result.State != StateCompleted {
		t.Fatalf("state=%s, want %s", result.State, StateCompleted)
	}
}

func TestRestoreCallerCancelAbortsWithoutTopologyChange(t *testing.T) {
	registry, target := newTestWorld(t)
	restorer := NewRestorer(route.NewDrainer(time.Minute), route.NewActivator(registry), registry, &sequenceIDs{}, now)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := restorer.Restore(ctx, target)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
	if result.State != StateAborted {
		t.Fatalf("state=%s, want %s", result.State, StateAborted)
	}
	assertTopologyUntouched(t, registry, target)
}

func TestRestoreDeviceFailureIsRetryable(t *testing.T) {
	registry, target := newTestWorld(t)
	drainer := &fakeDrainer{err: errors.New("line card rejected drain command")}
	restorer := NewRestorer(drainer, route.NewActivator(registry), registry, &sequenceIDs{}, now)

	result, err := restorer.Restore(context.Background(), target)
	if err == nil {
		t.Fatal("expected error")
	}
	if result.State != StateRetryable {
		t.Fatalf("state=%s, want %s", result.State, StateRetryable)
	}
	assertTopologyUntouched(t, registry, target)
}
