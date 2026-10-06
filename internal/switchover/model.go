package switchover

import (
	"sync"
	"time"
)

type State string

const (
	StateCreated         State = "created"
	StateDrainingPrimary State = "draining-primary"
	StateActivating      State = "activating-protection"
	StateProtected       State = "protected"
	StateRestoring       State = "restoring-primary"
	StateCompleted       State = "completed"
	// StateRetryable marks an operation that stopped before any topology
	// change because it was interrupted; reissuing it is safe.
	StateRetryable State = "retryable"
	// StateFailed marks a non-recoverable device/topology fault. Topology was
	// not changed; operator intervention is required.
	StateFailed State = "failed"
)

type Event struct {
	Stage   string    `json:"stage"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

type Snapshot struct {
	ID        string    `json:"id"`
	RouteID   string    `json:"route_id"`
	State     State     `json:"state"`
	Events    []Event   `json:"events"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Operation struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func NewOperation(id, routeID string, at time.Time) *Operation {
	return &Operation{snapshot: Snapshot{ID: id, RouteID: routeID, State: StateCreated, UpdatedAt: at.UTC()}}
}

func (o *Operation) Snapshot() Snapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()
	result := o.snapshot
	result.Events = append([]Event(nil), result.Events...)
	return result
}

func (o *Operation) record(state State, stage, message string, at time.Time) Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshot.State = state
	o.snapshot.Events = append(o.snapshot.Events, Event{Stage: stage, Message: message, At: at.UTC()})
	o.snapshot.UpdatedAt = at.UTC()
	return o.snapshot
}
