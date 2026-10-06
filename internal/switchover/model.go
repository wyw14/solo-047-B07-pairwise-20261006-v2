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
	// StateFailed is terminal after topology changes may have begun; the
	// operation must not be retried blindly.
	StateFailed State = "failed"
	// StateAborted is terminal before any topology change: the caller
	// canceled, so the operation stopped where it was.
	StateAborted State = "aborted"
	// StateRetryable is terminal before any topology change: a hard failure
	// stopped the operation and it is safe to issue it again.
	StateRetryable State = "retryable"
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
	return o.isolated()
}

func (o *Operation) record(state State, stage, message string, at time.Time) Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.snapshot.State = state
	o.snapshot.Events = append(o.snapshot.Events, Event{Stage: stage, Message: message, At: at.UTC()})
	o.snapshot.UpdatedAt = at.UTC()
	return o.isolated()
}

// isolated returns a copy of the snapshot whose Events slice shares no
// backing array with the operation, so returned snapshots stay intact as
// later events are recorded.
func (o *Operation) isolated() Snapshot {
	result := o.snapshot
	result.Events = append([]Event(nil), result.Events...)
	return result
}
