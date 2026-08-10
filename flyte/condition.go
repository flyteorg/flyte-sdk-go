package flyte

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
)

// Condition is a condition action: a pause point created by a task via
// flyte.new_condition(...) that waits for an external signal. It wraps an
// Action fetched with full details, so the spec (prompt, expected type,
// timeout) and signal info are available.
type Condition struct {
	*Action
}

// ListConditions returns the run's condition actions with full details.
func (r *RunHandle) ListConditions(ctx context.Context) ([]*Condition, error) {
	actions, err := r.ListActions(ctx)
	if err != nil {
		return nil, err
	}
	var conditions []*Condition
	for _, a := range actions {
		if a.Type() != ActionTypeCondition {
			continue
		}
		// Re-fetch for full details: the condition spec and signal info are
		// not part of the listing.
		if err := a.Refresh(ctx); err != nil {
			return nil, err
		}
		conditions = append(conditions, &Condition{Action: a})
	}
	return conditions, nil
}

// GetCondition fetches one condition action of the run by name.
func (r *RunHandle) GetCondition(ctx context.Context, name string) (*Condition, error) {
	a, err := r.GetAction(ctx, name)
	if err != nil {
		return nil, err
	}
	if a.Type() != ActionTypeCondition {
		return nil, fmt.Errorf("action %s is a %s, not a condition", name, a.Type())
	}
	return &Condition{Action: a}, nil
}

func (c *Condition) spec() *workflow.ConditionAction {
	return c.details().GetCondition()
}

// Prompt returns the prompt shown to the user when the condition is awaited.
func (c *Condition) Prompt() string { return c.spec().GetPrompt() }

// Description returns the condition's description.
func (c *Condition) Description() string { return c.spec().GetDescription() }

// Signal delivers the value the condition is waiting for, resuming the paused
// task. Supported value types are bool, string, signed/unsigned integers, and
// floats — matching the condition data types the platform supports. The server
// validates the value against the condition's declared type and rejects
// mismatches, double signals, and non-condition targets with typed Connect
// errors.
func (c *Condition) Signal(ctx context.Context, value any) error {
	payload, err := eventPayload(value)
	if err != nil {
		return fmt.Errorf("cannot signal condition %s: %w", c.Name(), err)
	}
	_, err = c.clientset.RunServiceClient().SignalEvent(ctx, connect.NewRequest(&workflow.SignalEventRequest{
		ActionId:         c.identifier(),
		ParentActionName: c.Parent(),
		Payload:          payload,
	}))
	if err != nil {
		return fmt.Errorf("failed to signal condition %s: %w", c.Name(), err)
	}
	return nil
}

// eventPayload converts a native Go value to the signal payload wire format.
func eventPayload(value any) (*workflow.EventPayload, error) {
	p := &workflow.EventPayload{}
	switch v := value.(type) {
	case bool:
		p.Value = &workflow.EventPayload_BoolValue{BoolValue: v}
	case string:
		p.Value = &workflow.EventPayload_StringValue{StringValue: v}
	case int:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case int8:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case int16:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case int32:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case int64:
		p.Value = &workflow.EventPayload_IntValue{IntValue: v}
	case uint:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case uint8:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case uint16:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case uint32:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case uint64:
		p.Value = &workflow.EventPayload_IntValue{IntValue: int64(v)}
	case float32:
		p.Value = &workflow.EventPayload_FloatValue{FloatValue: float64(v)}
	case float64:
		p.Value = &workflow.EventPayload_FloatValue{FloatValue: v}
	default:
		return nil, fmt.Errorf("unsupported signal value type %T (want bool, string, integer, or float)", value)
	}
	return p, nil
}
