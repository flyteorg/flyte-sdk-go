package flyte

import (
	"context"
	"fmt"
	"sync"

	"connectrpc.com/connect"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"

	client "github.com/unionai/flyte-sdk-go/flyte/client"
)

// ActionType distinguishes the kinds of actions a run is made of.
type ActionType = workflow.ActionType

const (
	// ActionTypeTask is a task execution (including the run's root action).
	ActionTypeTask = workflow.ActionType_ACTION_TYPE_TASK
	// ActionTypeCondition is a pause point awaiting an external signal.
	ActionTypeCondition = workflow.ActionType_ACTION_TYPE_CONDITION
	// ActionTypeTrace is a terminally-recorded traced function call.
	ActionTypeTrace = workflow.ActionType_ACTION_TYPE_TRACE
)

// ErrorKind classifies an action failure as user- or system-caused.
type ErrorKind = workflow.ErrorInfo_Kind

const (
	ErrorKindUser   = workflow.ErrorInfo_KIND_USER
	ErrorKindSystem = workflow.ErrorInfo_KIND_SYSTEM
)

// Action is a handle to a single action of a run: the root task, a child
// task, a condition, or a trace. Instances returned by RunHandle.ListActions
// carry identity, metadata, and last known status; instances returned by
// RunHandle.GetAction (or after Refresh) additionally carry full details:
// error/abort/signal info and per-attempt records. It is safe for concurrent
// use.
type Action struct {
	clientset *client.RunClientset
	config    Config

	mu sync.RWMutex
	pb *workflow.ActionDetails
}

// ListActions returns all actions of the run, paginating through the full
// list. The results are lightweight; call RunHandle.GetAction or
// Action.Refresh for full details on a specific action.
func (r *RunHandle) ListActions(ctx context.Context) ([]*Action, error) {
	runID := r.pb.GetAction().GetId().GetRun()
	var actions []*Action
	token := ""
	for {
		resp, err := r.clientset.RunServiceClient().ListActions(ctx, connect.NewRequest(&workflow.ListActionsRequest{
			RunId:   runID,
			Request: &common.ListRequest{Limit: 100, Token: token},
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to list actions for run %s: %w", runID.GetName(), err)
		}
		for _, a := range resp.Msg.GetActions() {
			actions = append(actions, &Action{
				clientset: r.clientset,
				config:    r.config,
				pb: &workflow.ActionDetails{
					Id:       a.GetId(),
					Metadata: a.GetMetadata(),
					Status:   a.GetStatus(),
				},
			})
		}
		token = resp.Msg.GetToken()
		if token == "" {
			return actions, nil
		}
	}
}

// GetAction fetches one action of the run by name, with full details.
func (r *RunHandle) GetAction(ctx context.Context, name string) (*Action, error) {
	a := &Action{
		clientset: r.clientset,
		config:    r.config,
		pb: &workflow.ActionDetails{
			Id: &common.ActionIdentifier{Run: r.pb.GetAction().GetId().GetRun(), Name: name},
		},
	}
	if err := a.Refresh(ctx); err != nil {
		return nil, err
	}
	return a, nil
}

// Refresh re-fetches the action's full details, updating the handle in place.
// Use it to poll a live action's phase.
func (a *Action) Refresh(ctx context.Context) error {
	id := a.identifier()
	resp, err := a.clientset.RunServiceClient().GetActionDetails(ctx, connect.NewRequest(&workflow.GetActionDetailsRequest{
		ActionId: id,
	}))
	if err != nil {
		return fmt.Errorf("failed to get action %s details: %w", id.GetName(), err)
	}
	a.mu.Lock()
	a.pb = resp.Msg.GetDetails()
	a.mu.Unlock()
	return nil
}

func (a *Action) identifier() *common.ActionIdentifier {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.pb.GetId()
}

func (a *Action) details() *workflow.ActionDetails {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.pb
}

// Name returns the action name (e.g. "a0" for the root action).
func (a *Action) Name() string { return a.identifier().GetName() }

// RunName returns the name of the run this action belongs to.
func (a *Action) RunName() string { return a.identifier().GetRun().GetName() }

// Type reports whether this is a task, condition, or trace action.
func (a *Action) Type() ActionType { return a.details().GetMetadata().GetActionType() }

// Parent returns the name of the parent action ("" for the root action).
func (a *Action) Parent() string { return a.details().GetMetadata().GetParent() }

// Phase returns the last observed phase, e.g. "ACTION_PHASE_RUNNING". Call
// Refresh (or Wait/Watch) to observe progress.
func (a *Action) Phase() string { return a.phase().String() }

func (a *Action) phase() common.ActionPhase {
	return a.details().GetStatus().GetPhase()
}

// Attempts returns how many attempts have been made so far.
func (a *Action) Attempts() uint32 { return a.details().GetStatus().GetAttempts() }

// RecoveredFrom returns the source action identifier when this action was
// recovered from a previous run, and nil otherwise.
func (a *Action) RecoveredFrom() *common.ActionIdentifier {
	return a.details().GetMetadata().GetRecoveredFrom()
}

// ErrorInfo returns the failure details when the action failed, and nil
// otherwise. Populated on full details (GetAction/Refresh).
func (a *Action) ErrorInfo() *workflow.ErrorInfo { return a.details().GetErrorInfo() }

// AbortInfo returns the abort reason and principal when the action was
// aborted, and nil otherwise. Populated on full details (GetAction/Refresh).
func (a *Action) AbortInfo() *workflow.AbortInfo { return a.details().GetAbortInfo() }

// SignalInfo returns the signal principal and payload when the action is a
// signalled condition, and nil otherwise. Populated on full details
// (GetAction/Refresh).
func (a *Action) SignalInfo() *workflow.SignalInfo { return a.details().GetSignalInfo() }

// AttemptDetails returns the per-attempt records (error info, logs, timings).
// Populated on full details (GetAction/Refresh).
func (a *Action) AttemptDetails() []*workflow.ActionAttempt { return a.details().GetAttempts() }

// Proto exposes the underlying protobuf for advanced use.
func (a *Action) Proto() *workflow.ActionDetails { return a.details() }

// Watch streams status updates until the action reaches a terminal phase,
// with the same semantics as RunHandle.Watch.
func (a *Action) Watch(ctx context.Context) (<-chan RunUpdate, error) {
	return watchActionPhases(ctx, a.clientset, a.identifier(), func(details *workflow.ActionDetails) {
		a.mu.Lock()
		a.pb = details
		a.mu.Unlock()
	})
}

// Wait blocks until the action reaches a terminal phase. It returns nil when
// the action succeeded (or was recovered) and an error describing the outcome
// otherwise.
func (a *Action) Wait(ctx context.Context) error {
	if !isTerminalPhase(a.phase()) {
		updates, err := a.Watch(ctx)
		if err != nil {
			return err
		}
		var last *RunUpdate
		for update := range updates {
			u := update
			last = &u
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if last != nil && last.Error != "" && !isTerminalPhase(a.phase()) {
			return fmt.Errorf("action %s: %s", a.Name(), last.Error)
		}
	}
	switch a.phase() {
	case common.ActionPhase_ACTION_PHASE_SUCCEEDED, common.ActionPhase_ACTION_PHASE_RECOVERED:
		return nil
	case common.ActionPhase_ACTION_PHASE_ABORTED:
		return fmt.Errorf("action %s was aborted", a.Name())
	case common.ActionPhase_ACTION_PHASE_TIMED_OUT:
		return fmt.Errorf("action %s timed out", a.Name())
	case common.ActionPhase_ACTION_PHASE_FAILED:
		return fmt.Errorf("action %s failed: %s", a.Name(), a.ErrorInfo().GetMessage())
	default:
		return fmt.Errorf("action %s ended in unexpected phase %s", a.Name(), a.Phase())
	}
}

// Abort requests termination of this single action; sibling actions keep
// running. The server rejects aborts of trace actions and other non-abortable
// states with a typed Connect error.
func (a *Action) Abort(ctx context.Context, reason string) error {
	_, err := a.clientset.RunServiceClient().AbortAction(ctx, connect.NewRequest(&workflow.AbortActionRequest{
		ActionId: a.identifier(),
		Reason:   reason,
	}))
	if err != nil {
		return fmt.Errorf("failed to abort action %s: %w", a.Name(), err)
	}
	return nil
}
