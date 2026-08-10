package flyte

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
)

func TestActionAccessors(t *testing.T) {
	a := &Action{pb: &workflow.ActionDetails{
		Id: &common.ActionIdentifier{
			Run:  &common.RunIdentifier{Org: "org", Project: "proj", Domain: "dev", Name: "run-1"},
			Name: "a1",
		},
		Metadata: &workflow.ActionMetadata{
			Parent:     "a0",
			ActionType: ActionTypeCondition,
			RecoveredFrom: &common.ActionIdentifier{
				Run:  &common.RunIdentifier{Name: "run-0"},
				Name: "a1",
			},
		},
		Status: &workflow.ActionStatus{
			Phase:    common.ActionPhase_ACTION_PHASE_FAILED,
			Attempts: 3,
		},
		Result: &workflow.ActionDetails_ErrorInfo{
			ErrorInfo: &workflow.ErrorInfo{Message: "boom", Kind: ErrorKindUser},
		},
	}}

	assert.Equal(t, "a1", a.Name())
	assert.Equal(t, "run-1", a.RunName())
	assert.Equal(t, "a0", a.Parent())
	assert.Equal(t, ActionTypeCondition, a.Type())
	assert.Equal(t, "ACTION_PHASE_FAILED", a.Phase())
	assert.Equal(t, uint32(3), a.Attempts())
	assert.Equal(t, "run-0", a.RecoveredFrom().GetRun().GetName())
	assert.Equal(t, "boom", a.ErrorInfo().GetMessage())
	assert.Equal(t, ErrorKindUser, a.ErrorInfo().GetKind())
	assert.Nil(t, a.AbortInfo())
	assert.Nil(t, a.SignalInfo())
}

func TestIsTerminalPhase(t *testing.T) {
	terminal := []common.ActionPhase{
		common.ActionPhase_ACTION_PHASE_SUCCEEDED,
		common.ActionPhase_ACTION_PHASE_FAILED,
		common.ActionPhase_ACTION_PHASE_ABORTED,
		common.ActionPhase_ACTION_PHASE_TIMED_OUT,
		common.ActionPhase_ACTION_PHASE_RECOVERED,
	}
	for _, p := range terminal {
		assert.True(t, isTerminalPhase(p), p.String())
	}
	nonTerminal := []common.ActionPhase{
		common.ActionPhase_ACTION_PHASE_UNSPECIFIED,
		common.ActionPhase_ACTION_PHASE_QUEUED,
		common.ActionPhase_ACTION_PHASE_INITIALIZING,
		common.ActionPhase_ACTION_PHASE_WAITING_FOR_RESOURCES,
		common.ActionPhase_ACTION_PHASE_RUNNING,
		common.ActionPhase_ACTION_PHASE_PAUSED,
	}
	for _, p := range nonTerminal {
		assert.False(t, isTerminalPhase(p), p.String())
	}
}
