package flyte

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
)

func TestEventPayload(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  any
	}{
		{"bool", true, true},
		{"string", "yes", "yes"},
		{"int", 42, int64(42)},
		{"int8", int8(-3), int64(-3)},
		{"int16", int16(7), int64(7)},
		{"int32", int32(7), int64(7)},
		{"int64", int64(7), int64(7)},
		{"uint", uint(7), int64(7)},
		{"uint8", uint8(7), int64(7)},
		{"uint16", uint16(7), int64(7)},
		{"uint32", uint32(7), int64(7)},
		{"uint64", uint64(7), int64(7)},
		{"float32", float32(1.5), 1.5},
		{"float64", 2.5, 2.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := eventPayload(tt.value)
			require.NoError(t, err)
			switch want := tt.want.(type) {
			case bool:
				assert.Equal(t, want, p.GetBoolValue())
			case string:
				assert.Equal(t, want, p.GetStringValue())
			case int64:
				assert.Equal(t, want, p.GetIntValue())
			case float64:
				assert.Equal(t, want, p.GetFloatValue())
			}
		})
	}

	for _, unsupported := range []any{nil, []int{1}, map[string]int{}, struct{}{}} {
		_, err := eventPayload(unsupported)
		assert.Error(t, err)
	}
}

func TestConditionSpecAccessors(t *testing.T) {
	c := &Condition{Action: &Action{pb: &workflow.ActionDetails{
		Metadata: &workflow.ActionMetadata{ActionType: ActionTypeCondition},
		Spec: &workflow.ActionDetails_Condition{
			Condition: &workflow.ConditionAction{
				Prompt:      "Approve?",
				Description: "release gate",
			},
		},
	}}}
	assert.Equal(t, "Approve?", c.Prompt())
	assert.Equal(t, "release gate", c.Description())
}
