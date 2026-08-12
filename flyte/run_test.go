package flyte

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/dataproxy"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/dataproxy/dataproxyconnect"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow/workflowconnect"
)

// fakeDataProxyClient stubs DataProxyServiceClient.GetActionData; every other
// method panics via the embedded nil interface if reached.
type fakeDataProxyClient struct {
	dataproxyconnect.DataProxyServiceClient
	outputs *taskpb.Outputs
	err     error
	calls   int
}

func (f *fakeDataProxyClient) GetActionData(ctx context.Context, req *connect.Request[dataproxy.GetActionDataRequest]) (*connect.Response[dataproxy.GetActionDataResponse], error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&dataproxy.GetActionDataResponse{Outputs: f.outputs}), nil
}

// fakeRunServiceClient stubs RunServiceClient.GetActionData the same way.
type fakeRunServiceClient struct {
	workflowconnect.RunServiceClient
	outputs *taskpb.Outputs
	err     error
	calls   int
}

func (f *fakeRunServiceClient) GetActionData(ctx context.Context, req *connect.Request[workflow.GetActionDataRequest]) (*connect.Response[workflow.GetActionDataResponse], error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&workflow.GetActionDataResponse{Outputs: f.outputs}), nil
}

func namedOutputs(name, value string) *taskpb.Outputs {
	return &taskpb.Outputs{Literals: []*taskpb.NamedLiteral{{
		Name: name,
		Value: &core.Literal{Value: &core.Literal_Scalar{Scalar: &core.Scalar{
			Value: &core.Scalar_Primitive{Primitive: &core.Primitive{
				Value: &core.Primitive_StringValue{StringValue: value},
			}},
		}}},
	}}}
}

func TestFetchOutputs(t *testing.T) {
	actionID := &common.ActionIdentifier{
		Run:  &common.RunIdentifier{Org: "org", Project: "proj", Domain: "dev", Name: "run-1"},
		Name: "a0",
	}
	proxied := namedOutputs("o0", "from-data-proxy")
	legacy := namedOutputs("o0", "from-run-service")

	tests := []struct {
		name        string
		dataProxy   *fakeDataProxyClient
		runService  *fakeRunServiceClient
		want        *taskpb.Outputs
		wantErrCode connect.Code
		wantLegacy  int
	}{
		{
			name:       "data proxy happy path, no fallback",
			dataProxy:  &fakeDataProxyClient{outputs: proxied},
			runService: &fakeRunServiceClient{outputs: legacy},
			want:       proxied,
			wantLegacy: 0,
		},
		{
			name:       "not found falls back to run service (cache-hit / recovered)",
			dataProxy:  &fakeDataProxyClient{err: connect.NewError(connect.CodeNotFound, errors.New("outputs not available"))},
			runService: &fakeRunServiceClient{outputs: legacy},
			want:       legacy,
			wantLegacy: 1,
		},
		{
			name:       "unimplemented falls back to run service (older control plane)",
			dataProxy:  &fakeDataProxyClient{err: connect.NewError(connect.CodeUnimplemented, errors.New("no data proxy"))},
			runService: &fakeRunServiceClient{outputs: legacy},
			want:       legacy,
			wantLegacy: 1,
		},
		{
			name:        "other data proxy errors are not retried on the run service",
			dataProxy:   &fakeDataProxyClient{err: connect.NewError(connect.CodeInternal, errors.New("boom"))},
			runService:  &fakeRunServiceClient{outputs: legacy},
			wantErrCode: connect.CodeInternal,
			wantLegacy:  0,
		},
		{
			name:        "fallback failure surfaces the run service error",
			dataProxy:   &fakeDataProxyClient{err: connect.NewError(connect.CodeNotFound, errors.New("outputs not available"))},
			runService:  &fakeRunServiceClient{err: connect.NewError(connect.CodeNotFound, errors.New("run data not available"))},
			wantErrCode: connect.CodeNotFound,
			wantLegacy:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fetchOutputs(context.Background(), tt.dataProxy, tt.runService, actionID)
			if tt.wantErrCode != 0 {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, connect.CodeOf(err))
			} else {
				require.NoError(t, err)
				assert.True(t, proto.Equal(tt.want, got), "got %v", got)
			}
			assert.Equal(t, 1, tt.dataProxy.calls, "data proxy calls")
			assert.Equal(t, tt.wantLegacy, tt.runService.calls, "run service calls")
		})
	}
}

func TestIsSuccessPhase(t *testing.T) {
	success := []common.ActionPhase{
		common.ActionPhase_ACTION_PHASE_SUCCEEDED,
		common.ActionPhase_ACTION_PHASE_RECOVERED,
	}
	for _, p := range success {
		assert.True(t, isSuccessPhase(p), p.String())
	}
	notSuccess := []common.ActionPhase{
		common.ActionPhase_ACTION_PHASE_UNSPECIFIED,
		common.ActionPhase_ACTION_PHASE_QUEUED,
		common.ActionPhase_ACTION_PHASE_RUNNING,
		common.ActionPhase_ACTION_PHASE_FAILED,
		common.ActionPhase_ACTION_PHASE_ABORTED,
		common.ActionPhase_ACTION_PHASE_TIMED_OUT,
	}
	for _, p := range notSuccess {
		assert.False(t, isSuccessPhase(p), p.String())
	}
}

// terminalRunHandle builds a RunHandle already in a terminal phase, without a
// clientset: the phase-gate tests below never get far enough to need one.
func terminalRunHandle(phase common.ActionPhase) *RunHandle {
	return &RunHandle{
		pb: &workflow.Run{Action: &workflow.Action{
			Id: &common.ActionIdentifier{
				Run:  &common.RunIdentifier{Org: "org", Project: "proj", Domain: "dev", Name: "run-1"},
				Name: "a0",
			},
		}},
		phase: phase,
	}
}

func TestOutputsPhaseGate(t *testing.T) {
	t.Run("failed run is rejected", func(t *testing.T) {
		r := terminalRunHandle(common.ActionPhase_ACTION_PHASE_FAILED)
		_, err := r.Outputs(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "did not succeed")
	})

	// A RECOVERED run must pass the success gate: with no task spec resolved
	// the next check fails, proving the phase itself was accepted.
	t.Run("recovered run is accepted", func(t *testing.T) {
		r := terminalRunHandle(common.ActionPhase_ACTION_PHASE_RECOVERED)
		_, err := r.Outputs(context.Background())
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "did not succeed")
		assert.Contains(t, err.Error(), "no resolved task spec")
	})

	t.Run("succeeded run is accepted", func(t *testing.T) {
		r := terminalRunHandle(common.ActionPhase_ACTION_PHASE_SUCCEEDED)
		_, err := r.Outputs(context.Background())
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "did not succeed")
		assert.Contains(t, err.Error(), "no resolved task spec")
	})
}
