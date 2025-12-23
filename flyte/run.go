package flyte

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
	taskpb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/task"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/workflow"
)

// runRef implements the Run interface for remote task executions.
// It provides methods to monitor progress, wait for completion, and retrieve results.
//
// Users should obtain runRef instances through Execute() or ExecuteWithContext(),
// not by creating them directly.
type runRef struct {
	// name is the unique run identifier
	name string

	// url is the web console URL for this run
	url string

	// pb is the protobuf run definition
	pb *workflow.Run

	// client is the gRPC client for communication
	client *client

	// phase is protected by mutex for thread-safety
	mu    sync.RWMutex
	phase string
}

// GetName implements Run interface
func (r *runRef) GetName() string {
	return r.name
}

// GetURL implements Run interface
func (r *runRef) GetURL() string {
	return r.url
}

// GetPhase implements Run interface
func (r *runRef) GetPhase() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.phase
}

// updatePhase updates the phase (thread-safe)
func (r *runRef) updatePhase(newPhase string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phase = newPhase
}

// Wait implements Run interface.
// It blocks until the run reaches a terminal state (success, failure, etc.)
//
// Returns:
//   - nil if the run succeeds
//   - error if the run fails, is aborted, times out, or if watching fails
//
// Example:
//
//	run, err := flyte.Execute(ctx, task, inputs)
//	if err != nil {
//	    return err
//	}
//
//	if err := run.Wait(ctx); err != nil {
//	    log.Printf("Run failed: %v", err)
//	    return err
//	}
//	log.Println("Run succeeded!")
func (r *runRef) Wait(ctx context.Context) error {
	if r.pb == nil || r.pb.Action == nil || r.pb.Action.Id == nil || r.pb.Action.Id.Run == nil {
		return fmt.Errorf("invalid run: missing identifiers")
	}

	runID := r.pb.Action.Id.Run

	// Stream updates using WatchRunDetails
	stream, err := r.client.runService.WatchRunDetails(ctx, &workflow.WatchRunDetailsRequest{
		RunId: runID,
	})
	if err != nil {
		return fmt.Errorf("failed to watch run: %w", err)
	}

	// Process updates until terminal state
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error watching run: %w", err)
		}

		if update.Details == nil || update.Details.Action == nil || update.Details.Action.Status == nil {
			continue
		}

		// Update phase
		phase := update.Details.Action.Status.Phase
		r.updatePhase(phase.String())

		// Check if terminal
		if isTerminalPhase(phase) {
			// Check for failure
			if phase == common.ActionPhase_ACTION_PHASE_FAILED {
				errorMsg := "run failed"
				if errorInfo := update.Details.Action.GetErrorInfo(); errorInfo != nil {
					errorMsg = fmt.Sprintf("run failed: %s", errorInfo.Message)
				}
				return fmt.Errorf(errorMsg)
			}
			if phase == common.ActionPhase_ACTION_PHASE_ABORTED {
				return fmt.Errorf("run was aborted")
			}
			if phase == common.ActionPhase_ACTION_PHASE_TIMED_OUT {
				return fmt.Errorf("run timed out")
			}
			// Success
			return nil
		}
	}

	return nil
}

// Watch implements Run interface.
// It returns a channel that streams run updates until completion.
// The channel is automatically closed when the run completes or an error occurs.
//
// Example:
//
//	run, err := flyte.Execute(ctx, task, inputs)
//	if err != nil {
//	    return err
//	}
//
//	updateChan, err := run.Watch(ctx)
//	if err != nil {
//	    return err
//	}
//
//	for update := range updateChan {
//	    if update.Error != "" {
//	        log.Printf("Error: %s", update.Error)
//	        break
//	    }
//	    log.Printf("Phase: %s at %v", update.Phase, update.Timestamp)
//	}
func (r *runRef) Watch(ctx context.Context) (<-chan *RunUpdate, error) {
	if r.pb == nil || r.pb.Action == nil || r.pb.Action.Id == nil || r.pb.Action.Id.Run == nil {
		return nil, fmt.Errorf("invalid run: missing identifiers")
	}

	runID := r.pb.Action.Id.Run
	updates := make(chan *RunUpdate, 10)

	// Start streaming in a goroutine
	go func() {
		defer close(updates)

		stream, err := r.client.runService.WatchRunDetails(ctx, &workflow.WatchRunDetailsRequest{
			RunId: runID,
		})
		if err != nil {
			updates <- &RunUpdate{Error: fmt.Sprintf("failed to watch run: %v", err)}
			return
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				updates <- &RunUpdate{Error: fmt.Sprintf("error watching run: %v", err)}
				return
			}

			if resp.Details == nil || resp.Details.Action == nil || resp.Details.Action.Status == nil {
				continue
			}

			phase := resp.Details.Action.Status.Phase
			r.updatePhase(phase.String())

			// Send update
			update := &RunUpdate{
				Phase: phase.String(),
			}
			// Set timestamp if available
			if resp.Details.Action.Status.StartTime != nil {
				update.Timestamp = resp.Details.Action.Status.StartTime.AsTime()
			} else {
				update.Timestamp = time.Now()
			}

			// Check for errors
			if phase == common.ActionPhase_ACTION_PHASE_FAILED {
				if errorInfo := resp.Details.Action.GetErrorInfo(); errorInfo != nil {
					update.Error = errorInfo.Message
				}
			}

			select {
			case updates <- update:
			case <-ctx.Done():
				return
			}

			// Exit if terminal
			if isTerminalPhase(phase) {
				break
			}
		}
	}()

	return updates, nil
}

// GetOutputs implements Run interface.
// If the run is not complete, it waits for completion first.
//
// Returns:
//   - The run's output values as a map
//   - error if the run fails or outputs cannot be retrieved
//
// Example:
//
//	run, err := flyte.Execute(ctx, task, inputs)
//	if err != nil {
//	    return err
//	}
//
//	outputs, err := run.GetOutputs(ctx)
//	if err != nil {
//	    return err
//	}
//
//	resultPath := outputs["result_path"].(string)
//	log.Printf("Results written to: %s", resultPath)
func (r *runRef) GetOutputs(ctx context.Context) (map[string]interface{}, error) {
	if r.pb == nil || r.pb.Action == nil || r.pb.Action.Id == nil {
		return nil, fmt.Errorf("invalid run: missing identifiers")
	}

	// Ensure run is complete
	r.mu.RLock()
	currentPhase := r.phase
	r.mu.RUnlock()

	// Parse current phase
	var phase common.ActionPhase
	switch currentPhase {
	case common.ActionPhase_ACTION_PHASE_SUCCEEDED.String():
		phase = common.ActionPhase_ACTION_PHASE_SUCCEEDED
	case common.ActionPhase_ACTION_PHASE_FAILED.String():
		phase = common.ActionPhase_ACTION_PHASE_FAILED
	case common.ActionPhase_ACTION_PHASE_ABORTED.String():
		phase = common.ActionPhase_ACTION_PHASE_ABORTED
	case common.ActionPhase_ACTION_PHASE_TIMED_OUT.String():
		phase = common.ActionPhase_ACTION_PHASE_TIMED_OUT
	default:
		// Not terminal yet, wait for completion
		if err := r.Wait(ctx); err != nil {
			return nil, fmt.Errorf("run did not complete successfully: %w", err)
		}
	}

	if !isTerminalPhase(phase) {
		// Should not reach here, but just in case
		if err := r.Wait(ctx); err != nil {
			return nil, fmt.Errorf("run did not complete successfully: %w", err)
		}
	}

	// Fetch outputs using GetActionData
	resp, err := r.client.runService.GetActionData(ctx, &workflow.GetActionDataRequest{
		ActionId: r.pb.Action.Id,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get action data: %w", err)
	}

	// Convert outputs
	outputs, err := convertOutputsToGo(resp.Outputs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert outputs: %w", err)
	}

	return outputs, nil
}

// Execute creates and starts a run for the given task.
// This is the primary API for executing tasks on a Flyte cluster.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - task: The task to execute (typically remote.TaskRef)
//   - inputs: Task input values as a map
//
// Returns:
//   - Run interface for monitoring progress and retrieving results
//   - error if the run cannot be created
//
// Example:
//
//	import "github.com/flyteorg/flyte-sdk-go-min/flyte"
//	import "github.com/flyteorg/flyte-sdk-go-min/flyte/remote"
//
//	// Initialize client
//	flyte.Initialize(ctx, config)
//
//	// Create task reference
//	task := &remote.TaskRef{
//	    Name:    "data_processing",
//	    Version: "v1.0.0",
//	    Project: "analytics",
//	    Domain:  "production",
//	}
//
//	// Execute task
//	run, err := flyte.Execute(ctx, task, map[string]interface{}{
//	    "input_path": "/data/input.csv",
//	    "threshold":  0.95,
//	})
//	if err != nil {
//	    return err
//	}
//
//	// Wait for completion
//	if err := run.Wait(ctx); err != nil {
//	    return fmt.Errorf("run failed: %w", err)
//	}
//
//	// Get outputs
//	outputs, err := run.GetOutputs(ctx)
//	fmt.Printf("Output path: %s\n", outputs["output_path"])
func Execute(ctx context.Context, task Task, inputs map[string]interface{}) (Run, error) {
	client, err := getClient()
	if err != nil {
		return nil, err
	}

	return client.createRun(ctx, task, inputs, nil)
}

// ExecuteWithContext creates and starts a run with custom configuration.
// This allows overriding project/domain, setting labels, environment variables, etc.
//
// Parameters:
//   - runCtx: RunContext with custom configuration
//   - task: The task to execute
//   - inputs: Task input values
//
// Returns:
//   - Run interface for monitoring and results
//   - error if the run cannot be created
//
// Example:
//
//	// Create custom run context
//	runCtx := flyte.NewRunContext(ctx).
//	    WithProject("my-project").
//	    WithDomain("staging").
//	    WithRunName("daily-run-001").
//	    WithLabel("team", "data-eng").
//	    WithLabel("priority", "high").
//	    WithAnnotation("owner", "alice@example.com").
//	    WithEnvVar("LOG_LEVEL", "DEBUG").
//	    WithOverwriteCache(true)
//
//	// Execute with context
//	run, err := flyte.ExecuteWithContext(runCtx, task, inputs)
//	if err != nil {
//	    return err
//	}
//
//	fmt.Printf("Run URL: %s\n", run.GetURL())
//	fmt.Printf("Run name: %s\n", run.GetName())
func ExecuteWithContext(runCtx RunContextBuilder, task Task, inputs map[string]interface{}) (Run, error) {
	client, err := getClient()
	if err != nil {
		return nil, err
	}

	return client.createRun(runCtx.GetContext(), task, inputs, runCtx)
}

// createRun is the internal implementation for creating runs
func (c *client) createRun(ctx context.Context, task Task, inputs map[string]interface{}, runCtx RunContextBuilder) (Run, error) {
	// Convert inputs to protobuf
	pbInputs, err := convertInputsToLiterals(inputs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert inputs: %w", err)
	}

	// Determine project and domain
	project := task.GetProject()
	domain := task.GetDomain()

	// Build RunSpec
	runSpec := &taskpb.RunSpec{}

	if runCtx != nil {
		// Override project/domain if specified
		if runCtx.GetProject() != "" {
			project = runCtx.GetProject()
		}
		if runCtx.GetDomain() != "" {
			domain = runCtx.GetDomain()
		}

		// Add labels
		if len(runCtx.GetLabels()) > 0 {
			runSpec.Labels = &taskpb.Labels{
				Values: runCtx.GetLabels(),
			}
		}

		// Add annotations
		if len(runCtx.GetAnnotations()) > 0 {
			runSpec.Annotations = &taskpb.Annotations{
				Values: runCtx.GetAnnotations(),
			}
		}

		// Add environment variables
		if len(runCtx.GetEnvVars()) > 0 {
			envs := make([]*core.KeyValuePair, 0, len(runCtx.GetEnvVars()))
			for k, v := range runCtx.GetEnvVars() {
				envs = append(envs, &core.KeyValuePair{
					Key:   k,
					Value: v,
				})
			}
			runSpec.Envs = &taskpb.Envs{
				Values: envs,
			}
		}

		// Set cache overwrite
		if runCtx.GetOverwriteCache() {
			runSpec.OverwriteCache = true
		}
	}

	// Build CreateRunRequest
	req := &workflow.CreateRunRequest{
		Task: &workflow.CreateRunRequest_TaskId{
			TaskId: &taskpb.TaskIdentifier{
				Org:     c.config.Org,
				Project: task.GetProject(),
				Domain:  task.GetDomain(),
				Name:    task.GetName(),
				Version: task.GetVersion(),
			},
		},
		Inputs:  pbInputs,
		RunSpec: runSpec,
		Source:  workflow.RunSource_RUN_SOURCE_CLI,
	}

	// Set run identifier
	if runCtx != nil && runCtx.GetRunName() != "" {
		// Explicit run name
		req.Id = &workflow.CreateRunRequest_RunId{
			RunId: &common.RunIdentifier{
				Org:     c.config.Org,
				Project: project,
				Domain:  domain,
				Name:    runCtx.GetRunName(),
			},
		}
	} else {
		// Auto-generate run name
		req.Id = &workflow.CreateRunRequest_ProjectId{
			ProjectId: &common.ProjectIdentifier{
				Organization: c.config.Org,
				Domain:       domain,
				Name:         project,
			},
		}
	}

	// Call CreateRun
	resp, err := c.runService.CreateRun(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create run: %w", err)
	}

	if resp.Run == nil || resp.Run.Action == nil || resp.Run.Action.Id == nil || resp.Run.Action.Id.Run == nil {
		return nil, fmt.Errorf("invalid response from CreateRun: missing run information")
	}

	// Build runRef object
	run := &runRef{
		name:   resp.Run.Action.Id.Run.Name,
		url:    buildConsoleURL(c.config, resp.Run),
		pb:     resp.Run,
		client: c,
		phase:  resp.Run.Action.Status.Phase.String(),
	}

	return run, nil
}

// Compile-time check that runRef implements Run
var _ Run = (*runRef)(nil)
