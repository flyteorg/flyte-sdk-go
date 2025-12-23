package remote

// TaskRef references a task that has been deployed to a Flyte cluster.
// It implements the flyte.Task interface, allowing it to be used with
// all SDK functions that accept tasks.
//
// TaskRef is used for executing pre-deployed tasks without needing to
// re-deploy or define them locally. This is useful for:
//   - Triggering existing workflows from Go applications
//   - Building task orchestration on top of existing Flyte deployments
//   - Cross-language task execution (e.g., calling Python tasks from Go)
//
// Example usage:
//
//	task := &remote.TaskRef{
//	    Name:    "data_processing",
//	    Version: "v1.0.0",
//	    Project: "analytics",
//	    Domain:  "production",
//	}
//
//	run, err := flyte.Execute(ctx, task, map[string]interface{}{
//	    "input_path": "/data/input.csv",
//	    "threshold":  0.95,
//	})
//
// Future enhancements (Phase 1):
//   - Auto-versioning: task.WithAutoVersion("latest")
//   - Task details: task.GetDetails(ctx)
//   - Resource overrides: task.Override(flyte.WithCPU("4"))
type TaskRef struct {
	// Name is the task name
	Name string

	// Version is the task version (e.g., "v1.0.0", "abc123def")
	Version string

	// Project is the project containing the task
	Project string

	// Domain is the domain (e.g., "development", "staging", "production")
	Domain string
}

// GetName implements flyte.Task interface
func (t *TaskRef) GetName() string {
	return t.Name
}

// GetVersion implements flyte.Task interface
func (t *TaskRef) GetVersion() string {
	return t.Version
}

// GetProject implements flyte.Task interface
func (t *TaskRef) GetProject() string {
	return t.Project
}

// GetDomain implements flyte.Task interface
func (t *TaskRef) GetDomain() string {
	return t.Domain
}

// Compile-time check that TaskRef implements flyte.Task
var _ interface {
	GetName() string
	GetVersion() string
	GetProject() string
	GetDomain() string
} = (*TaskRef)(nil)
