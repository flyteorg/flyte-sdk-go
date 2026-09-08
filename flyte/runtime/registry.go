package runtime

import (
	"fmt"
	"sort"
	"sync"
)

// The task registry: RegisterTask in init() makes the binary self-describing —
// Main() can dispatch any registered task by name and `describe-interface`
// prints every registered descriptor.

var registry = struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}{tasks: map[string]*Task{}}

// RegisterTask registers fn as a task. Call from init() (or before Main).
//
// fn must be func(Context, ...inputs) (T, error) or func(Context, ...inputs)
// error, with inputs and T drawn from the supported types (int, int32, int64,
// float32, float64, string, bool, or any struct — structs travel as msgpack).
// Panics on an invalid signature or duplicate name: registration runs at
// process start, where failing loudly beats a half-registered binary (the
// http.Handle convention).
func RegisterTask(name string, fn any, env *TaskEnvironment, opts ...TaskOption) *Task {
	t, err := newTask(name, fn, env, opts...)
	if err != nil {
		panic(fmt.Sprintf("flyteruntime.RegisterTask: %v", err))
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, dup := registry.tasks[name]; dup {
		panic(fmt.Sprintf("flyteruntime.RegisterTask: task %q registered twice", name))
	}
	registry.tasks[name] = t
	return t
}

// GetTask returns a registered task by name.
func GetTask(name string) (*Task, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	t, ok := registry.tasks[name]
	return t, ok
}

// ListTasks returns the registered task names, sorted.
func ListTasks() []string {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	names := make([]string, 0, len(registry.tasks))
	for name := range registry.tasks {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ResolveTask picks the task to run: the named one, or the sole registered
// task when name is empty. Used by Main and by the reuse module's pool to
// dispatch assignments.
func ResolveTask(name string) (*Task, error) {
	return resolveTask(name)
}

// resolveTask picks the task to run: the named one, or the sole registered
// task when name is empty.
func resolveTask(name string) (*Task, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	names := make([]string, 0, len(registry.tasks))
	for n := range registry.tasks {
		names = append(names, n)
	}
	sort.Strings(names)
	if name != "" {
		t, ok := registry.tasks[name]
		if !ok {
			return nil, SystemErrorf("no task named %q is registered (registered: %v)", name, names)
		}
		return t, nil
	}
	if len(registry.tasks) == 1 {
		for _, t := range registry.tasks {
			return t, nil
		}
	}
	return nil, SystemErrorf("binary registers %d tasks; the launcher must select one with --task <name> (registered: %v)", len(registry.tasks), names)
}
