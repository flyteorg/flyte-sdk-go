package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterTaskDerivesInterface(t *testing.T) {
	env := &TaskEnvironment{Name: "test_env"}
	task := RegisterTask("iface_add", func(ctx Context, a int64, label string) (int64, error) {
		return a, nil
	}, env, WithInputNames("a", "label"))

	assert.Equal(t,
		`{"flyte_interface_version":1,"task":"iface_add","inputs":[`+
			`{"name":"a","type":"integer","required":true},`+
			`{"name":"label","type":"string","required":true}],`+
			`"outputs":[{"name":"o0","type":"integer"}]}`,
		task.Interface().DescriptorJSON("iface_add"))
	assert.Equal(t, env, task.Environment())
}

func TestRegisterTaskDefaultsPositionalNames(t *testing.T) {
	task := RegisterTask("iface_pos", func(ctx Context, x, y float64) (float64, error) {
		return x + y, nil
	}, nil)
	json := task.Interface().DescriptorJSON("iface_pos")
	assert.Contains(t, json, `{"name":"a0","type":"float","required":true}`)
	assert.Contains(t, json, `{"name":"a1","type":"float","required":true}`)
}

func TestRegisterTaskRejectsBadSignatures(t *testing.T) {
	cases := map[string]any{
		"not a function":     42,
		"no context param":   func(a int64) (int64, error) { return a, nil },
		"no error return":    func(ctx Context, a int64) int64 { return a },
		"too many returns":   func(ctx Context) (int64, int64, error) { return 0, 0, nil },
		"unsupported input":  func(ctx Context, a []int) error { return nil },
		"unsupported output": func(ctx Context) (chan int, error) { return nil, nil },
		"variadic":           func(ctx Context, a ...int64) error { return nil },
		// Implements context.Context, but run passes a plain context.Context
		// value: reflect.Call would panic at invocation time.
		"concrete context param": func(ctx concreteCtx) error { return nil },
	}
	i := 0
	for name, fn := range cases {
		i++
		taskName := fmt.Sprintf("bad_sig_%d", i)
		assert.Panics(t, func() { RegisterTask(taskName, fn, nil) }, name)
	}

	assert.Panics(t, func() {
		RegisterTask("bad_names", func(ctx Context, a int64) error { return nil }, nil,
			WithInputNames("a", "extra"))
	}, "name count mismatch")
	assert.Panics(t, func() {
		RegisterTask("bad name!", func(ctx Context) error { return nil }, nil)
	}, "invalid task name")
}

// concreteCtx is a concrete type implementing context.Context.
type concreteCtx struct{ context.Context }

func TestRegisterTaskAcceptsContextInterfaces(t *testing.T) {
	// Any interface a context.Context satisfies is fine as the first parameter.
	task := RegisterTask("ctx_iface", func(ctx interface{ Done() <-chan struct{} }) error { return nil }, nil)
	_, err := task.run(context.Background(), nil)
	require.NoError(t, err)
}

func TestTaskRunNamedPrimitiveTypes(t *testing.T) {
	task := RegisterTask("run_named", func(ctx Context, l namedLabel, f namedFlag) (namedLabel, error) {
		if f {
			return l + "!", nil
		}
		return l, nil
	}, nil, WithInputNames("l", "f"))

	inputs, err := buildInputs([]string{"l", "f"}, reflectValues(namedLabel("hi"), namedFlag(true)))
	require.NoError(t, err)
	outputs, err := task.run(context.Background(), inputs)
	require.NoError(t, err, "named string/bool parameters must not panic at call time")
	assert.Equal(t, "hi!", outputs.GetLiterals()[0].GetValue().GetScalar().GetPrimitive().GetStringValue())
}

func TestRegisterTaskRejectsDuplicates(t *testing.T) {
	fn := func(ctx Context) error { return nil }
	RegisterTask("dup_task", fn, nil)
	assert.Panics(t, func() { RegisterTask("dup_task", fn, nil) })
}

func TestTaskRunRoundtrips(t *testing.T) {
	task := RegisterTask("run_add", func(ctx Context, a, b int64) (int64, error) {
		return a + b, nil
	}, nil, WithInputNames("a", "b"))

	inputs, err := buildInputs([]string{"a", "b"}, reflectValues(int64(2), int64(3)))
	require.NoError(t, err)
	outputs, err := task.run(context.Background(), inputs)
	require.NoError(t, err)
	require.Len(t, outputs.GetLiterals(), 1)
	assert.Equal(t, "o0", outputs.GetLiterals()[0].GetName())
	assert.Equal(t, int64(5), outputs.GetLiterals()[0].GetValue().GetScalar().GetPrimitive().GetInteger())
}

func TestTaskRunClassifiesFailures(t *testing.T) {
	boom := errors.New("boom")
	failing := RegisterTask("run_fail", func(ctx Context) error { return boom }, nil)
	_, err := failing.run(context.Background(), nil)
	require.ErrorIs(t, err, boom)
	assert.Equal(t, OriginUser, OriginOf(err))

	panicky := RegisterTask("run_panic", func(ctx Context) error { panic("kaboom") }, nil)
	_, err = panicky.run(context.Background(), nil)
	require.Error(t, err)
	assert.Equal(t, OriginUser, OriginOf(err))
	assert.Contains(t, err.Error(), "kaboom")

	// A missing input is the runtime's fault (bad envelope), not the user's.
	needsInput := RegisterTask("run_missing_input", func(ctx Context, a int64) error { return nil }, nil)
	_, err = needsInput.run(context.Background(), nil)
	require.Error(t, err)
	assert.Equal(t, OriginSystem, OriginOf(err))
}

func TestOverrideLeavesRegisteredTaskUntouched(t *testing.T) {
	env := &TaskEnvironment{Name: "orig"}
	task := RegisterTask("override_me", func(ctx Context) error { return nil }, env)
	changed := task.Override(WithEnvironment(&TaskEnvironment{Name: "new"}))
	assert.Equal(t, "new", changed.Environment().Name)
	registered, _ := GetTask("override_me")
	assert.Equal(t, "orig", registered.Environment().Name)
}

func TestTraceLocalModeRunsBody(t *testing.T) {
	double := func(ctx Context, x int64) (int64, error) { return x * 2, nil }
	// No runtime state in ctx → the body just runs, no recording.
	v, err := Trace[int64](context.Background(), double, int64(21)).Get()
	require.NoError(t, err)
	assert.Equal(t, int64(42), v)

	fails := func(ctx Context) error { return errors.New("nope") }
	_, err = Trace[struct{}](context.Background(), fails).Get()
	assert.Error(t, err)

	// Arg mismatch is reported, not panicked.
	_, err = Trace[int64](context.Background(), double, "wrong").Get()
	assert.Error(t, err)
	// Output type mismatch is reported.
	_, err = Trace[string](context.Background(), double, int64(1)).Get()
	assert.Error(t, err)
}

func reflectValues(vals ...any) (out []reflect.Value) {
	for _, v := range vals {
		out = append(out, reflect.ValueOf(v))
	}
	return out
}
