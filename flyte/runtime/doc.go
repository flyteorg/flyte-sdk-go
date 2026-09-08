// Package runtime is the worker-side half of the Flyte Go SDK: author tasks as
// plain Go functions and run them inside containers launched by a Flyte v2
// control plane.
//
// It is the Go counterpart of the Rust worker SDK (flyte-sdk-rust): the binary
// built from your task code is the container entrypoint. Registration and
// deployment stay in Python via the companion flyteplugins-go package, which
// builds the image, discovers the interface with `<binary> describe-interface`,
// and deploys through the Python SDK.
//
// A task is a function whose first parameter is a Context and whose last
// return value is an error:
//
//	var env = &flyteruntime.TaskEnvironment{Name: "hello_env"}
//
//	func addNumbers(ctx flyteruntime.Context, a, b int64) (int64, error) {
//		return a + b, nil
//	}
//
//	func init() {
//		flyteruntime.RegisterTask("add", addNumbers, env, flyteruntime.WithInputNames("a", "b"))
//	}
//
//	func main() { flyteruntime.Main() }
//
// Import the package under the alias flyteruntime to avoid clashing with the
// standard library's runtime package.
//
// Reusable ("actor") containers are provided by the separate module
// github.com/unionai/union-reuse-go — swap Main() for reuse.Main() and the
// same binary can serve a warm-container pool.
package runtime
