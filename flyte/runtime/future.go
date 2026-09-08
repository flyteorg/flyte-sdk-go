package runtime

// Future is a typed handle to an asynchronously running traced call, in the
// spirit of the prototype Go SDK's futures: generics only at the call site.
type Future[T any] struct {
	done chan struct{}
	val  T
	err  error
}

func newFuture[T any]() *Future[T] {
	return &Future[T]{done: make(chan struct{})}
}

func (f *Future[T]) complete(val T, err error) {
	f.val, f.err = val, err
	close(f.done)
}

// Get blocks until the call finishes and returns its result.
func (f *Future[T]) Get() (T, error) {
	<-f.done
	return f.val, f.err
}

// IsReady reports whether the result is available without blocking.
func (f *Future[T]) IsReady() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}
