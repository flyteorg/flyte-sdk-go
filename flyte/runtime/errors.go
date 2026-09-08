package runtime

import (
	"errors"
	"fmt"

	corepb "github.com/flyteorg/flyte/v2/gen/go/flyteidl2/core"
)

// ErrorOrigin says whose fault a task failure is: the user's code or the
// SDK/infrastructure. It selects the ExecutionError kind written to error.pb,
// which drives the backend's retry-budget accounting.
type ErrorOrigin int

const (
	// OriginUser marks a failure of the task's own logic. Errors returned by a
	// task function (and panics inside it) default to this.
	OriginUser ErrorOrigin = iota
	// OriginSystem marks an SDK or infrastructure failure (storage, config,
	// control-plane RPCs). System failures do not spend the user's retry budget
	// the same way.
	OriginSystem
)

// Error is a classified task-runtime error. Task functions may return any
// error; the runtime wraps them as user errors unless they already carry a
// classification (see UserErrorf / SystemErrorf).
type Error struct {
	Code   string
	Origin ErrorOrigin
	err    error
}

func (e *Error) Error() string { return e.err.Error() }
func (e *Error) Unwrap() error { return e.err }

// UserErrorf builds a user-fault error with an explicit code, mirroring the
// Rust SDK's Error::User. The code lands in error.pb's ContainerError.code.
func UserErrorf(code, format string, args ...any) error {
	return &Error{Code: code, Origin: OriginUser, err: fmt.Errorf(format, args...)}
}

// SystemErrorf builds a system-fault error (code "SystemError"), for failures
// of the runtime itself rather than the task's logic.
func SystemErrorf(format string, args ...any) error {
	return &Error{Code: "SystemError", Origin: OriginSystem, err: fmt.Errorf(format, args...)}
}

// classify wraps err as an *Error, defaulting to a user fault with code
// "UserError" — the classification for a plain error returned by a task fn.
func classify(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return &Error{Code: "UserError", Origin: OriginUser, err: err}
}

// errorDocument renders err as the core.ErrorDocument written to error.pb.
// Matches the Rust worker's error_document: kind is always RECOVERABLE (the
// backend decides retryability), origin USER or SYSTEM.
func errorDocument(err error) *corepb.ErrorDocument {
	e := classify(err)
	origin := corepb.ExecutionError_USER
	if e.Origin == OriginSystem {
		origin = corepb.ExecutionError_SYSTEM
	}
	return &corepb.ErrorDocument{
		Error: &corepb.ContainerError{
			Code:    e.Code,
			Message: e.Error(),
			Kind:    corepb.ContainerError_RECOVERABLE,
			Origin:  origin,
		},
	}
}
