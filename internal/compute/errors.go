package compute

import (
	"errors"
	"fmt"
)

// Code classifies domain errors. grpcapi maps each Code to exactly one gRPC
// status code.
type Code uint8

const (
	CodeInvalid Code = iota + 1
	CodeNotFound
	CodeAlreadyExists
	// CodeRequestMismatch reports a request id that was already used for a
	// different intent.
	CodeRequestMismatch
	// CodeFailedPrecondition reports an intent that is not valid for the
	// resource's current desired state, such as starting a terminated
	// instance.
	CodeFailedPrecondition
	// CodeAborted reports that planning kept losing optimistic-concurrency
	// races. Retrying with the same request id is safe.
	CodeAborted
	CodeInternal
	// CodeLimitExceeded reports a write the store cannot hold, such as a
	// resource record over its size limit. Retrying it fails the same way.
	CodeLimitExceeded
)

var codeNames = [...]string{
	CodeInvalid:            "Invalid",
	CodeNotFound:           "NotFound",
	CodeAlreadyExists:      "AlreadyExists",
	CodeRequestMismatch:    "RequestMismatch",
	CodeFailedPrecondition: "FailedPrecondition",
	CodeAborted:            "Aborted",
	CodeInternal:           "Internal",
	CodeLimitExceeded:      "LimitExceeded",
}

func (c Code) String() string {
	if int(c) < len(codeNames) && codeNames[c] != "" {
		return codeNames[c]
	}
	return fmt.Sprintf("Code(%d)", c)
}

// Error is the error type package compute returns. Its Code survives
// wrapping, so callers classify with CodeOf.
type Error struct {
	Code Code
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

// Errorf builds an *Error.
func Errorf(c Code, format string, args ...any) error {
	return &Error{Code: c, Msg: fmt.Sprintf(format, args...)}
}

// CodeOf returns the Code of the first *Error in err's chain, or
// CodeInternal if there is none.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeInternal
}
