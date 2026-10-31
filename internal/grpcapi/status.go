package grpcapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
)

const errorDomain = "vws.dev"

func withReason(code codes.Code, reason, msg string) error {
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: errorDomain})
	if err != nil {
		return status.Error(code, msg)
	}
	return st.Err()
}

func invalidSpec(msg string) error {
	return withReason(codes.InvalidArgument, "INVALID_SPEC", msg)
}

func invalidID(msg string) error {
	return withReason(codes.InvalidArgument, "INVALID_RESOURCE_ID", msg)
}

func notFound(msg string) error {
	return withReason(codes.NotFound, "RESOURCE_NOT_FOUND", msg)
}

func alreadyExists(msg string) error {
	return withReason(codes.AlreadyExists, "ALREADY_EXISTS", msg)
}

func aborted(reason, msg string) error {
	return withReason(codes.Aborted, reason, msg)
}

func failedPre(reason, msg string) error {
	return withReason(codes.FailedPrecondition, reason, msg)
}

func dependencyMissing(msg string) error {
	return failedPre("DEPENDENCY_MISSING", msg)
}

func statusNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

// toStatus maps a domain error onto the gRPC status and ErrorInfo.reason
// from vws.common.v1.ErrorReason. A status error is returned unchanged.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch compute.CodeOf(err) {
	case compute.CodeInvalid:
		return invalidSpec(err.Error())
	case compute.CodeNotFound:
		return notFound(err.Error())
	case compute.CodeAlreadyExists:
		return alreadyExists(err.Error())
	case compute.CodeRequestMismatch:
		return aborted("IDEMPOTENCY_CONFLICT", err.Error())
	case compute.CodeFailedPrecondition:
		return failedPre("DEPENDENCY_NOT_READY", err.Error())
	case compute.CodeAborted:
		return status.Error(codes.Aborted, err.Error())
	case compute.CodeLimitExceeded:
		return withReason(codes.ResourceExhausted, "QUOTA_EXCEEDED", err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func grpcCode(c compute.Code) codes.Code {
	switch c {
	case compute.CodeInvalid:
		return codes.InvalidArgument
	case compute.CodeNotFound:
		return codes.NotFound
	case compute.CodeAlreadyExists:
		return codes.AlreadyExists
	case compute.CodeRequestMismatch, compute.CodeAborted:
		return codes.Aborted
	case compute.CodeFailedPrecondition:
		return codes.FailedPrecondition
	case compute.CodeLimitExceeded:
		return codes.ResourceExhausted
	default:
		return codes.Internal
	}
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("random request id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// randomResourceID is a ResourceId-shaped token for an operation this process
// does not store, such as validate_only.
func randomResourceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("random request id: %w", err)
	}
	return "n-" + hex.EncodeToString(b[:]), nil
}

// submit admits in. fresh runs only when requestID has no stored operation,
// which is where an etag is checked: a retry of a request that already
// moved the resource returns the original operation instead of ETAG_MISMATCH.
// fresh runs only for a request id the store has not seen. A non-nil intent
// it returns replaces in: CreateInstance and UpdateInstance fill in volume
// claims from the storage service there. The replacement must have in's
// fingerprint.
func (s *server) submit(ctx context.Context, requestID string, in compute.Intent, inErr error, fresh func() (compute.Intent, error)) (controller.Receipt, error) {
	if err := checkRequestID(requestID); err != nil {
		return controller.Receipt{}, err
	}
	if requestID == "" {
		id, err := newUUID()
		if err != nil {
			return controller.Receipt{}, status.Error(codes.Internal, err.Error())
		}
		requestID = id
	}
	req, err := compute.ParseRequestID(requestID)
	if err != nil {
		return controller.Receipt{}, toStatus(err)
	}
	if inErr != nil {
		return controller.Receipt{}, toStatus(inErr)
	}
	if fresh != nil {
		_, err := s.read.OperationByRequest(ctx, req)
		switch {
		case compute.CodeOf(err) == compute.CodeNotFound:
			next, err := fresh()
			if err != nil {
				return controller.Receipt{}, err
			}
			if next != nil {
				in = next
			}
		case err != nil:
			return controller.Receipt{}, toStatus(err)
		}
	}
	rc, err := s.ctl.Submit(ctx, req, in)
	if err != nil {
		return controller.Receipt{}, toStatus(err)
	}
	return rc, nil
}

// storedOperation returns the operation for requestID, or the zero operation
// when the id is empty or unused.
func (s *server) storedOperation(ctx context.Context, requestID string) (compute.Operation, error) {
	if requestID == "" {
		return compute.Operation{}, nil
	}
	req, err := compute.ParseRequestID(requestID)
	if err != nil {
		return compute.Operation{}, toStatus(err)
	}
	op, err := s.read.OperationByRequest(ctx, req)
	if err == nil {
		return op, nil
	}
	if compute.CodeOf(err) == compute.CodeNotFound {
		return compute.Operation{}, nil
	}
	return compute.Operation{}, toStatus(err)
}

// check adapts a fresh-request check that does not replace the intent.
func check(f func() error) func() (compute.Intent, error) {
	return func() (compute.Intent, error) { return nil, f() }
}
