package grpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
)

const (
	// RequestIDWindow is how long LookupOperation remembers a request_id,
	// counted from the operation's create time. It is the 24 hours of the
	// idempotency rules on vws.common.v1.OperationMetadata.
	RequestIDWindow = 24 * time.Hour

	// lookupTimeout bounds the store reads of one LookupOperation when the
	// caller set no shorter deadline.
	lookupTimeout = 10 * time.Second
)

// LookupOperation returns the operation a request_id produced, in the form
// GetOperation returns it, as vws.compute.v1.OperationLookupService
// specifies. It is read-only.
//
// The lookup is scoped to parent. An operation is in a project only when its
// target resource proves it: a controller-driven instance's project, an
// image's scope, or a key pair's resource name. A request id of another
// project, of a resource that names no project, or one created
// RequestIDWindow or longer ago is NOT_FOUND with the same message as an
// unknown request id, so a caller cannot learn whether another project used
// it.
func (s *server) LookupOperation(ctx context.Context, r *computev1.LookupOperationRequest,
) (*computev1.LookupOperationResponse, error) {
	project, err := parseProjectParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	raw := r.GetRequestId()
	if raw == "" {
		return nil, invalidSpec("request_id is required")
	}
	if !uuid4Pattern.MatchString(raw) {
		return nil, invalidSpec("request_id must be a UUID")
	}
	req, err := compute.ParseRequestID(raw)
	if err != nil {
		return nil, toStatus(err)
	}
	ctx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()

	missing := notFound("no operation for request_id " + raw + " in " + r.GetParent())
	op, err := s.read.OperationByRequest(ctx, req)
	if err != nil {
		if compute.CodeOf(err) == compute.CodeNotFound {
			lookupOutcome(ctx, "unknown")
			return nil, missing
		}
		return nil, toStatus(err)
	}
	if !s.now().Before(op.CreatedAt.Add(RequestIDWindow)) {
		lookupOutcome(ctx, "expired")
		return nil, missing
	}
	owner, err := s.operationProject(ctx, op)
	if err != nil {
		return nil, err
	}
	if owner == "" || owner != project {
		// Logged without either project, so no account id reaches the log.
		slog.DebugContext(ctx, "lookup operation denied: request id belongs to another project",
			slog.String("operation", op.ID.String()))
		lookupOutcome(ctx, "foreign")
		return nil, missing
	}
	out, err := s.operationPB(ctx, project, "", op)
	if err != nil {
		return nil, err
	}
	lookupOutcome(ctx, "found")
	return &computev1.LookupOperationResponse{Operation: out}, nil
}

// lookupOutcome records why a lookup answered as it did on the RPC span
// that the grpcobs interceptor started.
func lookupOutcome(ctx context.Context, outcome string) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("vws.lookup.outcome", outcome))
}

// operationProject is the project op's target resource belongs to, or ""
// when the resource names none or no longer exists.
func (s *server) operationProject(ctx context.Context, op compute.Operation) (string, error) {
	var (
		project string
		err     error
	)
	switch op.Target.Kind {
	case compute.KindInstance:
		var id compute.InstanceID
		if id, err = compute.ParseInstanceID(op.Target.ID); err != nil {
			return "", nil //nolint:nilerr // an unparsable stored id proves no project: NOT_FOUND
		}
		var inst compute.Instance
		if inst, err = s.read.Instance(ctx, id); err == nil && inst.Spec.ControllerDriven() {
			project = inst.Spec.Project
		}
	case compute.KindImage:
		var id compute.ImageID
		if id, err = compute.ParseImageID(op.Target.ID); err != nil {
			return "", nil //nolint:nilerr // see above
		}
		var img compute.Image
		if img, err = s.read.Image(ctx, id); err == nil {
			project = img.Spec.Scope.Project()
		}
	case compute.KindKeyPair:
		var id compute.KeyPairID
		if id, err = compute.ParseKeyPairID(op.Target.ID); err != nil {
			return "", nil //nolint:nilerr // see above
		}
		var kp compute.KeyPair
		if kp, err = s.read.KeyPair(ctx, id); err == nil {
			project, _, _, err = parseKeyPairName(kp.Spec.Name.String())
			if err != nil {
				return "", nil //nolint:nilerr // a stored name outside projects/* proves no project
			}
		}
	default:
		return "", nil
	}
	switch {
	case err == nil:
		return project, nil
	case compute.CodeOf(err) == compute.CodeNotFound:
		return "", nil
	default:
		return "", toStatus(err)
	}
}

// parseProjectParent splits projects/{project}. A malformed parent is
// INVALID_ARGUMENT / INVALID_SPEC, as LookupOperationRequest specifies.
func parseProjectParent(parent string) (string, error) {
	project, ok := strings.CutPrefix(parent, "projects/")
	if !ok || !resourceIDPattern.MatchString(project) {
		return "", invalidSpec(fmt.Sprintf("parent %q is not projects/*", parent))
	}
	return project, nil
}
