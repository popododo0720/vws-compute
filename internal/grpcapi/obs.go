package grpcapi

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/popododo0720/vws-lib/errs"
	"github.com/popododo0720/vws-lib/obs"
	"github.com/popododo0720/vws-lib/obs/grpcobs"
)

// ServiceName is the service label in logs, traces, and metrics.
const ServiceName = "compute"

// audited are the RPC name prefixes that change state. A report is not here:
// it carries observed state from a controller and would drown the audit log.
var audited = []string{"Create", "Update", "Delete", "Stop", "Start", "Attach", "Detach", "Import"}

// ServerOptions is the one place the server interceptors are chosen: vws-lib
// grpcobs (tracing, vws.rpc.* metrics, request ids, panic recovery, one error
// report, and a handler's gRPC status pass-through), then the audit log.
func ServerOptions() []grpc.ServerOption {
	return append(grpcobs.ServerOptions(ServiceName), grpc.ChainUnaryInterceptor(auditUnary))
}

func methodName(full string) string { return full[strings.LastIndex(full, "/")+1:] }

// auditUnary writes one audit log line and one vws.audit.events point for each
// mutating RPC. It never reads the request body, so no token or account id can
// reach the log. A panic is audited as internal and then re-raised for grpcobs
// to recover.
func auditUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	next grpc.UnaryHandler,
) (resp any, err error) {
	method := methodName(info.FullMethod)
	if !isAudited(method) {
		return next(ctx, req)
	}
	panicked := true
	defer func() {
		outcome := "ok"
		var errCode string
		switch {
		case panicked:
			outcome, errCode = strings.ToLower(codes.Internal.String()), errs.CodeInternal.String()
		case err != nil:
			outcome, errCode = strings.ToLower(status.Code(err).String()), errs.CodeOf(err).String()
		}
		attrs := []slog.Attr{
			slog.Bool(obs.FieldAudit, true),
			slog.String(obs.FieldOp, method),
			slog.String("outcome", outcome),
		}
		if id := resourceID(req); id != "" {
			attrs = append(attrs, slog.String(obs.FieldResourceID, id))
		}
		if errCode != "" {
			attrs = append(attrs, slog.String(obs.FieldErrCode, errCode))
		}
		slog.Default().LogAttrs(ctx, slog.LevelInfo, "audit", attrs...)
		obs.M().Audit(ctx, ServiceName, method, outcome)
	}()
	resp, err = next(ctx, req)
	panicked = false
	return resp, err
}

// resourceID names the request's resource for the audit line, without the
// projects/{project} segment, so an account id never reaches the log. It
// keeps the collection and id, as in "instances/i-...". Requests that
// create a resource have no name yet, so that field stays empty.
func resourceID(req any) string {
	named, ok := req.(interface{ GetName() string })
	if !ok {
		return ""
	}
	name := strings.TrimSuffix(named.GetName(), "/")
	if i := strings.LastIndex(name, "/"); i > 0 {
		if j := strings.LastIndex(name[:i], "/"); j >= 0 {
			return name[j+1:]
		}
	}
	return name
}

func isAudited(method string) bool {
	for _, prefix := range audited {
		if strings.HasPrefix(method, prefix) {
			return true
		}
	}
	return false
}
