// Package grpcapi serves the compute services defined by
// github.com/popododo0720/vws-proto.
//
// It is the only package that sees wire types. Requests are parsed into
// compute values, mutations go through controller.Submit, and reads go to a
// store.Reader. No wire type crosses Register.
//
// This process keeps one repository. Key pairs are stored under their full
// resource name. Instances and images are the repository rows: every parent
// lists the same rows, and the returned name uses the parent the caller
// asked for. Volumes, snapshots, subnets, and security groups are not hosted
// here, so a spec that requires one is rejected with DEPENDENCY_MISSING.
package grpcapi

import (
	"context"
	"strings"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/store"
)

const (
	// DefaultWait is the WaitOperation timeout used when the request omits one.
	DefaultWait = 30 * time.Second
	// MaxWait is the longest WaitOperation blocks. Shutdown cancels sooner.
	MaxWait   = 60 * time.Second
	pollEvery = 20 * time.Millisecond

	defaultPageSize = 50
	maxPageSize     = 1000
)

// Register adds InstanceService, ImageService, KeyPairService, and
// google.longrunning.Operations to s. ctl is the only write path and r the
// only read path.
func Register(s grpc.ServiceRegistrar, ctl *controller.Controller, r store.Reader) {
	srv := &server{ctl: ctl, read: r}
	computev1.RegisterInstanceServiceServer(s, srv)
	computev1.RegisterImageServiceServer(s, srv)
	computev1.RegisterKeyPairServiceServer(s, srv)
	longrunningpb.RegisterOperationsServer(s, srv)
}

// server embeds the Unsafe markers so a method added to a compute service
// fails to compile until it is served. Operations embeds the stock
// unimplemented server: List, Delete, and Cancel are not part of this
// repository and return UNIMPLEMENTED, which that service allows.
type server struct {
	computev1.UnsafeInstanceServiceServer
	computev1.UnsafeImageServiceServer
	computev1.UnsafeKeyPairServiceServer
	longrunningpb.UnimplementedOperationsServer
	ctl  *controller.Controller
	read store.Reader
}

func (s *server) GetOperation(ctx context.Context, r *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	return s.wait(ctx, r.GetName(), 0)
}

func (s *server) WaitOperation(ctx context.Context, r *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	timeout := DefaultWait
	if d := r.GetTimeout(); d != nil {
		if err := d.CheckValid(); err != nil || d.AsDuration() < 0 {
			return nil, invalidSpec("timeout must be a valid non-negative duration")
		}
		timeout = min(d.AsDuration(), MaxWait)
	}
	return s.wait(ctx, r.GetName(), timeout)
}

func (s *server) wait(ctx context.Context, name string, timeout time.Duration) (*longrunningpb.Operation, error) {
	project, rawID, err := parseOperationName(name)
	if err != nil {
		return nil, err
	}
	id, err := compute.ParseOperationID(rawID)
	if err != nil {
		return nil, notFound("operation " + name + " not found")
	}
	end := time.Now().Add(timeout)
	for {
		op, err := s.read.Operation(ctx, id)
		if err != nil {
			return nil, toStatus(err)
		}
		if op.State().Terminal() || !time.Now().Before(end) {
			return s.operationPB(ctx, project, "", op)
		}
		timer := time.NewTimer(pollEvery)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, toStatus(ctx.Err())
		case <-timer.C:
		}
	}
}

func checkMask(paths []string) error {
	for _, p := range paths {
		switch {
		case p == "spec" || strings.HasPrefix(p, "spec."):
		case p == "labels" || strings.HasPrefix(p, "labels."):
		default:
			return invalidSpec("field " + p + " cannot be updated")
		}
	}
	return nil
}

func effectiveMask(paths []string) []string {
	if len(paths) == 0 {
		return []string{"spec", "labels"}
	}
	return paths
}

func maskIncludes(paths []string, field string) bool {
	for _, p := range paths {
		if p == field || strings.HasPrefix(p, field+".") {
			return true
		}
	}
	return false
}

func cutPage[T any](items []T, pageSize int32, token string, name func(T) string) ([]T, string, error) {
	if pageSize < 0 {
		return nil, "", invalidSpec("page_size must not be negative")
	}
	size := int(pageSize)
	if size == 0 {
		size = defaultPageSize
	}
	if size > maxPageSize {
		size = maxPageSize
	}
	var filtered []T
	for _, it := range items {
		if token != "" && name(it) <= token {
			continue
		}
		filtered = append(filtered, it)
	}
	if len(filtered) <= size {
		return filtered, "", nil
	}
	return filtered[:size], name(filtered[size-1]), nil
}

func doneOperation(project, resource string, verb commonv1.OperationVerb, requestID string, body proto.Message) (*longrunningpb.Operation, error) {
	id, err := randomResourceID()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if requestID == "" {
		requestID, err = newUUID()
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	now := timestamppb.Now()
	md := &commonv1.OperationMetadata{
		Resource:   resource,
		Verb:       verb,
		RequestId:  requestID,
		CreateTime: now,
		EndTime:    now,
	}
	metaAny, err := anypb.New(md)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &longrunningpb.Operation{
		Name:     operationResourceName(project, id),
		Metadata: metaAny,
		Done:     true,
	}
	if body != nil {
		packed, err := anypb.New(body)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Result = &longrunningpb.Operation_Response{Response: packed}
	}
	return out, nil
}
