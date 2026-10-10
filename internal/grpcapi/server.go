// Package grpcapi serves vws.compute.v1.ComputeService.
//
// It is the only package that sees wire types. Requests are parsed into
// compute values by the compute constructors, mutations go through
// controller.Submit, reads go to a store.Reader, and every error leaves
// through toStatus. No wire type crosses Register.
package grpcapi

import (
	"context"
	"errors"
	"time"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	pb "github.com/popododo0720/vws-compute/internal/grpcapi/internal/computepb"
	"github.com/popododo0720/vws-compute/internal/store"
)

const (
	DefaultWait = 30 * time.Second
	MaxWait     = 60 * time.Second
	pollEvery   = 20 * time.Millisecond
)

// Register adds ComputeService to s. ctl is the only write path and r the
// only read path.
func Register(s grpc.ServiceRegistrar, ctl *controller.Controller, r store.Reader) {
	pb.RegisterComputeServiceServer(s, &server{ctl: ctl, read: r})
}

// server embeds the Unsafe marker instead of UnimplementedComputeServiceServer
// so a method added to the proto fails to compile until it is served.
type server struct {
	pb.UnsafeComputeServiceServer
	ctl  *controller.Controller
	read store.Reader
}

// toStatus is the single mapping from domain errors to gRPC status.
func toStatus(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(grpcCode(compute.CodeOf(err)), err.Error())
}

func grpcCode(c compute.Code) codes.Code {
	switch c {
	case compute.CodeInvalid, compute.CodeRequestMismatch:
		return codes.InvalidArgument
	case compute.CodeNotFound:
		return codes.NotFound
	case compute.CodeAlreadyExists:
		return codes.AlreadyExists
	case compute.CodeFailedPrecondition:
		return codes.FailedPrecondition
	case compute.CodeAborted:
		return codes.Aborted
	}
	return codes.Internal
}

// submit parses the request id and, if the intent parsed (inErr == nil),
// submits it.
func (s *server) submit(ctx context.Context, requestID string, in compute.Intent, inErr error) (controller.Receipt, error) {
	req, err := compute.ParseRequestID(requestID)
	if err != nil {
		return controller.Receipt{}, toStatus(err)
	}
	if inErr != nil {
		return controller.Receipt{}, toStatus(inErr)
	}
	r, err := s.ctl.Submit(ctx, req, in)
	if err != nil {
		return controller.Receipt{}, toStatus(err)
	}
	return r, nil
}

func (s *server) RunInstance(ctx context.Context, r *pb.RunInstanceRequest) (*pb.RunInstanceResponse, error) {
	in, err := compute.NewRunInstance(r.GetImageId(), r.GetInstanceType(), r.GetKeyName())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.RunInstanceResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) StartInstance(ctx context.Context, r *pb.StartInstanceRequest) (*pb.StartInstanceResponse, error) {
	in, err := compute.NewStartInstance(r.GetInstanceId())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.StartInstanceResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) StopInstance(ctx context.Context, r *pb.StopInstanceRequest) (*pb.StopInstanceResponse, error) {
	in, err := compute.NewStopInstance(r.GetInstanceId())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.StopInstanceResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) RebootInstance(ctx context.Context, r *pb.RebootInstanceRequest) (*pb.RebootInstanceResponse, error) {
	in, err := compute.NewRebootInstance(r.GetInstanceId())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.RebootInstanceResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) TerminateInstance(ctx context.Context, r *pb.TerminateInstanceRequest) (*pb.TerminateInstanceResponse, error) {
	in, err := compute.NewTerminateInstance(r.GetInstanceId())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.TerminateInstanceResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) RegisterImage(ctx context.Context, r *pb.RegisterImageRequest) (*pb.RegisterImageResponse, error) {
	in, err := compute.NewRegisterImage(r.GetName(), r.GetSourceUrl())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.RegisterImageResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) CreateImage(ctx context.Context, r *pb.CreateImageRequest) (*pb.CreateImageResponse, error) {
	in, err := compute.NewCreateImage(r.GetInstanceId(), r.GetName())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.CreateImageResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) DeregisterImage(ctx context.Context, r *pb.DeregisterImageRequest) (*pb.DeregisterImageResponse, error) {
	in, err := compute.NewDeregisterImage(r.GetImageId())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.DeregisterImageResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) CreateKeyPair(ctx context.Context, r *pb.CreateKeyPairRequest) (*pb.CreateKeyPairResponse, error) {
	in, err := compute.NewCreateKeyPair(r.GetKeyName())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	resp := &pb.CreateKeyPairResponse{Operation: operationPB(rc.Operation)}
	if rc.PrivateKey != nil {
		resp.PrivateKeyOpenssh = string(rc.PrivateKey.OpenSSH())
	}
	if rc.KeyPair != nil {
		resp.KeyPair = keyPairPB(*rc.KeyPair)
		return resp, nil
	}
	id, err := compute.ParseKeyPairID(rc.Operation.Target.ID)
	if err != nil {
		return nil, toStatus(err)
	}
	kp, err := s.read.KeyPair(ctx, id)
	if err != nil {
		return nil, toStatus(err)
	}
	resp.KeyPair = keyPairPB(kp)
	return resp, nil
}

func (s *server) ImportKeyPair(ctx context.Context, r *pb.ImportKeyPairRequest) (*pb.ImportKeyPairResponse, error) {
	in, err := compute.NewImportKeyPair(r.GetKeyName(), r.GetPublicKeyOpenssh())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.ImportKeyPairResponse{Operation: operationPB(rc.Operation)}, nil
}

func (s *server) DeleteKeyPair(ctx context.Context, r *pb.DeleteKeyPairRequest) (*pb.DeleteKeyPairResponse, error) {
	in, err := compute.NewDeleteKeyPair(r.GetKeyName())
	rc, err := s.submit(ctx, r.GetRequestId(), in, err)
	if err != nil {
		return nil, err
	}
	return &pb.DeleteKeyPairResponse{Operation: operationPB(rc.Operation)}, nil
}

func pageSize(n int32) (int, error) {
	if n < 0 {
		return 0, compute.Errorf(compute.CodeInvalid, "page_size must not be negative")
	}
	return int(n), nil
}

func parseAll[T any](raw []string, parse func(string) (T, error)) ([]T, error) {
	out := make([]T, 0, len(raw))
	for _, s := range raw {
		v, err := parse(s)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *server) DescribeInstances(ctx context.Context, r *pb.DescribeInstancesRequest) (*pb.DescribeInstancesResponse, error) {
	ids, err := parseAll(r.GetInstanceIds(), compute.ParseInstanceID)
	if err != nil {
		return nil, toStatus(err)
	}
	size, err := pageSize(r.GetPageSize())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.read.Instances(ctx, store.InstanceQuery{IDs: ids, PageSize: size, After: r.GetPageToken()})
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &pb.DescribeInstancesResponse{NextPageToken: page.Next}
	for _, inst := range page.Items {
		resp.Instances = append(resp.Instances, instancePB(inst))
	}
	return resp, nil
}

func (s *server) DescribeImages(ctx context.Context, r *pb.DescribeImagesRequest) (*pb.DescribeImagesResponse, error) {
	ids, err := parseAll(r.GetImageIds(), compute.ParseImageID)
	if err != nil {
		return nil, toStatus(err)
	}
	size, err := pageSize(r.GetPageSize())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.read.Images(ctx, store.ImageQuery{IDs: ids, PageSize: size, After: r.GetPageToken()})
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &pb.DescribeImagesResponse{NextPageToken: page.Next}
	for _, img := range page.Items {
		resp.Images = append(resp.Images, imagePB(img))
	}
	return resp, nil
}

func (s *server) DescribeKeyPairs(ctx context.Context, r *pb.DescribeKeyPairsRequest) (*pb.DescribeKeyPairsResponse, error) {
	names, err := parseAll(r.GetKeyNames(), compute.ParseKeyName)
	if err != nil {
		return nil, toStatus(err)
	}
	size, err := pageSize(r.GetPageSize())
	if err != nil {
		return nil, toStatus(err)
	}
	page, err := s.read.KeyPairs(ctx, store.KeyPairQuery{Names: names, PageSize: size, After: r.GetPageToken()})
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &pb.DescribeKeyPairsResponse{NextPageToken: page.Next}
	for _, kp := range page.Items {
		resp.KeyPairs = append(resp.KeyPairs, keyPairPB(kp))
	}
	return resp, nil
}

func (s *server) GetOperation(ctx context.Context, r *pb.GetOperationRequest) (*pb.Operation, error) {
	return s.wait(ctx, r.GetOperationId(), 0)
}

func (s *server) WaitOperation(ctx context.Context, r *pb.WaitOperationRequest) (*pb.Operation, error) {
	timeout := DefaultWait
	if d := r.GetTimeout(); d != nil {
		if err := d.CheckValid(); err != nil || d.AsDuration() < 0 {
			return nil, toStatus(compute.Errorf(compute.CodeInvalid, "timeout must be a valid non-negative duration"))
		}
		timeout = min(d.AsDuration(), MaxWait)
	}
	return s.wait(ctx, r.GetOperationId(), timeout)
}

// wait polls the operation until it is terminal or timeout elapses, then
// returns its current state. Running out of time is not an error.
func (s *server) wait(ctx context.Context, rawID string, timeout time.Duration) (*pb.Operation, error) {
	id, err := compute.ParseOperationID(rawID)
	if err != nil {
		return nil, toStatus(err)
	}
	end := time.Now().Add(timeout)
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for {
		op, err := s.read.Operation(ctx, id)
		if err != nil {
			return nil, toStatus(err)
		}
		if op.State().Terminal() || !time.Now().Before(end) {
			return operationPB(op), nil
		}
		select {
		case <-ctx.Done():
			return nil, toStatus(ctx.Err())
		case <-tick.C:
		}
	}
}

var (
	instanceStates = map[compute.InstanceLifecycle]pb.InstanceState{
		compute.InstancePending:      pb.InstanceState_INSTANCE_STATE_PENDING,
		compute.InstanceRunning:      pb.InstanceState_INSTANCE_STATE_RUNNING,
		compute.InstanceStopping:     pb.InstanceState_INSTANCE_STATE_STOPPING,
		compute.InstanceStopped:      pb.InstanceState_INSTANCE_STATE_STOPPED,
		compute.InstanceShuttingDown: pb.InstanceState_INSTANCE_STATE_SHUTTING_DOWN,
		compute.InstanceTerminated:   pb.InstanceState_INSTANCE_STATE_TERMINATED,
		compute.InstanceError:        pb.InstanceState_INSTANCE_STATE_ERROR,
	}
	imageStates = map[compute.ImageLifecycle]pb.ImageState{
		compute.ImagePending:      pb.ImageState_IMAGE_STATE_PENDING,
		compute.ImageAvailable:    pb.ImageState_IMAGE_STATE_AVAILABLE,
		compute.ImageFailed:       pb.ImageState_IMAGE_STATE_FAILED,
		compute.ImageDeregistered: pb.ImageState_IMAGE_STATE_DEREGISTERED,
	}
	keyTypes = map[compute.KeyAlgorithm]pb.KeyType{
		compute.KeyED25519: pb.KeyType_KEY_TYPE_ED25519,
		compute.KeyRSA:     pb.KeyType_KEY_TYPE_RSA,
	}
	operationStates = map[compute.OperationState]pb.OperationState{
		compute.OperationPending:    pb.OperationState_OPERATION_STATE_PENDING,
		compute.OperationSucceeded:  pb.OperationState_OPERATION_STATE_SUCCEEDED,
		compute.OperationFailed:     pb.OperationState_OPERATION_STATE_FAILED,
		compute.OperationSuperseded: pb.OperationState_OPERATION_STATE_SUPERSEDED,
	}
	operationKinds = map[compute.OperationKind]pb.OperationKind{
		compute.OpRunInstance:       pb.OperationKind_OPERATION_KIND_RUN_INSTANCE,
		compute.OpStartInstance:     pb.OperationKind_OPERATION_KIND_START_INSTANCE,
		compute.OpStopInstance:      pb.OperationKind_OPERATION_KIND_STOP_INSTANCE,
		compute.OpRebootInstance:    pb.OperationKind_OPERATION_KIND_REBOOT_INSTANCE,
		compute.OpTerminateInstance: pb.OperationKind_OPERATION_KIND_TERMINATE_INSTANCE,
		compute.OpRegisterImage:     pb.OperationKind_OPERATION_KIND_REGISTER_IMAGE,
		compute.OpCreateImage:       pb.OperationKind_OPERATION_KIND_CREATE_IMAGE,
		compute.OpDeregisterImage:   pb.OperationKind_OPERATION_KIND_DEREGISTER_IMAGE,
		compute.OpCreateKeyPair:     pb.OperationKind_OPERATION_KIND_CREATE_KEY_PAIR,
		compute.OpImportKeyPair:     pb.OperationKind_OPERATION_KIND_IMPORT_KEY_PAIR,
		compute.OpDeleteKeyPair:     pb.OperationKind_OPERATION_KIND_DELETE_KEY_PAIR,
	}
)

func instancePB(i compute.Instance) *pb.Instance {
	out := &pb.Instance{
		InstanceId:   i.Spec.ID.String(),
		ImageId:      i.Spec.Image.String(),
		InstanceType: i.Spec.Type.Name(),
		State:        instanceStates[i.Lifecycle()],
		LaunchTime:   timestamppb.New(i.Spec.CreatedAt),
	}
	if k := i.Spec.Key; k != nil {
		out.KeyName, out.KeyFingerprint = k.Name.String(), k.PublicKey.Fingerprint()
	}
	if out.State == pb.InstanceState_INSTANCE_STATE_ERROR {
		out.StateReason = i.Status.Observed.Failure.Message
	}
	return out
}

func imagePB(i compute.Image) *pb.Image {
	out := &pb.Image{
		ImageId:      i.Spec.ID.String(),
		Name:         i.Spec.Name.String(),
		State:        imageStates[i.Lifecycle()],
		SizeBytes:    i.Status.SizeBytes,
		CreationTime: timestamppb.New(i.Spec.CreatedAt),
	}
	switch src := i.Spec.Source.(type) {
	case compute.FromURL:
		out.Source = &pb.Image_SourceUrl{SourceUrl: src.URL.String()}
	case compute.FromInstance:
		out.Source = &pb.Image_SourceInstanceId{SourceInstanceId: src.Instance.String()}
	}
	if out.State == pb.ImageState_IMAGE_STATE_FAILED {
		out.StateReason = i.Status.Observed.Failure.Message
	}
	return out
}

func keyPairPB(k compute.KeyPair) *pb.KeyPair {
	return &pb.KeyPair{
		KeyPairId:   k.Spec.ID.String(),
		KeyName:     k.Spec.Name.String(),
		KeyType:     keyTypes[k.Spec.PublicKey.Algorithm()],
		Fingerprint: k.Spec.PublicKey.Fingerprint(),
		CreateTime:  timestamppb.New(k.Spec.CreatedAt),
	}
}

func operationPB(o compute.Operation) *pb.Operation {
	out := &pb.Operation{
		OperationId: o.ID.String(),
		RequestId:   o.Request.String(),
		Kind:        operationKinds[o.Kind],
		State:       operationStates[o.State()],
		CreateTime:  timestamppb.New(o.CreatedAt),
	}
	switch o.Target.Kind {
	case compute.KindInstance:
		out.Target = &pb.Operation_InstanceId{InstanceId: o.Target.ID}
	case compute.KindImage:
		out.Target = &pb.Operation_ImageId{ImageId: o.Target.ID}
	case compute.KindKeyPair:
		out.Target = &pb.Operation_KeyPairId{KeyPairId: o.Target.ID}
	}
	if f := o.Failure(); f != nil {
		out.Error = &statuspb.Status{Code: int32(grpcCode(f.Code)), Message: f.Message}
	}
	if o.State().Terminal() {
		out.EndTime = timestamppb.New(o.SettledAt())
	}
	return out
}
