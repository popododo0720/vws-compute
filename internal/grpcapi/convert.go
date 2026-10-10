package grpcapi

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// etagOf is the generation until metadata changes, then generation.revision.
// A generation-only etag issued before the first metadata write still matches.
func etagOf(generation, revision int64) string {
	gen := strconv.FormatInt(generation, 10)
	if revision == 0 {
		return gen
	}
	return gen + "." + strconv.FormatInt(revision, 10)
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func checkEtag(got, want string) error {
	if got == "" || got == want {
		return nil
	}
	return aborted("ETAG_MISMATCH", "etag does not match")
}

func hexFingerprint(k compute.PublicKey) (string, error) {
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(k.Fingerprint(), "SHA256:"))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func instanceReconciliation(life compute.InstanceLifecycle) commonv1.ReconciliationState {
	switch life {
	case compute.InstancePending:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING
	case compute.InstanceRunning, compute.InstanceStopped:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_READY
	case compute.InstanceStopping:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING
	case compute.InstanceShuttingDown, compute.InstanceTerminated:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING
	case compute.InstanceError:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR
	default:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED
	}
}

func imageReconciliation(life compute.ImageLifecycle) commonv1.ReconciliationState {
	switch life {
	case compute.ImagePending:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING
	case compute.ImageAvailable:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_READY
	case compute.ImageFailed:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR
	case compute.ImageDeregistered:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING
	default:
		return commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED
	}
}

func powerPB(p compute.Power) computev1.InstancePower {
	if p == compute.PowerRunning {
		return computev1.InstancePower_INSTANCE_POWER_RUNNING
	}
	return computev1.InstancePower_INSTANCE_POWER_STOPPED
}

func instancePB(i compute.Instance, name string) *computev1.Instance {
	if name == "" {
		name = instanceResourceName(placeholderLocation, placeholderLocation, i.Spec.ID.String())
	}
	life := i.Lifecycle()
	spec := &computev1.InstanceSpec{
		MachineType: i.Spec.Type.Name(),
		Power:       powerPB(i.Spec.Power),
	}
	if i.Spec.Key != nil {
		spec.KeyPair = i.Spec.Key.Name.String()
	}
	var message string
	if life == compute.InstanceError && i.Status.Observed.Failure != nil {
		message = i.Status.Observed.Failure.Message
	}
	out := &computev1.Instance{
		Name:       name,
		Uid:        i.Spec.ID.String(),
		Generation: i.Spec.Generation,
		Etag:       etagOf(i.Spec.Generation, i.Meta.Revision),
		Labels:     i.Meta.Labels.Map(),
		Spec:       spec,
		Status: &computev1.InstanceStatus{
			Observed: &commonv1.ObservedState{
				State:      instanceReconciliation(life),
				Generation: i.Status.Observed.Generation,
				Message:    message,
			},
			Power: powerPB(i.Spec.Power),
		},
		CreateTime: timestamppb.New(i.Spec.CreatedAt),
		UpdateTime: timestamppb.New(latest(i.Spec.ChangedAt, i.Meta.ChangedAt)),
	}
	if i.Spec.Power == compute.PowerTerminated {
		out.DeleteTime = timestamppb.New(i.Spec.ChangedAt)
	}
	return out
}

func imagePB(i compute.Image, name string) *computev1.Image {
	if name == "" {
		name = imageResourceName(placeholderLocation, placeholderLocation, i.Spec.ID.String())
	}
	life := i.Lifecycle()
	var message string
	if life == compute.ImageFailed && i.Status.Observed.Failure != nil {
		message = i.Status.Observed.Failure.Message
	}
	progress := int32(0)
	if life == compute.ImageAvailable {
		progress = 100
	}
	out := &computev1.Image{
		Name:       name,
		Uid:        i.Spec.ID.String(),
		Generation: i.Spec.Generation,
		Etag:       etagOf(i.Spec.Generation, i.Meta.Revision),
		Labels:     i.Meta.Labels.Map(),
		Spec:       &computev1.ImageSpec{Family: i.Spec.Name.String(), Description: i.Meta.Description.String()},
		Status: &computev1.ImageStatus{
			Observed: &commonv1.ObservedState{
				State:      imageReconciliation(life),
				Generation: i.Status.Observed.Generation,
				Message:    message,
			},
			SizeGib:         i.Status.SizeBytes / (1 << 30),
			ProgressPercent: progress,
		},
		CreateTime: timestamppb.New(i.Spec.CreatedAt),
		UpdateTime: timestamppb.New(latest(i.Spec.ChangedAt, i.Meta.ChangedAt)),
	}
	if i.Spec.Deregistered {
		out.DeleteTime = timestamppb.New(i.Spec.ChangedAt)
	}
	return out
}

func keyPairPB(k compute.KeyPair) (*computev1.KeyPair, error) {
	fp, err := hexFingerprint(k.Spec.PublicKey)
	if err != nil {
		return nil, err
	}
	state := commonv1.ReconciliationState_RECONCILIATION_STATE_READY
	out := &computev1.KeyPair{
		Name:       k.Spec.Name.String(),
		Uid:        k.Spec.ID.String(),
		Generation: k.Spec.Generation,
		Etag:       etagOf(k.Spec.Generation, k.Meta.Revision),
		Labels:     k.Meta.Labels.Map(),
		Spec:       &computev1.KeyPairSpec{PublicKey: k.Spec.PublicKey.AuthorizedKey()},
		Status: &computev1.KeyPairStatus{
			Observed: &commonv1.ObservedState{
				State:      state,
				Generation: k.Spec.Generation,
			},
			FingerprintSha256: fp,
		},
		CreateTime: timestamppb.New(k.Spec.CreatedAt),
		UpdateTime: timestamppb.New(latest(k.Spec.ChangedAt, k.Meta.ChangedAt)),
	}
	if k.Spec.Deleted {
		out.DeleteTime = timestamppb.New(k.Spec.ChangedAt)
		out.Status.Observed.State = commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING
	}
	return out, nil
}

func operationVerb(k compute.OperationKind) commonv1.OperationVerb {
	switch k {
	case compute.OpRunInstance, compute.OpRegisterImage, compute.OpCreateImage, compute.OpCreateKeyPair, compute.OpImportKeyPair:
		return commonv1.OperationVerb_OPERATION_VERB_CREATE
	case compute.OpTerminateInstance, compute.OpDeregisterImage, compute.OpDeleteKeyPair:
		return commonv1.OperationVerb_OPERATION_VERB_DELETE
	default:
		return commonv1.OperationVerb_OPERATION_VERB_UPDATE
	}
}

func (s *server) operationPB(ctx context.Context, project, resourceName string, op compute.Operation) (*longrunningpb.Operation, error) {
	md := &commonv1.OperationMetadata{
		Verb:       operationVerb(op.Kind),
		Generation: op.TargetGeneration,
		RequestId:  op.Request.String(),
		CreateTime: timestamppb.New(op.CreatedAt),
	}
	if op.State().Terminal() {
		md.EndTime = timestamppb.New(op.SettledAt())
	}
	resource, response, err := s.operationResource(ctx, project, resourceName, op)
	if err != nil {
		return nil, err
	}
	md.Resource = resource
	metaAny, err := anypb.New(md)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &longrunningpb.Operation{
		Name:     operationResourceName(project, op.ID.String()),
		Metadata: metaAny,
		Done:     op.State().Terminal(),
	}
	if !out.GetDone() {
		return out, nil
	}
	if f := op.Failure(); f != nil {
		out.Result = &longrunningpb.Operation_Error{Error: &statuspb.Status{
			Code:    int32(grpcCode(f.Code)),
			Message: f.Message,
		}}
		return out, nil
	}
	if op.State() == compute.OperationSuperseded {
		out.Result = &longrunningpb.Operation_Error{Error: &statuspb.Status{
			Code:    int32(codes.Aborted),
			Message: "superseded",
		}}
		return out, nil
	}
	if response != nil {
		body, err := anypb.New(response)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Result = &longrunningpb.Operation_Response{Response: body}
	}
	return out, nil
}

func (s *server) operationResource(ctx context.Context, project, resourceName string, op compute.Operation) (string, proto.Message, error) {
	switch op.Target.Kind {
	case compute.KindInstance:
		name := resourceName
		if name == "" {
			name = instanceResourceName(project, placeholderLocation, op.Target.ID)
		}
		id, err := compute.ParseInstanceID(op.Target.ID)
		if err != nil {
			return name, nil, nil
		}
		inst, err := s.read.Instance(ctx, id)
		if err != nil {
			if compute.CodeOf(err) == compute.CodeNotFound {
				return name, nil, nil
			}
			return "", nil, toStatus(err)
		}
		return name, instancePB(inst, name), nil
	case compute.KindImage:
		name := resourceName
		if name == "" {
			name = imageResourceName(project, placeholderLocation, op.Target.ID)
		}
		id, err := compute.ParseImageID(op.Target.ID)
		if err != nil {
			return name, nil, nil
		}
		img, err := s.read.Image(ctx, id)
		if err != nil {
			if compute.CodeOf(err) == compute.CodeNotFound {
				return name, nil, nil
			}
			return "", nil, toStatus(err)
		}
		return name, imagePB(img, name), nil
	case compute.KindKeyPair:
		id, err := compute.ParseKeyPairID(op.Target.ID)
		if err != nil {
			return "", nil, nil
		}
		kp, err := s.read.KeyPair(ctx, id)
		if err != nil {
			if compute.CodeOf(err) == compute.CodeNotFound {
				return "", nil, nil
			}
			return "", nil, toStatus(err)
		}
		pb, err := keyPairPB(kp)
		if err != nil {
			return "", nil, status.Error(codes.Internal, err.Error())
		}
		return pb.GetName(), pb, nil
	default:
		return "", nil, nil
	}
}
