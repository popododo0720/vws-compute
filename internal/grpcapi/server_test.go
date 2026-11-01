package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

func TestServicesMatchTheResourceAPI(t *testing.T) {
	files := []protoreflect.FileDescriptor{
		computev1.File_vws_compute_v1_instance_proto,
		computev1.File_vws_compute_v1_image_proto,
		computev1.File_vws_compute_v1_key_pair_proto,
	}
	for _, file := range files {
		if file.Services().Len() != 1 {
			t.Fatalf("%s has %d services", file.Path(), file.Services().Len())
		}
		svc := file.Services().Get(0)
		for i := 0; i < svc.Methods().Len(); i++ {
			m := svc.Methods().Get(i)
			name := string(m.Name())
			reqID := m.Input().Fields().ByName("request_id")
			switch {
			case strings.HasPrefix(name, "Get"), strings.HasPrefix(name, "List"):
				if reqID != nil {
					t.Errorf("%s is a read but takes request_id", name)
				}
			case strings.HasPrefix(name, "Report"):
				if reqID == nil || reqID.Kind() != protoreflect.StringKind {
					t.Errorf("%s does not take request_id", name)
				}
				if m.Output().FullName() == "google.longrunning.Operation" {
					t.Errorf("%s returns an operation; reports return the resource", name)
				}
			default:
				if reqID == nil || reqID.Kind() != protoreflect.StringKind {
					t.Errorf("%s mutates but has no string request_id", name)
				}
				if m.Output().FullName() != "google.longrunning.Operation" {
					t.Errorf("%s output = %s, want google.longrunning.Operation", name, m.Output().FullName())
				}
			}
		}
	}
}

func TestComputeMessagesCarryNoPrivateKey(t *testing.T) {
	files := []protoreflect.FileDescriptor{
		computev1.File_vws_compute_v1_instance_proto,
		computev1.File_vws_compute_v1_image_proto,
		computev1.File_vws_compute_v1_key_pair_proto,
	}
	var found []string
	for _, file := range files {
		msgs := file.Messages()
		for i := 0; i < msgs.Len(); i++ {
			m := msgs.Get(i)
			for j := 0; j < m.Fields().Len(); j++ {
				f := m.Fields().Get(j)
				if strings.Contains(string(f.Name()), "private_key") {
					found = append(found, fmt.Sprintf("%s.%s", m.Name(), f.Name()))
				}
			}
		}
	}
	if len(found) != 0 {
		t.Fatalf("private fields = %q, want none", found)
	}
}

func TestReconciliationCoversLifecycles(t *testing.T) {
	for _, life := range []compute.InstanceLifecycle{
		compute.InstancePending, compute.InstanceRunning, compute.InstanceStopping,
		compute.InstanceStopped, compute.InstanceShuttingDown, compute.InstanceTerminated, compute.InstanceError,
	} {
		if instanceReconciliation(life) == commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED {
			t.Errorf("instance lifecycle %d has no reconciliation state", life)
		}
	}
	for _, life := range []compute.ImageLifecycle{compute.ImagePending, compute.ImageAvailable, compute.ImageFailed, compute.ImageDeregistered} {
		if imageReconciliation(life) == commonv1.ReconciliationState_RECONCILIATION_STATE_UNSPECIFIED {
			t.Errorf("image lifecycle %d has no reconciliation state", life)
		}
	}
}

func TestErrorsMapToGRPCCodes(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{compute.Errorf(compute.CodeInvalid, "x"), codes.InvalidArgument},
		{compute.Errorf(compute.CodeRequestMismatch, "x"), codes.Aborted},
		{compute.Errorf(compute.CodeNotFound, "x"), codes.NotFound},
		{compute.Errorf(compute.CodeAlreadyExists, "x"), codes.AlreadyExists},
		{compute.Errorf(compute.CodeFailedPrecondition, "x"), codes.FailedPrecondition},
		{compute.Errorf(compute.CodeAborted, "x"), codes.Aborted},
		{compute.Errorf(compute.CodeInternal, "x"), codes.Internal},
		{compute.Errorf(compute.CodeLimitExceeded, "x"), codes.ResourceExhausted},
		{fmt.Errorf("wrapped: %w", compute.Errorf(compute.CodeNotFound, "x")), codes.NotFound},
		{errors.New("disk full"), codes.Internal},
		{context.Canceled, codes.Canceled},
		{fmt.Errorf("query: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
	} {
		if got := status.Code(toStatus(tc.err)); got != tc.want {
			t.Errorf("toStatus(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
}

// classifiedSpecFields is every spec field this server has decided how to
// treat. A vendored proto that adds a field fails this test until that
// field is classified: stored, rejected, or explicitly ignored.
var classifiedSpecFields = map[string]map[string]bool{
	"InstanceSpec": {
		"machine_type":       true,
		"power":              true,
		"key_pair":           true,
		"user_data":          true,
		"user_data_raw":      true,
		"network_interfaces": true,
		"volumes":            true,
	},
	"ImageSpec": {
		"volume":      true,
		"snapshot":    true,
		"description": true,
		"family":      true,
	},
	"KeyPairSpec": {
		"public_key": true,
	},
}

func TestMaskClassifiesEverySpecField(t *testing.T) {
	for _, msg := range []protoreflect.MessageDescriptor{
		(&computev1.InstanceSpec{}).ProtoReflect().Descriptor(),
		(&computev1.ImageSpec{}).ProtoReflect().Descriptor(),
		(&computev1.KeyPairSpec{}).ProtoReflect().Descriptor(),
	} {
		allowed := classifiedSpecFields[string(msg.Name())]
		fields := msg.Fields()
		if fields.Len() != len(allowed) {
			t.Errorf("%s has %d fields, classification lists %d", msg.Name(), fields.Len(), len(allowed))
		}
		for i := 0; i < fields.Len(); i++ {
			name := string(fields.Get(i).Name())
			if !allowed[name] {
				t.Errorf("%s.%s is not classified", msg.Name(), name)
			}
		}
	}
}

func TestNodeServiceIsNotServed(t *testing.T) {
	repo := memstore.New()
	srv := grpc.NewServer()
	Register(srv, controller.New(repo), repo)
	info := srv.GetServiceInfo()
	for name := range info {
		if strings.Contains(name, "Node") {
			t.Errorf("registered %s", name)
		}
	}
	if _, ok := info["vws.compute.v1.InstanceService"]; !ok {
		t.Fatalf("InstanceService missing from %v", info)
	}
}
