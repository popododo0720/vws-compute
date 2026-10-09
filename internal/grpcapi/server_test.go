package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/popododo0720/vws-compute/internal/compute"
	pb "github.com/popododo0720/vws-compute/internal/grpcapi/internal/computepb"
)

func TestMutatingRPCsTakeRequestIDAndReturnAnOperation(t *testing.T) {
	svc := pb.File_vws_compute_v1_compute_proto.Services().ByName("ComputeService")
	if svc == nil || svc.FullName() != "vws.compute.v1.ComputeService" {
		t.Fatal("vws.compute.v1.ComputeService not found")
	}
	reads := map[string]bool{"DescribeInstances": true, "DescribeImages": true, "DescribeKeyPairs": true, "GetOperation": true, "WaitOperation": true}
	mutating := 0
	for i := 0; i < svc.Methods().Len(); i++ {
		m := svc.Methods().Get(i)
		name := string(m.Name())
		reqID := m.Input().Fields().ByName("request_id")
		if reads[name] {
			if reqID != nil {
				t.Errorf("%s is a read but takes request_id", name)
			}
			continue
		}
		mutating++
		if reqID == nil || reqID.Kind() != protoreflect.StringKind {
			t.Errorf("%s mutates but has no string request_id", name)
		}
		op := m.Output().Fields().ByName("operation")
		if op == nil || op.Message() == nil || op.Message().FullName() != "vws.compute.v1.Operation" {
			t.Errorf("%s mutates but its response has no Operation", name)
		}
	}
	if mutating != len(compute.OperationKinds()) {
		t.Fatalf("%d mutating RPCs, want one per operation kind (%d)", mutating, len(compute.OperationKinds()))
	}
}

func TestOnlyCreateKeyPairResponseCarriesAPrivateKey(t *testing.T) {
	msgs := pb.File_vws_compute_v1_compute_proto.Messages()
	var found []string
	for i := 0; i < msgs.Len(); i++ {
		m := msgs.Get(i)
		for j := 0; j < m.Fields().Len(); j++ {
			f := m.Fields().Get(j)
			if strings.Contains(string(f.Name()), "private") {
				found = append(found, fmt.Sprintf("%s.%s", m.Name(), f.Name()))
			}
		}
	}
	if len(found) != 1 || found[0] != "CreateKeyPairResponse.private_key_openssh" {
		t.Fatalf("private fields = %q, want only CreateKeyPairResponse.private_key_openssh", found)
	}
}

// checkTable fails unless table maps every domain value to a distinct,
// specified wire value and covers every specified wire value.
func checkTable[K comparable, V interface {
	~int32
	Descriptor() protoreflect.EnumDescriptor
}](t *testing.T, name string, domain []K, table map[K]V) {
	t.Helper()
	seen := map[V]bool{}
	for _, k := range domain {
		v, ok := table[k]
		if !ok || v == 0 || seen[v] {
			t.Errorf("%s: %v maps to %v (present=%v, duplicate=%v)", name, k, v, ok, seen[v])
		}
		seen[v] = true
	}
	var zero V
	if want := zero.Descriptor().Values().Len() - 1; len(seen) != want || len(table) != len(domain) {
		t.Errorf("%s: %d domain values map onto %d wire values, want all %d", name, len(seen), want, want)
	}
}

func TestEnumTablesCoverDomainAndWire(t *testing.T) {
	checkTable(t, "operation kinds", compute.OperationKinds(), operationKinds)
	checkTable(t, "operation states", []compute.OperationState{compute.OperationPending, compute.OperationSucceeded, compute.OperationFailed, compute.OperationSuperseded}, operationStates)
	checkTable(t, "instance states", []compute.InstanceLifecycle{compute.InstancePending, compute.InstanceRunning, compute.InstanceStopping,
		compute.InstanceStopped, compute.InstanceShuttingDown, compute.InstanceTerminated, compute.InstanceError}, instanceStates)
	checkTable(t, "image states", []compute.ImageLifecycle{compute.ImagePending, compute.ImageAvailable, compute.ImageFailed, compute.ImageDeregistered}, imageStates)
	checkTable(t, "key types", []compute.KeyAlgorithm{compute.KeyED25519, compute.KeyRSA}, keyTypes)
}

func TestErrorsMapToGRPCCodes(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{compute.Errorf(compute.CodeInvalid, "x"), codes.InvalidArgument},
		{compute.Errorf(compute.CodeRequestMismatch, "x"), codes.InvalidArgument},
		{compute.Errorf(compute.CodeNotFound, "x"), codes.NotFound},
		{compute.Errorf(compute.CodeAlreadyExists, "x"), codes.AlreadyExists},
		{compute.Errorf(compute.CodeFailedPrecondition, "x"), codes.FailedPrecondition},
		{compute.Errorf(compute.CodeAborted, "x"), codes.Aborted},
		{compute.Errorf(compute.CodeInternal, "x"), codes.Internal},
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
