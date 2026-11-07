package grpcapi_test

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// rawUserData is EC2 UserData that a proto3 string cannot carry.
var rawUserData = []byte("\xff\xfe#!/bin/sh\x00\x80echo hi\n")

func rawCreateReq(id, req string, raw []byte) *computev1.CreateInstanceRequest {
	r := createReq(id, req, "root-1")
	r.Instance.Spec.UserData = ""
	r.Instance.Spec.UserDataRaw = raw
	return r
}

// TestCreateInstanceUserDataRaw pins vws-compute#148: user_data_raw is stored
// and returned byte for byte, survives a restart, and is part of the request
// fingerprint.
func TestCreateInstanceUserDataRaw(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	h := serveRepo(t, repo, grpcapi.WithVolumes(st))

	const req = "41414141-4141-4141-8141-414141414141"
	op, err := h.inst.CreateInstance(ctx, rawCreateReq("web-1", req, rawUserData))
	if err != nil {
		t.Fatal(err)
	}
	name := zoneParent + "/instances/web-1"
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.GetSpec().GetUserDataRaw(), rawUserData) || got.GetSpec().GetUserData() != "" {
		t.Fatalf("spec user data = %q raw %q, want raw %q",
			got.GetSpec().GetUserData(), got.GetSpec().GetUserDataRaw(), rawUserData)
	}

	// A replay of the same body, also after a restart, is the same operation.
	h.stop()
	h = serveRepo(t, repo, grpcapi.WithVolumes(st))
	replay, err := h.inst.CreateInstance(ctx, rawCreateReq("web-1", req, rawUserData))
	if err != nil || replay.GetName() != op.GetName() {
		t.Fatalf("replay = %v, %v; want %s", replay.GetName(), err, op.GetName())
	}

	// The same request id with other raw bytes, or with the same bytes sent
	// as user_data, is a different request.
	other := append(bytes.Clone(rawUserData), 'x')
	_, err = h.inst.CreateInstance(ctx, rawCreateReq("web-1", req, other))
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")
	asText := createReq("web-1", req, "root-1")
	asText.Instance.Spec.UserData = "#!/bin/sh"
	_, err = h.inst.CreateInstance(ctx, rawCreateReq("web-1", req, []byte("#!/bin/sh")))
	wantCode(t, err, codes.Aborted)
	_, err = h.inst.CreateInstance(ctx, asText)
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")

	// Other bytes under a new request id do not replace the instance.
	_, err = h.inst.CreateInstance(ctx, rawCreateReq("web-1", "42424242-4242-4242-8242-424242424242", other))
	wantCode(t, err, codes.AlreadyExists)

	// user_data_raw is not mutable.
	_, err = h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId: "43434343-4343-4343-8343-434343434343", UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}},
		Instance: &computev1.Instance{Name: name, Spec: &computev1.InstanceSpec{
			Power: computev1.InstancePower_INSTANCE_POWER_RUNNING, UserDataRaw: other,
		}},
	})
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")
	got, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil || !bytes.Equal(got.GetSpec().GetUserDataRaw(), rawUserData) {
		t.Fatalf("after refused update = %q, %v", got.GetSpec().GetUserDataRaw(), err)
	}
}

// TestCreateInstanceRejectsBadUserDataRaw pins the vws-proto validation rule:
// user_data and user_data_raw together, or user_data_raw over 32768 bytes, is
// INVALID_ARGUMENT / INVALID_SPEC, and nothing is written or asked of storage.
func TestCreateInstanceRejectsBadUserDataRaw(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*computev1.InstanceSpec)
	}{
		{"both set", func(s *computev1.InstanceSpec) { s.UserData, s.UserDataRaw = "#cloud-config", rawUserData }},
		{"both set, one byte each", func(s *computev1.InstanceSpec) { s.UserData, s.UserDataRaw = "#", []byte{0} }},
		{"raw over the limit", func(s *computev1.InstanceSpec) {
			s.UserData, s.UserDataRaw = "", make([]byte, compute.MaxUserDataBytes+1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			repo := memstore.New()
			st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
			h := serveRepo(t, repo, grpcapi.WithVolumes(st))
			const req = "44444444-4444-4444-8444-444444444444"
			r := createReq("web-1", req, "root-1")
			tc.mut(r.Instance.Spec)
			_, err := h.inst.CreateInstance(ctx, r)
			wantCode(t, err, codes.InvalidArgument)
			wantReason(t, err, "INVALID_SPEC")
			_, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: zoneParent + "/instances/web-1"})
			if err == nil {
				t.Fatal("a refused create stored an instance")
			}
			id, err := compute.ParseRequestID(req)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.OperationByRequest(ctx, id); compute.CodeOf(err) != compute.CodeNotFound {
				t.Fatalf("a refused create stored an operation: %v", err)
			}
			if st.count() != 0 {
				t.Fatalf("a refused create asked storage %d times", st.count())
			}
		})
	}

	// Exactly 32768 bytes is accepted.
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(st))
	if _, err := h.inst.CreateInstance(ctx, rawCreateReq("web-1", "45454545-4545-4545-8545-454545454545",
		bytes.Repeat([]byte{0xff}, compute.MaxUserDataBytes))); err != nil {
		t.Fatalf("raw user data at the limit: %v", err)
	}
}
