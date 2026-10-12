package grpcapi_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

func wantUnknownType(t *testing.T, err error, name string) {
	t.Helper()
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")
	want := `instance.spec.machine_type "` + name + `" is not a supported machine type`
	if got := status.Convert(err).Message(); got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}

// TestUnknownMachineTypeIsRejected: CreateInstance and UpdateInstance refuse
// a machine type outside the catalog before storage is asked or anything is
// written, instead of storing a 0 vCPU machine no agent can create.
func TestUnknownMachineTypeIsRejected(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	h := serveRepo(t, repo, grpcapi.WithVolumes(st))

	req := createReq("web-1", "81818181-8181-4181-8181-818181818181", "root-1")
	req.Instance.Spec.MachineType = "n1.medium"
	_, err := h.inst.CreateInstance(ctx, req)
	wantUnknownType(t, err, "n1.medium")
	req.ValidateOnly = true
	_, err = h.inst.CreateInstance(ctx, req)
	wantUnknownType(t, err, "n1.medium")
	if st.count() != 0 {
		t.Fatalf("storage was asked %d times for a refused request", st.count())
	}
	if _, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: zoneParent + "/instances/web-1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("refused create stored an instance: %v", err)
	}

	if _, err := h.inst.CreateInstance(ctx, createReq("web-1", "82828282-8282-4282-8282-828282828282", "root-1")); err != nil {
		t.Fatal(err)
	}
	update := func(machineType string) error {
		_, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{Instance: &computev1.Instance{
			Name: zoneParent + "/instances/web-1",
			Spec: &computev1.InstanceSpec{MachineType: machineType, Power: computev1.InstancePower_INSTANCE_POWER_RUNNING},
		}})
		return err
	}
	wantUnknownType(t, update("n1.medium"), "n1.medium")
	err = update("vws.medium")
	wantReason(t, err, "INVALID_SPEC")
	if got := status.Convert(err).Message(); got != "machine_type is immutable" {
		t.Fatalf("catalog type change = %q, want the immutable error", got)
	}
}

// TestStoredUnknownMachineTypeStaysManageable: an instance stored with an
// unlisted type before #45 still reads, accepts an update that repeats its
// type, and deletes.
func TestStoredUnknownMachineTypeStaysManageable(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	h := serveRepo(t, repo)

	now := time.Now()
	spec := compute.InstanceSpec{
		ID: must(compute.ParseInstanceID("legacy")), Generation: 1, CreatedAt: now, ChangedAt: now,
		Type: must(compute.ParseMachineType("n1.medium")), Power: compute.PowerRunning, Project: "demo", Zone: "z1",
		Volumes: []compute.VolumeAttachment{{Volume: must(compute.ParseVolumeRef(volBase + "old")), DeviceName: "vda", Boot: true, Claim: "pvc-old"}},
	}
	in := must(compute.NewTerminateInstance("legacy")) // any intent; the store records only its fingerprint
	op := must(compute.NewOperation(compute.Env{Now: now, Rand: rand.Reader}, must(compute.ParseRequestID("legacy-create")), in, compute.Change{Spec: spec, Write: true}))
	if _, err := repo.CommitDesired(ctx, store.DesiredChange{Op: op, Spec: spec, Write: true}); err != nil {
		t.Fatal(err)
	}

	name := zoneParent + "/instances/legacy"
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil || got.GetSpec().GetMachineType() != "n1.medium" {
		t.Fatalf("legacy instance = %v, %v", got, err)
	}
	if _, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{Instance: &computev1.Instance{
		Name: name, Spec: &computev1.InstanceSpec{MachineType: "n1.medium", Power: computev1.InstancePower_INSTANCE_POWER_STOPPED},
	}}); err != nil {
		t.Fatalf("update repeating the stored type: %v", err)
	}
	if _, err := h.inst.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: name}); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
