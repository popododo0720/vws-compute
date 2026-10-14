package grpcapi_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const volBase = "projects/demo/zones/z1/volumes/"

// fakeStorage is a grpcapi.VolumeLookup over a map.
type fakeStorage struct {
	mu    sync.Mutex
	vols  map[string]grpcapi.VolumeInfo
	err   error
	calls int
}

func (f *fakeStorage) Volume(_ context.Context, name string) (grpcapi.VolumeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return grpcapi.VolumeInfo{}, f.err
	}
	v, ok := f.vols[name]
	if !ok {
		return grpcapi.VolumeInfo{}, grpcapi.ErrVolumeNotFound
	}
	return v, nil
}

func (f *fakeStorage) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func createReq(id, req, volume string) *computev1.CreateInstanceRequest {
	return &computev1.CreateInstanceRequest{
		Parent: zoneParent, InstanceId: id, RequestId: req,
		Instance: &computev1.Instance{
			Labels: map[string]string{"app": "web"},
			Spec: &computev1.InstanceSpec{
				MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING, UserData: "#cloud-config",
				Volumes: []*computev1.VolumeAttachment{{Volume: volBase + volume, DeviceName: "vda", Boot: true}},
				NetworkInterfaces: []*computev1.NetworkInterface{{
					DeviceName: "eth0", Subnet: "projects/demo/regions/r/vpcs/v/subnets/s",
					SecurityGroups: []string{"projects/demo/regions/r/vpcs/v/securityGroups/g"},
				}},
			},
		},
	}
}

func TestCreateInstanceIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}, volBase + "root-2": {ClaimName: "pvc-root-2"}}}
	h := serveRepo(t, repo, grpcapi.WithVolumes(st))

	const req = "31313131-3131-4131-8131-313131313131"
	op, err := h.inst.CreateInstance(ctx, createReq("web-1", req, "root-1"))
	if err != nil {
		t.Fatal(err)
	}
	if op.GetDone() {
		t.Fatal("a controller-driven create is done only when the controller reports READY")
	}
	var md commonv1.OperationMetadata
	if err := anypb.UnmarshalTo(op.GetMetadata(), &md, proto.UnmarshalOptions{}); err != nil ||
		md.GetResource() != zoneParent+"/instances/web-1" || md.GetGeneration() != 1 {
		t.Fatalf("metadata = %v, %v", &md, err)
	}
	// The row exists and waits for the controller.
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: md.GetResource()})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSpec().GetUserData() != "#cloud-config" ||
		got.GetSpec().GetVolumes()[0].GetVolume() != volBase+"root-1" || got.GetLabels()["app"] != "web" ||
		got.GetSpec().GetNetworkInterfaces()[0].GetSubnet() != "projects/demo/regions/r/vpcs/v/subnets/s" {
		t.Fatalf("instance = %v", got)
	}
	checks := st.count()

	// A retry with the same request id asks storage nothing and returns the same operation,
	// even when storage is down.
	st.err = errors.New("storage is down")
	replay, err := h.inst.CreateInstance(ctx, createReq("web-1", req, "root-1"))
	if err != nil || replay.GetName() != op.GetName() || st.count() != checks {
		t.Fatalf("replay = %v, %v, storage calls %d -> %d", replay.GetName(), err, checks, st.count())
	}
	// The same holds across a restart.
	h.stop()
	h = serveRepo(t, repo, grpcapi.WithVolumes(st))
	replay, err = h.inst.CreateInstance(ctx, createReq("web-1", req, "root-1"))
	if err != nil || replay.GetName() != op.GetName() {
		t.Fatalf("replay after restart = %v, %v", replay.GetName(), err)
	}
	st.err = nil
	// Same content under a new request id: a retry from a client that lost the id.
	again, err := h.inst.CreateInstance(ctx, createReq("web-1", "32323232-3232-4232-8232-323232323232", "root-1"))
	if err != nil || again.GetError() != nil {
		t.Fatalf("same create, new request id = %v, %v", again, err)
	}
	// The same request id with another body is a conflict.
	_, err = h.inst.CreateInstance(ctx, createReq("web-1", req, "root-2"))
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")
	// Another body under a new request id is ALREADY_EXISTS.
	_, err = h.inst.CreateInstance(ctx, createReq("web-1", "33333333-3333-4333-8333-333333333333", "root-2"))
	wantCode(t, err, codes.AlreadyExists)
	// The only holder of a volume is the first live instance.
	_, err = h.inst.CreateInstance(ctx, createReq("web-2", "34343434-3434-4434-8434-343434343434", "root-1"))
	wantCode(t, err, codes.FailedPrecondition)
	if _, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: zoneParent + "/instances/web-2"}); err == nil {
		t.Fatal("a refused create stored an instance")
	}

	// The controller reports READY and the create operation succeeds.
	rep, err := h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
		Name: md.GetResource(),
		Status: &computev1.InstanceStatus{
			Observed: &commonv1.ObservedState{State: commonv1.ReconciliationState_RECONCILIATION_STATE_READY, Generation: 1, Node: "host-a"},
			Power:    computev1.InstancePower_INSTANCE_POWER_RUNNING,
		},
	})
	if err != nil || rep.GetInstance().GetStatus().GetObserved().GetGeneration() != 1 {
		t.Fatalf("report = %v, %v", rep, err)
	}
	if o := h.opByName(t, op.GetName()); !o.GetDone() || o.GetError() != nil {
		t.Fatalf("operation after READY = %v", o)
	}
}

func TestCreateInstanceVerifiesVolumes(t *testing.T) {
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{
		volBase + "ok":       {ClaimName: "pvc-ok"},
		volBase + "going":    {Deleting: true, ClaimName: "pvc-going"},
		volBase + "held":     {Instance: zoneParent + "/instances/other", ClaimName: "pvc-held"},
		volBase + "mine":     {Instance: zoneParent + "/instances/web", ClaimName: "pvc-mine"},
		volBase + "no-owner": {ClaimName: "pvc-no-owner"},
		volBase + "pending":  {},
	}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(st))
	n := 0
	req := func() string {
		n++
		return "4141414" + string(rune('0'+n)) + "-4141-4141-8141-414141414141"
	}
	for vol, want := range map[string]struct {
		code   codes.Code
		reason string
	}{
		"missing": {codes.FailedPrecondition, "DEPENDENCY_MISSING"},
		"going":   {codes.FailedPrecondition, "DEPENDENCY_MISSING"},
		"held":    {codes.FailedPrecondition, "VOLUME_IN_USE"},
	} {
		_, err := h.inst.CreateInstance(ctx, createReq("web", req(), vol))
		wantCode(t, err, want.code)
		wantReason(t, err, want.reason)
	}
	st.err = errors.New("connection refused")
	_, err := h.inst.CreateInstance(ctx, createReq("web", req(), "ok"))
	wantCode(t, err, codes.Unavailable)
	wantReason(t, err, "DEPENDENCY_UNAVAILABLE")
	st.err = nil
	if _, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: zoneParent + "/instances/web"}); err == nil {
		t.Fatal("a refused create stored an instance")
	}

	// validate_only verifies and writes nothing.
	vr := createReq("web", "", "ok")
	vr.ValidateOnly = true
	vo, err := h.inst.CreateInstance(ctx, vr)
	if err != nil || !vo.GetDone() {
		t.Fatalf("validate_only = %v, %v", vo, err)
	}
	if _, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: zoneParent + "/instances/web"}); err == nil {
		t.Fatal("validate_only stored an instance")
	}
	// A volume the storage service already names for this instance is fine.
	if _, err := h.inst.CreateInstance(ctx, createReq("web", req(), "mine")); err != nil {
		t.Fatalf("volume attached to this instance: %v", err)
	}
}

func TestCreateInstanceNeedsStorage(t *testing.T) {
	h := serve(t)
	_, err := h.inst.CreateInstance(context.Background(), createReq("web", "", "root"))
	wantCode(t, err, codes.FailedPrecondition)
	wantReason(t, err, "DEPENDENCY_MISSING")
}

// TestUpdateInstanceAttachesAndDetachesVolumes: volumes are mutable through
// UpdateInstance, carry the claim storage reports, and keep the boot entry.
func TestUpdateInstanceAttachesAndDetachesVolumes(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{
		volBase + "root-1":  {ClaimName: "pvc-root-1"},
		volBase + "root-2":  {ClaimName: "pvc-root-2"},
		volBase + "data":    {ClaimName: "pvc-data"},
		volBase + "held":    {Instance: zoneParent + "/instances/other", ClaimName: "pvc-held"},
		volBase + "pending": {},
	}}
	h := serveRepo(t, repo, grpcapi.WithVolumes(st))
	if _, err := h.inst.CreateInstance(ctx, createReq("web-1", "51515151-5151-4151-8151-515151515151", "root-1")); err != nil {
		t.Fatal(err)
	}
	name := zoneParent + "/instances/web-1"
	id, _ := compute.ParseInstanceID("web-1")
	if got, err := repo.Instance(ctx, id); err != nil || got.Spec.Volumes[0].Claim != "pvc-root-1" {
		t.Fatalf("create stored %+v, %v; want claim pvc-root-1", got.Spec.Volumes, err)
	}
	n := 0
	update := func(vols ...*computev1.VolumeAttachment) error {
		n++
		_, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId: "6161616" + string(rune('0'+n)) + "-6161-4161-8161-616161616161",
			Instance: &computev1.Instance{Name: name, Spec: &computev1.InstanceSpec{
				Power: computev1.InstancePower_INSTANCE_POWER_RUNNING, Volumes: vols,
			}},
		})
		return err
	}
	root := &computev1.VolumeAttachment{Volume: volBase + "root-1", DeviceName: "vda", Boot: true}
	att := func(vol string) *computev1.VolumeAttachment {
		return &computev1.VolumeAttachment{Volume: volBase + vol, DeviceName: "vdb"}
	}

	if err := update(root, att("data")); err != nil {
		t.Fatalf("attach: %v", err)
	}
	got, _ := repo.Instance(ctx, id)
	if got.Spec.Generation != 2 || len(got.Spec.Volumes) != 2 || got.Spec.Volumes[1].Claim != "pvc-data" {
		t.Fatalf("after attach = gen %d %+v", got.Spec.Generation, got.Spec.Volumes)
	}
	if err := update(root); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if got, _ := repo.Instance(ctx, id); got.Spec.Generation != 3 || len(got.Spec.Volumes) != 1 {
		t.Fatalf("after detach = gen %d %+v", got.Spec.Generation, got.Spec.Volumes)
	}

	err := update(root, att("held"))
	wantCode(t, err, codes.FailedPrecondition)
	wantReason(t, err, "VOLUME_IN_USE")
	err = update(&computev1.VolumeAttachment{Volume: volBase + "root-2", DeviceName: "vda", Boot: true})
	wantCode(t, err, codes.InvalidArgument)
	if got, _ := repo.Instance(ctx, id); got.Spec.Generation != 3 {
		t.Fatalf("a refused update wrote generation %d", got.Spec.Generation)
	}
	// A volume storage is still provisioning attaches with no claim yet; the
	// reconciler resolves it.
	if err := update(root, att("pending")); err != nil {
		t.Fatalf("attach a provisioning volume: %v", err)
	}
	if got, _ := repo.Instance(ctx, id); got.Spec.Generation != 4 || got.Spec.Volumes[1].Claim != "" {
		t.Fatalf("after attaching a provisioning volume = gen %d %+v", got.Spec.Generation, got.Spec.Volumes)
	}
}
