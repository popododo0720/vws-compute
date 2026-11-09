package grpcapi_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestAttachTimeIsServerOwned is vws-compute#166 at the wire: attach_time is
// set by the server when a volume enters spec.volumes, ignored when a client
// or a controller sends it, kept on later updates, and reported on the
// status entry from the spec through ATTACHED, BUSY and DETACHING.
func TestAttachTimeIsServerOwned(t *testing.T) {
	ctx := context.Background()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{
		volBase + "root-1": {ClaimName: "pvc-root-1"},
		volBase + "data":   {ClaimName: "pvc-data"},
	}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(st))
	forged := timestamppb.New(time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC))
	name := zoneParent + "/instances/web-1"

	create := createReq("web-1", "71717171-7171-4171-8171-717171717171", "root-1")
	create.Instance.Spec.Volumes[0].AttachTime = forged
	before := time.Now()
	if _, err := h.inst.CreateInstance(ctx, create); err != nil {
		t.Fatal(err)
	}
	get := func() *computev1.Instance {
		t.Helper()
		got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	specTime := func(i *computev1.Instance, vol string) time.Time {
		t.Helper()
		for _, v := range i.GetSpec().GetVolumes() {
			if v.GetVolume() == volBase+vol {
				if v.GetAttachTime() == nil {
					t.Fatalf("spec volume %s has no attach_time", vol)
				}
				return v.GetAttachTime().AsTime()
			}
		}
		t.Fatalf("spec has no volume %s: %v", vol, i.GetSpec().GetVolumes())
		return time.Time{}
	}
	rootAt := specTime(get(), "root-1")
	if rootAt.Before(before) {
		t.Fatalf("root attach_time = %v, want the server's time, not the client's", rootAt)
	}

	n := 0
	update := func(vols ...*computev1.VolumeAttachment) {
		t.Helper()
		n++
		if _, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId: "8181818" + string(rune('0'+n)) + "-8181-4181-8181-818181818181",
			Instance: &computev1.Instance{Name: name, Spec: &computev1.InstanceSpec{
				Power: computev1.InstancePower_INSTANCE_POWER_RUNNING, Volumes: vols,
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	root := &computev1.VolumeAttachment{Volume: volBase + "root-1", DeviceName: "vda", Boot: true, AttachTime: forged}
	data := &computev1.VolumeAttachment{Volume: volBase + "data", DeviceName: "vdb", AttachTime: forged}
	update(root, data)
	attached := get()
	if !specTime(attached, "root-1").Equal(rootAt) {
		t.Fatalf("attach moved the root attach_time to %v", specTime(attached, "root-1"))
	}
	dataAt := specTime(attached, "data")
	if dataAt.Before(rootAt) {
		t.Fatalf("data attach_time = %v, want the server's time at attach", dataAt)
	}

	report := func(generation int64, state computev1.VolumeAttachState) *computev1.ObservedVolumeAttachment {
		t.Helper()
		if _, err := h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
			Name: name,
			Status: &computev1.InstanceStatus{
				Observed: &commonv1.ObservedState{
					State: commonv1.ReconciliationState_RECONCILIATION_STATE_READY, Generation: generation, Node: "host-a",
				},
				Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
				Volumes: []*computev1.ObservedVolumeAttachment{{
					Volume: volBase + "data", DeviceName: "vdb", State: state, StateTime: timestamppb.Now(), AttachTime: forged,
				}},
			},
		}); err != nil {
			t.Fatal(err)
		}
		vols := get().GetStatus().GetVolumes()
		if len(vols) != 1 {
			t.Fatalf("status volumes = %v", vols)
		}
		return vols[0]
	}
	for _, s := range []computev1.VolumeAttachState{
		computev1.VolumeAttachState_VOLUME_ATTACH_STATE_ATTACHING,
		computev1.VolumeAttachState_VOLUME_ATTACH_STATE_ATTACHED,
		computev1.VolumeAttachState_VOLUME_ATTACH_STATE_BUSY,
	} {
		if got := report(2, s); !got.GetAttachTime().AsTime().Equal(dataAt) {
			t.Fatalf("%v status attach_time = %v, want the spec's %v", s, got.GetAttachTime(), dataAt)
		}
	}

	update(root)
	got := report(3, computev1.VolumeAttachState_VOLUME_ATTACH_STATE_DETACHING)
	if !got.GetAttachTime().AsTime().Equal(dataAt) {
		t.Fatalf("DETACHING status attach_time = %v, want %v", got.GetAttachTime(), dataAt)
	}
}
