package grpcapi_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
)

// TestReportedVolumeAttachStatesRoundTrip is vws-proto#56 AC2 at the wire: a
// controller reports InstanceStatus.volumes and every state reads back on the
// instance with its state_time; a volume without a state or a valid name is
// refused and changes nothing.
func TestReportedVolumeAttachStatesRoundTrip(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	name := "projects/demo/zones/z1/instances/web-1"
	h.seedControllerDriven(t, "seed-1", "web-1", "root-1")

	at := time.Date(2026, 10, 30, 1, 2, 3, 0, time.UTC)
	report := func(vols ...*computev1.ObservedVolumeAttachment) (*computev1.ReportInstanceStatusResponse, error) {
		return h.inst.ReportInstanceStatus(ctx, &computev1.ReportInstanceStatusRequest{
			Name: name,
			Status: &computev1.InstanceStatus{
				Observed: &commonv1.ObservedState{
					State: commonv1.ReconciliationState_RECONCILIATION_STATE_READY, Generation: 1, Node: "host-a",
				},
				Power:   computev1.InstancePower_INSTANCE_POWER_RUNNING,
				Volumes: vols,
			},
		})
	}
	vol := func(v string, s computev1.VolumeAttachState) *computev1.ObservedVolumeAttachment {
		return &computev1.ObservedVolumeAttachment{
			Volume: "projects/demo/zones/z1/volumes/" + v, DeviceName: "/dev/" + v, State: s, StateTime: timestamppb.New(at),
		}
	}
	want := []*computev1.ObservedVolumeAttachment{
		vol("a", computev1.VolumeAttachState_VOLUME_ATTACH_STATE_ATTACHING),
		vol("b", computev1.VolumeAttachState_VOLUME_ATTACH_STATE_ATTACHED),
		vol("c", computev1.VolumeAttachState_VOLUME_ATTACH_STATE_DETACHING),
		vol("d", computev1.VolumeAttachState_VOLUME_ATTACH_STATE_BUSY),
	}

	_, err := report(vol("a", computev1.VolumeAttachState_VOLUME_ATTACH_STATE_UNSPECIFIED))
	wantCode(t, err, codes.InvalidArgument)
	_, err = report(&computev1.ObservedVolumeAttachment{
		Volume: "root-1", State: computev1.VolumeAttachState_VOLUME_ATTACH_STATE_ATTACHED,
	})
	wantCode(t, err, codes.InvalidArgument)
	_, err = report(&computev1.ObservedVolumeAttachment{
		Volume: "projects/demo/zones/z1/volumes/a", State: computev1.VolumeAttachState_VOLUME_ATTACH_STATE_ATTACHED,
		StateTime: &timestamppb.Timestamp{Nanos: -1},
	})
	wantCode(t, err, codes.InvalidArgument)
	got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(got.GetStatus().GetVolumes()); n != 0 {
		t.Fatalf("a refused report stored %d volumes", n)
	}

	if _, err := report(want...); err != nil {
		t.Fatal(err)
	}
	got, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	vols := got.GetStatus().GetVolumes()
	if len(vols) != len(want) {
		t.Fatalf("volumes = %v, want %v", vols, want)
	}
	for i, w := range want {
		g := vols[i]
		if g.GetVolume() != w.GetVolume() || g.GetDeviceName() != w.GetDeviceName() || g.GetState() != w.GetState() ||
			!g.GetStateTime().AsTime().Equal(at) {
			t.Fatalf("volume %d = %v, want %v", i, g, w)
		}
	}
}
