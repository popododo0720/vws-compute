package grpcapi_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	storagev1 "github.com/popododo0720/vws-proto/gen/go/vws/storage/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// storageClient answers GetVolume from a map of storage Volumes, so the
// test goes through NewStorageVolumes' mapping of the storage status.
type storageClient struct {
	storagev1.VolumeServiceClient
	vols map[string]*storagev1.Volume
}

func (c storageClient) GetVolume(_ context.Context, r *storagev1.GetVolumeRequest, _ ...grpc.CallOption) (*storagev1.Volume, error) {
	v, ok := c.vols[r.GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no volume")
	}
	return v, nil
}

func storageVolume(state commonv1.ReconciliationState, msg, claim string) *storagev1.Volume {
	return &storagev1.Volume{Status: &storagev1.VolumeStatus{
		Observed:  &commonv1.ObservedState{State: state, Message: msg},
		ClaimName: claim,
	}}
}

// TestAttachFailedVolume: a volume storage left in ERROR is refused as
// VOLUME_FAILED with storage's message, on create and on attach; one still
// provisioning stays VOLUME_NOT_PROVISIONED (vws-compute#68).
func TestAttachFailedVolume(t *testing.T) {
	ctx := context.Background()
	const storageMsg = "ceph pool rbd-ssd is full"
	sc := storageClient{vols: map[string]*storagev1.Volume{
		volBase + "root":    storageVolume(commonv1.ReconciliationState_RECONCILIATION_STATE_READY, "", "pvc-root"),
		volBase + "broken":  storageVolume(commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR, storageMsg, ""),
		volBase + "pending": storageVolume(commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING, "creating claim", ""),
	}}
	h := serveRepo(t, memstore.New(), grpcapi.WithVolumes(grpcapi.NewStorageVolumes(sc)))

	wantFailed := func(t *testing.T, err error) {
		t.Helper()
		wantCode(t, err, codes.FailedPrecondition)
		wantReason(t, err, "VOLUME_FAILED")
		if msg := status.Convert(err).Message(); !strings.Contains(msg, storageMsg) {
			t.Fatalf("message %q lacks storage's %q", msg, storageMsg)
		}
	}

	_, err := h.inst.CreateInstance(ctx, createReq("web-0", "71717170-7171-4171-8171-717171717171", "broken"))
	wantFailed(t, err)
	_, err = h.inst.CreateInstance(ctx, createReq("web-0", "71717171-7171-4171-8171-717171717171", "pending"))
	wantCode(t, err, codes.FailedPrecondition)
	wantReason(t, err, "VOLUME_NOT_PROVISIONED")

	if _, err := h.inst.CreateInstance(ctx, createReq("web-1", "71717172-7171-4171-8171-717171717171", "root")); err != nil {
		t.Fatal(err)
	}
	attach := func(vol, req string) error {
		_, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId: req,
			Instance: &computev1.Instance{Name: zoneParent + "/instances/web-1", Spec: &computev1.InstanceSpec{
				Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
				Volumes: []*computev1.VolumeAttachment{
					{Volume: volBase + "root", DeviceName: "vda", Boot: true},
					{Volume: volBase + vol, DeviceName: "vdb"},
				},
			}},
		})
		return err
	}
	wantFailed(t, attach("broken", "71717173-7171-4171-8171-717171717171"))
	err = attach("pending", "71717174-7171-4171-8171-717171717171")
	wantCode(t, err, codes.FailedPrecondition)
	wantReason(t, err, "VOLUME_NOT_PROVISIONED")
}
