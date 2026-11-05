package grpcapi_test

import (
	"context"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// awsCode is the ErrorInfo metadata aws_code of err, or "" without one.
func awsCode(t *testing.T, err error) string {
	t.Helper()
	st, _ := status.FromError(err)
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == "vws.dev" {
			return info.GetMetadata()["aws_code"]
		}
	}
	return ""
}

func wantZoneMismatch(t *testing.T, what string, err error) {
	t.Helper()
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")
	if got := awsCode(t, err); got != "InvalidVolume.ZoneMismatch" {
		t.Fatalf("%s: aws_code = %q, want InvalidVolume.ZoneMismatch (err %v)", what, got, err)
	}
}

// TestVolumeInAnotherZoneIsZoneMismatch: a volume whose zone is not the
// instance zone is refused as EC2 AttachVolume refuses it, with
// InvalidVolume.ZoneMismatch in ErrorInfo metadata aws_code under
// INVALID_SPEC. A replay and a restart answer the same and nothing is
// written. A same-zone attach still succeeds and other volume faults keep
// INVALID_SPEC with no aws_code.
func TestVolumeInAnotherZoneIsZoneMismatch(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{
		volBase + "root":                         {ClaimName: "pvc-root"},
		volBase + "data":                         {ClaimName: "pvc-data"},
		"projects/demo/zones/z2/volumes/far":     {ClaimName: "pvc-far"},
		"projects/demo/zones/z10/volumes/prefix": {ClaimName: "pvc-prefix"},
	}}
	h := serveRepo(t, repo, grpcapi.WithVolumes(st))

	cross := createReq("web-0", "71717171-7171-4171-8171-717171717171", "root")
	cross.Instance.Spec.Volumes[0].Volume = "projects/demo/zones/z2/volumes/far"
	_, err := h.inst.CreateInstance(ctx, cross)
	wantZoneMismatch(t, "create", err)

	_, err = h.inst.CreateInstance(ctx, createReq("web-1", "72727272-7272-4272-8272-727272727272", "root"))
	if err != nil {
		t.Fatal(err)
	}
	name := zoneParent + "/instances/web-1"
	id, _ := compute.ParseInstanceID("web-1")
	root := &computev1.VolumeAttachment{Volume: volBase + "root", DeviceName: "vda", Boot: true}
	update := func(h *harness, req string, vols ...*computev1.VolumeAttachment) error {
		_, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId: req,
			Instance: &computev1.Instance{Name: name, Spec: &computev1.InstanceSpec{
				Power: computev1.InstancePower_INSTANCE_POWER_RUNNING, Volumes: vols,
			}},
		})
		return err
	}
	data := func(vol string) *computev1.VolumeAttachment {
		return &computev1.VolumeAttachment{Volume: vol, DeviceName: "vdb"}
	}
	const attachReq = "73737373-7373-4373-8373-737373737373"
	calls := st.count()
	for _, vol := range []string{"projects/demo/zones/z2/volumes/far", "projects/demo/zones/z10/volumes/prefix"} {
		wantZoneMismatch(t, "attach "+vol, update(h, attachReq, root, data(vol)))
	}
	wantZoneMismatch(t, "replay", update(h, attachReq, root, data("projects/demo/zones/z2/volumes/far")))
	restarted := serveRepo(t, repo, grpcapi.WithVolumes(st))
	wantZoneMismatch(t, "after restart", update(restarted, attachReq, root, data("projects/demo/zones/z2/volumes/far")))
	if got, _ := repo.Instance(ctx, id); got.Spec.Generation != 1 || len(got.Spec.Volumes) != 1 {
		t.Fatalf("a cross-zone attach wrote gen %d %+v", got.Spec.Generation, got.Spec.Volumes)
	}
	if st.count() != calls {
		t.Fatalf("a cross-zone attach asked the storage service %d times", st.count()-calls)
	}

	dup := &computev1.VolumeAttachment{Volume: volBase + "data", DeviceName: "vda"}
	err = update(h, "74747474-7474-4474-8474-747474747474", root, dup)
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")
	if got := awsCode(t, err); got != "" {
		t.Fatalf("a duplicate device carries aws_code %q", got)
	}

	if err := update(h, "75757575-7575-4575-8575-757575757575", root, data(volBase+"data")); err != nil {
		t.Fatalf("same-zone attach: %v", err)
	}
	if got, _ := repo.Instance(ctx, id); got.Spec.Generation != 2 || len(got.Spec.Volumes) != 2 {
		t.Fatalf("after same-zone attach = gen %d %+v", got.Spec.Generation, got.Spec.Volumes)
	}
}
