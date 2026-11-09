package grpcapi_test

import (
	"context"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestSharedSnapshotClonesTheOwnersObject is vws-compute#164: the owner and
// the caller each have a snapshot named "shared". A CreateImage of the
// owner's snapshot, shared with the caller, clones the VolumeSnapshot storage
// reports for the owner's full resource name, never the caller's one and
// never one named after the base name. The caller's own snapshot clones its
// own object. A replay converges nothing new, and after a restart the image
// is restored from the owner's object. An unshared or absent snapshot never
// reaches the agent.
func TestSharedSnapshotClonesTheOwnersObject(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		h := serveRepo(t, memstore.New().WithLegacyImageScope(demoScope), grpcapi.WithSnapshotPermissions(newGrants()))
		sharedSnapshotClonesTheOwnersObject(t, h)
	})
	t.Run("envtest", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		open := func() store.Repository { return kubetest.Open(t, kc, ns).WithLegacyImageScope(demoScope) }
		h := serveRepo(t, open(), grpcapi.WithSnapshotPermissions(newGrants()))
		owner, _ := sharedSnapshotClonesTheOwnersObject(t, h)
		h.stop()
		// A fresh agent is rebuilt from the stored image, with no storage
		// lookup deciding the object again.
		h = serveRepo(t, open(), grpcapi.WithSnapshotPermissions(newGrants()))
		deadline := time.Now().Add(10 * time.Second)
		for len(sourcesOf(h.images.sources(), owner)) == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("image %s was not restored to the fresh agent", owner)
			}
			time.Sleep(10 * time.Millisecond)
		}
		for _, s := range h.images.sources() {
			if s.Name == owner && s.SourceVolumeSnapshot != objectOf("snap", sharedSnap) {
				t.Fatalf("restored %s clones %q, want the owner's %q", owner, s.SourceVolumeSnapshot, objectOf("snap", sharedSnap))
			}
		}
	})
}

// sharedSnapshotClonesTheOwnersObject returns the agent names of the image
// copied from the owner's snapshot and of the one from the caller's own.
func sharedSnapshotClonesTheOwnersObject(t *testing.T, h *harness) (owner, own string) {
	t.Helper()
	ctx := context.Background()
	callerSnap := sharedParent + "/snapshots/shared"
	ownerObject, callerObject := objectOf("snap", sharedSnap), objectOf("snap", callerSnap)
	if ownerObject == callerObject {
		t.Fatal("the fake storage gave both snapshots one object")
	}

	create := func(snapshot, imageID, requestID string) string {
		t.Helper()
		req := &computev1.CreateImageRequest{
			Parent: sharedParent, ImageId: imageID, RequestId: requestID,
			Image: &computev1.Image{Spec: snapSpec(snapshot)},
		}
		op, err := h.img.CreateImage(ctx, req)
		if err != nil {
			t.Fatalf("CreateImage of %s: %v", snapshot, err)
		}
		done, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{
			Name: op.GetName(), Timeout: durationpb.New(10 * time.Second),
		})
		if err != nil || !done.GetDone() || done.GetError() != nil {
			t.Fatalf("operation %v %v", done, err)
		}
		again, err := h.img.CreateImage(ctx, req)
		if err != nil || again.GetName() != op.GetName() {
			t.Fatalf("replay = %v %v, want operation %s", again.GetName(), err, op.GetName())
		}
		img := &computev1.Image{}
		if err := done.GetResponse().UnmarshalTo(img); err != nil {
			t.Fatal(err)
		}
		return img.GetName()[len(sharedParent+"/images/"):]
	}

	owner = create(sharedSnap, "ami-0000000000000eee1", "e1e1e1e1-e1e1-41e1-81e1-e1e1e1e1e1e1")
	own = create(callerSnap, "ami-0000000000000eee2", "e2e2e2e2-e2e2-42e2-82e2-e2e2e2e2e2e2")
	for name, want := range map[string]string{owner: ownerObject, own: callerObject} {
		specs := sourcesOf(h.images.sources(), name)
		if len(specs) == 0 {
			t.Fatalf("the agent never got image %s", name)
		}
		for _, s := range specs {
			if s.SourceVolumeSnapshot != want {
				t.Fatalf("image %s clones %q, want %q", name, s.SourceVolumeSnapshot, want)
			}
		}
	}

	before := len(h.images.sources())
	for i, snapshot := range []string{privateSnap, absentSnap} {
		_, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
			Parent: sharedParent, ImageId: "ami-0000000000000eee3",
			RequestId: []string{"e3e3e3e3-e3e3-43e3-83e3-e3e3e3e3e3e3", "e4e4e4e4-e4e4-44e4-84e4-e4e4e4e4e4e4"}[i],
			Image:     &computev1.Image{Spec: snapSpec(snapshot)},
		})
		wantCode(t, err, codes.NotFound)
		wantReason(t, err, "RESOURCE_NOT_FOUND")
	}
	listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: sharedParent})
	if err != nil || len(listed.GetImages()) != 2 {
		t.Fatalf("caller lists %v %v, want only the two copies", listed.GetImages(), err)
	}
	for _, s := range h.images.sources()[before:] {
		if s.Name != owner && s.Name != own {
			t.Fatalf("a refused snapshot reached the agent: %+v", s)
		}
		if s.SourceVolumeSnapshot == objectOf("snap", privateSnap) || s.SourceVolumeSnapshot == objectOf("snap", absentSnap) {
			t.Fatalf("a refused snapshot's object reached the agent: %+v", s)
		}
	}
	return owner, own
}

func sourcesOf(specs []hypervisor.ImageSpec, name string) []hypervisor.ImageSpec {
	var out []hypervisor.ImageSpec
	for _, s := range specs {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}
