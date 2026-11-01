package grpcapi_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// grants is a SnapshotPermissions that holds each snapshot's
// createVolumePermission as its owner set it: project ids, or "all".
type grants struct {
	mu    sync.Mutex
	perms map[string][]string
	err   error
	asked []string
}

func (g *grants) CanCreateVolume(_ context.Context, snapshot, project string) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.asked = append(g.asked, snapshot+" for "+project)
	if g.err != nil {
		return false, g.err
	}
	for _, p := range g.perms[snapshot] {
		if p == project || p == "all" {
			return true, nil
		}
	}
	return false, nil
}

func (g *grants) questions() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.asked...)
}

// TestCreateImageFromASharedSnapshot: a snapshot of another project in the
// caller's region is copied when its owner granted the caller
// createVolumePermission, directly or to "all", as EC2 lets an account
// register an image from a snapshot shared with it. An unshared snapshot,
// and one that does not exist, stay the same NOT_FOUND. A shared snapshot of
// another region is still NOT_FOUND, because a grant does not cross regions.
// A grant that cannot be read is UNAVAILABLE and stores nothing.
func TestCreateImageFromASharedSnapshot(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		g := newGrants()
		h := serveRepo(t, memstore.New().WithLegacyImageScope(demoScope), grpcapi.WithSnapshotPermissions(g))
		createImageFromASharedSnapshot(t, h, g)
	})
	t.Run("envtest", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		g := newGrants()
		open := func() store.Repository { return kubetest.Open(t, kc, ns).WithLegacyImageScope(demoScope) }
		h := serveRepo(t, open(), grpcapi.WithSnapshotPermissions(g))
		createImageFromASharedSnapshot(t, h, g)
		h.stop()
		// After a restart the accepted request id still names its operation.
		h = serveRepo(t, open(), grpcapi.WithSnapshotPermissions(g))
		createImageFromASharedSnapshot(t, h, g)
	})
}

const (
	sharedOwner  = "projects/a333333333333"
	sharedCaller = "projects/a444444444444"
	sharedParent = sharedCaller + "/regions/vws-1"
	sharedSnap   = sharedOwner + "/regions/vws-1/snapshots/shared"
	publicSnap   = sharedOwner + "/regions/vws-1/snapshots/public"
	privateSnap  = sharedOwner + "/regions/vws-1/snapshots/private"
	absentSnap   = sharedOwner + "/regions/vws-1/snapshots/absent"
	otherRegion  = sharedOwner + "/regions/vws-2/snapshots/shared"
)

func newGrants() *grants {
	return &grants{perms: map[string][]string{
		sharedSnap:  {"a444444444444"},
		publicSnap:  {"all"},
		privateSnap: {"a555555555555"},
		otherRegion: {"a444444444444"},
	}}
}

func createImageFromASharedSnapshot(t *testing.T, h *harness, g *grants) {
	t.Helper()
	ctx := context.Background()

	for _, tc := range []struct {
		name, snapshot, imageID, requestID string
	}{
		{"shared with the caller", sharedSnap, "ami-0000000000000ddd1", "a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1"},
		{"shared with all", publicSnap, "ami-0000000000000ddd2", "a2a2a2a2-a2a2-42a2-82a2-a2a2a2a2a2a2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &computev1.CreateImageRequest{
				Parent: sharedParent, ImageId: tc.imageID, RequestId: tc.requestID,
				ValidateOnly: true, Image: &computev1.Image{Spec: snapSpec(tc.snapshot)},
			}
			if op, err := h.img.CreateImage(ctx, req); err != nil || !op.GetDone() {
				t.Fatalf("validate_only: %v %v", op, err)
			}
			req.ValidateOnly = false
			op, err := h.img.CreateImage(ctx, req)
			if err != nil {
				t.Fatalf("CreateImage: %v", err)
			}
			again, err := h.img.CreateImage(ctx, req)
			if err != nil || again.GetName() != op.GetName() {
				t.Fatalf("retry = %v %v, want operation %s", again.GetName(), err, op.GetName())
			}
			done, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{
				Name: op.GetName(), Timeout: durationpb.New(10 * time.Second),
			})
			if err != nil || !done.GetDone() || done.GetError() != nil {
				t.Fatalf("operation %v %v", done, err)
			}
			img := &computev1.Image{}
			if err := done.GetResponse().UnmarshalTo(img); err != nil {
				t.Fatal(err)
			}
			// The image belongs to the caller and records the shared source.
			got, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: img.GetName()})
			if err != nil {
				t.Fatalf("caller GetImage %s: %v", img.GetName(), err)
			}
			if got.GetSpec().GetSnapshot() != tc.snapshot {
				t.Fatalf("source = %q, want %q", got.GetSpec().GetSnapshot(), tc.snapshot)
			}
			if _, err := h.img.GetImage(ctx, &computev1.GetImageRequest{
				Name: sharedOwner + "/regions/vws-1/images/" + img.GetName()[len(sharedParent+"/images/"):],
			}); status.Code(err) != codes.NotFound {
				t.Fatalf("owner GetImage of the caller's image = %v, want NOT_FOUND", err)
			}
		})
	}

	t.Run("unshared and absent snapshots are the same NOT_FOUND", func(t *testing.T) {
		for _, validateOnly := range []bool{false, true} {
			req := &computev1.CreateImageRequest{
				Parent: sharedParent, ImageId: "ami-0000000000000ddd3", RequestId: "a3a3a3a3-a3a3-43a3-83a3-a3a3a3a3a3a3",
				ValidateOnly: validateOnly, Image: &computev1.Image{Spec: snapSpec(privateSnap)},
			}
			_, private := h.img.CreateImage(ctx, req)
			wantCode(t, private, codes.NotFound)
			wantReason(t, private, "RESOURCE_NOT_FOUND")
			req.Image = &computev1.Image{Spec: snapSpec(absentSnap)}
			_, absent := h.img.CreateImage(ctx, req)
			wantCode(t, absent, codes.NotFound)
			if got, want := status.Convert(absent).Message(), "snapshot "+absentSnap+" not found"; got != want {
				t.Fatalf("absent message %q, want %q", got, want)
			}
			if got, want := status.Convert(private).Message(), "snapshot "+privateSnap+" not found"; got != want {
				t.Fatalf("unshared message %q, want %q", got, want)
			}
		}
	})

	t.Run("a grant does not cross regions", func(t *testing.T) {
		before := len(g.questions())
		_, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
			Parent: sharedParent, ImageId: "ami-0000000000000ddd4", RequestId: "a4a4a4a4-a4a4-44a4-84a4-a4a4a4a4a4a4",
			Image: &computev1.Image{Spec: snapSpec(otherRegion)},
		})
		wantCode(t, err, codes.NotFound)
		if asked := g.questions()[before:]; len(asked) != 0 {
			t.Fatalf("asked %v for a snapshot of another region", asked)
		}
	})

	t.Run("the caller's own snapshot needs no grant", func(t *testing.T) {
		before := len(g.questions())
		op, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
			Parent: sharedParent, ImageId: "ami-0000000000000ddd5", RequestId: "a5a5a5a5-a5a5-45a5-85a5-a5a5a5a5a5a5",
			ValidateOnly: true, Image: &computev1.Image{Spec: snapSpec(sharedParent + "/snapshots/own")},
		})
		if err != nil || !op.GetDone() {
			t.Fatalf("own snapshot: %v %v", op, err)
		}
		if asked := g.questions()[before:]; len(asked) != 0 {
			t.Fatalf("asked %v for the caller's own snapshot", asked)
		}
	})

	t.Run("an unreadable grant is UNAVAILABLE and stores nothing", func(t *testing.T) {
		g.mu.Lock()
		g.err = errors.New("storage down")
		g.mu.Unlock()
		defer func() {
			g.mu.Lock()
			g.err = nil
			g.mu.Unlock()
		}()
		_, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
			Parent: sharedParent, ImageId: "ami-0000000000000ddd6", RequestId: "a6a6a6a6-a6a6-46a6-86a6-a6a6a6a6a6a6",
			Image: &computev1.Image{Spec: snapSpec(sharedSnap)},
		})
		wantCode(t, err, codes.Unavailable)
		wantReason(t, err, "DEPENDENCY_UNAVAILABLE")
	})

	// Only the two shared copies are the caller's images.
	listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: sharedParent})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetImages()) != 2 {
		t.Fatalf("caller lists %d images, want the 2 shared copies: %v", len(listed.GetImages()), listed.GetImages())
	}
}

// TestForeignSnapshotIsNotFoundWithoutSnapshotPermissions: with no
// SnapshotPermissions configured, no snapshot is shared, so even one an owner
// would share is NOT_FOUND, as before createVolumePermission was read.
func TestForeignSnapshotIsNotFoundWithoutSnapshotPermissions(t *testing.T) {
	h := serve(t)
	_, err := h.img.CreateImage(context.Background(), &computev1.CreateImageRequest{
		Parent: sharedParent, ImageId: "ami-0000000000000ddd7", RequestId: "a7a7a7a7-a7a7-47a7-87a7-a7a7a7a7a7a7",
		Image: &computev1.Image{Spec: snapSpec(sharedSnap)},
	})
	wantCode(t, err, codes.NotFound)
	wantReason(t, err, "RESOURCE_NOT_FOUND")
}
