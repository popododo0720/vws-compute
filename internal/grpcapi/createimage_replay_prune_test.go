package grpcapi_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// lookupHook is a repository that runs hook once, right after a lookup of
// req found its operation.
type lookupHook struct {
	store.Repository
	mu   sync.Mutex
	req  compute.RequestID
	hook func()
}

func (r *lookupHook) OperationByRequest(ctx context.Context, req compute.RequestID) (compute.Operation, error) {
	op, err := r.Repository.OperationByRequest(ctx, req)
	r.mu.Lock()
	hook := r.hook
	if err != nil || req != r.req {
		hook = nil
	}
	if hook != nil {
		r.hook = nil
	}
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return op, err
}

// TestReplayRacingAPruneNeverSkipsTheGrant: one lookup decides both that a
// request id is a replay and that its grant is not read. When
// PruneOperations drops the stored operation right after that lookup, the
// retry still returns that operation, and once it is gone a retry is a new
// request that reads the grant. A revoked caller never gets a second image.
func TestReplayRacingAPruneNeverSkipsTheGrant(t *testing.T) {
	ctx := context.Background()
	repo := &lookupHook{Repository: memstore.New().WithLegacyImageScope(demoScope)}
	g := newGrants()
	h := serveRepo(t, repo, grpcapi.WithSnapshotPermissions(g))

	const requestID = "b1b1b1b1-b1b1-41b1-81b1-b1b1b1b1b1b1"
	req := &computev1.CreateImageRequest{
		Parent: sharedParent, ImageId: "ami-0000000000000eee1", RequestId: requestID,
		Image: &computev1.Image{Spec: snapSpec(sharedSnap)},
	}
	first, err := h.img.CreateImage(ctx, req)
	if err != nil {
		t.Fatalf("CreateImage: %v", err)
	}
	done, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{
		Name: first.GetName(), Timeout: durationpb.New(10 * time.Second),
	})
	if err != nil || !done.GetDone() || done.GetError() != nil {
		t.Fatalf("operation %v %v", done, err)
	}
	img := &computev1.Image{}
	if err := done.GetResponse().UnmarshalTo(img); err != nil {
		t.Fatal(err)
	}
	imageID := img.GetName()[len(sharedParent+"/images/"):]

	// Updates committed two days later push the settled create operation
	// out of the image's history on the KeepOperations-th one.
	later := controller.New(repo, controller.WithClock(func() time.Time { return time.Now().Add(48 * time.Hour) }))
	update := func(i int) {
		labels, err := compute.ParseLabels(map[string]string{"n": fmt.Sprint(i)})
		if err != nil {
			t.Fatal(err)
		}
		in, err := compute.NewUpdateImageMetadata(imageID, compute.MetadataPatch{}.ReplaceLabels(labels))
		if err != nil {
			t.Fatal(err)
		}
		r, err := compute.ParseRequestID(fmt.Sprintf("update-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := later.Submit(ctx, r, in); err != nil {
			t.Errorf("update %d: %v", i, err)
		}
	}
	for i := 1; i < store.KeepOperations; i++ {
		update(i)
	}
	restore := g.swap(map[string][]string{}, nil)
	defer restore()

	stored, err := compute.ParseRequestID(requestID)
	if err != nil {
		t.Fatal(err)
	}
	repo.mu.Lock()
	repo.req, repo.hook = stored, func() { update(store.KeepOperations) }
	repo.mu.Unlock()

	before := len(g.questions())
	again, err := h.img.CreateImage(ctx, req)
	if err != nil || again.GetName() != first.GetName() {
		t.Fatalf("replay racing the prune = %v %v, want operation %s", again.GetName(), err, first.GetName())
	}
	if asked := g.questions()[before:]; len(asked) != 0 {
		t.Fatalf("the replay asked %v", asked)
	}

	_, err = h.img.CreateImage(ctx, req)
	wantCode(t, err, codes.NotFound)
	if asked := g.questions()[before:]; len(asked) != 1 {
		t.Fatalf("a retry after the prune asked %v, want one grant read", asked)
	}

	listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: sharedParent})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetImages()) != 1 {
		t.Fatalf("caller lists %d images, want only the one granted: %v", len(listed.GetImages()), listed.GetImages())
	}
}
