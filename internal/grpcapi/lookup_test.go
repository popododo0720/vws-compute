package grpcapi_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const (
	lookupInstanceReq = "c1c1c1c1-c1c1-4c1c-8c1c-c1c1c1c1c1c1"
	lookupImageReq    = "c2c2c2c2-c2c2-4c2c-8c2c-c2c2c2c2c2c2"
	lookupKeyReq      = "c3c3c3c3-c3c3-4c3c-8c3c-c3c3c3c3c3c3"
	lookupUnknownReq  = "c4c4c4c4-c4c4-4c4c-8c4c-c4c4c4c4c4c4"
)

// testClock is a clock a test moves forward while the server runs.
type testClock struct{ offset atomic.Int64 }

func (c *testClock) now() time.Time          { return time.Now().Add(time.Duration(c.offset.Load())) }
func (c *testClock) advance(d time.Duration) { c.offset.Store(int64(d)) }
func (c *testClock) option() grpcapi.Option  { return grpcapi.WithClock(c.now) }

// TestLookupOperation pins popododo0720/vws-compute#140 (vws-proto#57 AC2):
// LookupOperation answers a request id with the operation GetOperation
// returns, only for the project that owns the operation's resource, and
// only within the 24-hour request-id window. A foreign, expired, or unknown
// request id is the same NOT_FOUND. The kubestore run restarts the server
// before the lookups, so the answer comes from the repository alone.
func TestLookupOperation(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		repo := memstore.New().WithLegacyImageScope(demoScope)
		lookupOperation(t, func() store.Repository { return repo }, false)
	})
	t.Run("kubestore", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		lookupOperation(t, func() store.Repository {
			return kubetest.Open(t, kc, ns).WithLegacyImageScope(demoScope)
		}, true)
	})
}

func lookupOperation(t *testing.T, open func() store.Repository, reopen bool) {
	ctx := context.Background()
	clock := &testClock{}
	vols := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{volBase + "root-1": {ClaimName: "pvc-root-1"}}}
	opts := []grpcapi.Option{grpcapi.WithVolumes(vols), clock.option()}
	h := serveRepo(t, open(), opts...)

	instOp, err := h.inst.CreateInstance(ctx, createReq("web-1", lookupInstanceReq, "root-1"))
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	imgOp, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
		Parent: "projects/demo/regions/demo", ImageId: "debian-12", RequestId: lookupImageReq,
		Image: &computev1.Image{Spec: &computev1.ImageSpec{
			Source: &computev1.ImageSpec_Snapshot{Snapshot: "projects/demo/regions/demo/snapshots/snap"},
		}},
	})
	if err != nil {
		t.Fatalf("CreateImage: %v", err)
	}
	line, _ := ed25519Line(t)
	keyOp, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: "projects/demo/regions/r1", KeyPairId: "deploy", RequestId: lookupKeyReq,
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
	})
	if err != nil {
		t.Fatalf("CreateKeyPair: %v", err)
	}
	if reopen {
		h.stop()
		h = serveRepo(t, open(), opts...)
	}

	lookup := func(parent, req string) (*longrunningpb.Operation, error) {
		resp, err := h.lookup.LookupOperation(ctx, &computev1.LookupOperationRequest{Parent: parent, RequestId: req})
		return resp.GetOperation(), err
	}
	cases := []struct {
		kind, req string
		created   *longrunningpb.Operation
	}{
		{"instance", lookupInstanceReq, instOp},
		{"image", lookupImageReq, imgOp},
		{"key pair", lookupKeyReq, keyOp},
	}

	t.Run("owner finds the operation GetOperation returns", func(t *testing.T) {
		for _, c := range cases {
			got, err := lookup("projects/demo", c.req)
			if err != nil {
				t.Fatalf("%s: LookupOperation: %v", c.kind, err)
			}
			if got.GetName() != c.created.GetName() {
				t.Errorf("%s: name = %q, want %q", c.kind, got.GetName(), c.created.GetName())
			}
			md := &commonv1.OperationMetadata{}
			if err := got.GetMetadata().UnmarshalTo(md); err != nil {
				t.Fatalf("%s: metadata: %v", c.kind, err)
			}
			if md.GetRequestId() != c.req {
				t.Errorf("%s: metadata request_id = %q, want %q", c.kind, md.GetRequestId(), c.req)
			}
			polled, err := h.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: got.GetName()})
			if err != nil {
				t.Fatalf("%s: GetOperation: %v", c.kind, err)
			}
			if !proto.Equal(got, polled) {
				t.Errorf("%s: lookup = %v, want GetOperation's %v", c.kind, got, polled)
			}
			again, err := lookup("projects/demo", c.req)
			if err != nil || !proto.Equal(again, got) {
				t.Errorf("%s: repeated lookup = %v, %v; want the same operation", c.kind, again, err)
			}
		}
	})

	_, unknownErr := lookup("projects/other", lookupUnknownReq)
	wantCode(t, unknownErr, codes.NotFound)
	unknown := status.Convert(unknownErr)
	t.Run("another project's request id is NOT_FOUND", func(t *testing.T) {
		for _, c := range cases {
			_, err := lookup("projects/other", c.req)
			wantCode(t, err, codes.NotFound)
			wantReason(t, err, "RESOURCE_NOT_FOUND")
			// The answer is the unknown-id answer with only the id swapped,
			// so nothing tells the caller that the id was used elsewhere.
			want := strings.ReplaceAll(unknown.Message(), lookupUnknownReq, c.req)
			if got := status.Convert(err).Message(); got != want {
				t.Errorf("%s: message = %q, want %q", c.kind, got, want)
			}
			if opID := c.created.GetName()[strings.LastIndex(c.created.GetName(), "/")+1:]; strings.Contains(err.Error(), opID) {
				t.Errorf("%s: error %v leaks the foreign operation %s", c.kind, err, opID)
			}
		}
	})

	t.Run("unknown request id is NOT_FOUND", func(t *testing.T) {
		_, err := lookup("projects/demo", lookupUnknownReq)
		wantCode(t, err, codes.NotFound)
		wantReason(t, err, "RESOURCE_NOT_FOUND")
	})

	t.Run("hostile input is INVALID_SPEC", func(t *testing.T) {
		for _, in := range []struct{ parent, req string }{
			{"projects/demo", ""},
			{"projects/demo", "not-a-uuid"},
			{"projects/demo", lookupInstanceReq + "0"},
			{"projects/demo", "c1c1c1c1-c1c1-1c1c-8c1c-c1c1c1c1c1c1"},
			{"", lookupInstanceReq},
			{"projects/", lookupInstanceReq},
			{"projects/Demo", lookupInstanceReq},
			{"projects/demo/", lookupInstanceReq},
			{"projects/demo/zones/z1", lookupInstanceReq},
			{"folders/demo", lookupInstanceReq},
		} {
			_, err := lookup(in.parent, in.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("lookup(%q, %q) = %v, want INVALID_ARGUMENT", in.parent, in.req, err)
				continue
			}
			wantReason(t, err, "INVALID_SPEC")
		}
	})

	t.Run("the request-id window ends after 24 hours", func(t *testing.T) {
		clock.advance(grpcapi.RequestIDWindow - time.Minute)
		for _, c := range cases {
			if _, err := lookup("projects/demo", c.req); err != nil {
				t.Fatalf("%s: lookup inside the window: %v", c.kind, err)
			}
		}
		clock.advance(grpcapi.RequestIDWindow + time.Second)
		for _, c := range cases {
			_, err := lookup("projects/demo", c.req)
			wantCode(t, err, codes.NotFound)
			wantReason(t, err, "RESOURCE_NOT_FOUND")
			// The operation itself is still stored and polled by name.
			if _, err := h.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: c.created.GetName()}); err != nil {
				t.Errorf("%s: GetOperation after the window: %v", c.kind, err)
			}
		}
	})
}
