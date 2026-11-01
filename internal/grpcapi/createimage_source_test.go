package grpcapi_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// TestCreateImageOfAnotherProjectsSourceIsNotFound: a CreateImage whose
// volume or snapshot sits in another project, a volume in a zone outside the
// caller's region, or a snapshot in another region, fails as NOT_FOUND, as
// AWS answers InvalidVolume.NotFound and InvalidSnapshot.NotFound for a
// resource the caller cannot see. The answer
// does not depend on whether the foreign source exists, validate_only
// answers the same, a retry answers the same, and nothing is stored.
func TestCreateImageOfAnotherProjectsSourceIsNotFound(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		createImageOfAnotherProjectsSourceIsNotFound(t, serve(t))
	})
	t.Run("envtest", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		h := serveRepo(t, kubetest.Open(t, kc, ns).WithLegacyImageScope(demoScope))
		createImageOfAnotherProjectsSourceIsNotFound(t, h)
		h.stop()
		// After a restart the same request id still finds nothing to replay.
		h = serveRepo(t, kubetest.Open(t, kc, ns).WithLegacyImageScope(demoScope))
		createImageOfAnotherProjectsSourceIsNotFound(t, h)
	})
}

func createImageOfAnotherProjectsSourceIsNotFound(t *testing.T, h *harness) {
	t.Helper()
	ctx := context.Background()
	const (
		owner  = "projects/a111111111111"
		caller = "projects/a222222222222/regions/vws-1"
	)
	// The owner's snapshot exists: the owner can copy it.
	ownSnap := owner + "/regions/vws-1/snapshots/held"
	ownReq := createImageReq(owner+"/regions/vws-1", "ami-0000000000000aaa1", "10101010-1010-4010-8010-101010101010")
	ownReq.GetImage().GetSpec().Source = &computev1.ImageSpec_Snapshot{Snapshot: ownSnap}
	if _, err := h.img.CreateImage(ctx, ownReq); err != nil {
		t.Fatalf("owner CreateImage: %v", err)
	}

	for _, tc := range []struct {
		name          string
		source, other *computev1.ImageSpec
		wantNamed     string
		requestID     string
	}{
		{
			name:      "snapshot of another project",
			source:    snapSpec(ownSnap),
			other:     snapSpec(owner + "/regions/vws-1/snapshots/absent"),
			wantNamed: "snapshot",
			requestID: "20202020-2020-4020-8020-202020202020",
		},
		{
			name:      "snapshot in another region",
			source:    snapSpec("projects/a222222222222/regions/vws-2/snapshots/held"),
			other:     snapSpec("projects/a222222222222/regions/vws-2/snapshots/absent"),
			wantNamed: "snapshot",
			requestID: "30303030-3030-4030-8030-303030303030",
		},
		{
			name:      "volume of another project",
			source:    volSpec(owner + "/zones/vws-1a/volumes/held"),
			other:     volSpec(owner + "/zones/vws-1a/volumes/absent"),
			wantNamed: "volume",
			requestID: "40404040-4040-4040-8040-404040404040",
		},
		{
			name:      "volume in a zone of another region",
			source:    volSpec("projects/a222222222222/zones/vws-2a/volumes/held"),
			other:     volSpec("projects/a222222222222/zones/vws-2a/volumes/absent"),
			wantNamed: "volume",
			requestID: "50505050-5050-4050-8050-505050505050",
		},
		{
			name:      "volume in a zone that only starts with the region",
			source:    volSpec("projects/a222222222222/zones/vws-10a/volumes/held"),
			other:     volSpec("projects/a222222222222/zones/vws-10a/volumes/absent"),
			wantNamed: "volume",
			requestID: "60606060-6060-4060-8060-606060606060",
		},
		{
			name:      "volume named by the region, not a zone",
			source:    volSpec("projects/a222222222222/zones/vws-1/volumes/held"),
			other:     volSpec("projects/a222222222222/zones/vws-1/volumes/absent"),
			wantNamed: "volume",
			requestID: "70707070-7070-4070-8070-707070707070",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, validateOnly := range []bool{false, true} {
				req := &computev1.CreateImageRequest{
					Parent: caller, ImageId: "ami-0000000000000bbb2", RequestId: tc.requestID,
					ValidateOnly: validateOnly, Image: &computev1.Image{Spec: tc.source},
				}
				_, err := h.img.CreateImage(ctx, req)
				wantCode(t, err, codes.NotFound)
				wantReason(t, err, "RESOURCE_NOT_FOUND")
				msg := status.Convert(err).Message()
				if !strings.HasPrefix(msg, tc.wantNamed+" ") {
					t.Fatalf("message %q does not name the %s", msg, tc.wantNamed)
				}
				// A retry with the same request id answers the same.
				_, again := h.img.CreateImage(ctx, req)
				if status.Code(again) != codes.NotFound || status.Convert(again).Message() != msg {
					t.Fatalf("retry = %v, first %v", again, err)
				}
				// A foreign source nobody holds gets the same answer.
				req.Image = &computev1.Image{Spec: tc.other}
				_, absent := h.img.CreateImage(ctx, req)
				if status.Code(absent) != codes.NotFound ||
					strings.ReplaceAll(status.Convert(absent).Message(), "/absent", "/held") != msg {
					t.Fatalf("absent source %v, held source %v", absent, err)
				}
			}
		})
	}
	// The caller's own volume in a zone of its region passes the scope check.
	own := &computev1.CreateImageRequest{
		Parent: caller, ImageId: "ami-0000000000000ccc3", RequestId: "80808080-8080-4080-8080-808080808080",
		ValidateOnly: true, Image: &computev1.Image{Spec: volSpec("projects/a222222222222/zones/vws-1a/volumes/root")},
	}
	if op, err := h.img.CreateImage(ctx, own); err != nil || !op.GetDone() {
		t.Fatalf("own volume in vws-1a: %v %v", op, err)
	}
	listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: caller})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetImages()) != 0 {
		t.Fatalf("caller lists %v", listed.GetImages())
	}
}

func snapSpec(name string) *computev1.ImageSpec {
	return &computev1.ImageSpec{Source: &computev1.ImageSpec_Snapshot{Snapshot: name}}
}

func volSpec(name string) *computev1.ImageSpec {
	return &computev1.ImageSpec{Source: &computev1.ImageSpec_Volume{Volume: name}}
}
