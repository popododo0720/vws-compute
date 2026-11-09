package grpcapi_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestCreateImageZoneComesFromTheCatalog: CreateImage places a volume
// source's zone in a region by the -zones catalog, not by its name, so a
// Local Zone or Wavelength Zone of the caller's region is copied as a
// standard zone is. A zone of another region, and a zone the catalog does not
// list, are NOT_FOUND (RESOURCE_NOT_FOUND) with nothing stored.
func TestCreateImageZoneComesFromTheCatalog(t *testing.T) {
	zones, err := compute.ParseZones("vws-1a=vws-1,vws-1-lax-1a=vws-1,vws-1-wl1-bos-wlz-1=vws-1," +
		"vws-2a=vws-2,vws-2-lax-1a=vws-2,vws-2-wl1-nrt-wlz-1=vws-2")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("memstore", func(t *testing.T) {
		createImageZoneComesFromTheCatalog(t, serveRepo(t, memstore.New(), grpcapi.WithZones(zones)))
	})
	t.Run("envtest", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		createImageZoneComesFromTheCatalog(t, serveRepo(t, kubetest.Open(t, kc, ns), grpcapi.WithZones(zones)))
	})
}

func createImageZoneComesFromTheCatalog(t *testing.T, h *harness) {
	t.Helper()
	ctx := context.Background()
	const caller = "projects/a222222222222/regions/vws-1"
	req := func(zone, requestID string, validateOnly bool) *computev1.CreateImageRequest {
		return &computev1.CreateImageRequest{
			Parent: caller, ImageId: "ami-0000000000000ddd4", RequestId: requestID, ValidateOnly: validateOnly,
			Image: &computev1.Image{Spec: volSpec("projects/a222222222222/zones/" + zone + "/volumes/root")},
		}
	}
	for _, tc := range []struct{ zone, requestID string }{
		{"vws-1a", "a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1"},
		{"vws-1-lax-1a", "a2a2a2a2-a2a2-42a2-82a2-a2a2a2a2a2a2"},
		{"vws-1-wl1-bos-wlz-1", "a3a3a3a3-a3a3-43a3-83a3-a3a3a3a3a3a3"},
	} {
		if op, err := h.img.CreateImage(ctx, req(tc.zone, tc.requestID, true)); err != nil || !op.GetDone() {
			t.Fatalf("own volume in %s: %v %v", tc.zone, op, err)
		}
	}
	for _, tc := range []struct{ zone, requestID string }{
		{"vws-2a", "b1b1b1b1-b1b1-41b1-81b1-b1b1b1b1b1b1"},
		{"vws-2-lax-1a", "b2b2b2b2-b2b2-42b2-82b2-b2b2b2b2b2b2"},
		{"vws-2-wl1-nrt-wlz-1", "b3b3b3b3-b3b3-43b3-83b3-b3b3b3b3b3b3"},
		{"vws-1b", "b4b4b4b4-b4b4-44b4-84b4-b4b4b4b4b4b4"},
		{"vws-1-lax-1b", "b5b5b5b5-b5b5-45b5-85b5-b5b5b5b5b5b5"},
	} {
		for _, validateOnly := range []bool{true, false, false} {
			_, err := h.img.CreateImage(ctx, req(tc.zone, tc.requestID, validateOnly))
			wantCode(t, err, codes.NotFound)
			wantReason(t, err, "RESOURCE_NOT_FOUND")
		}
	}
	listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: caller})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetImages()) != 0 {
		t.Fatalf("caller lists %v", listed.GetImages())
	}
}
