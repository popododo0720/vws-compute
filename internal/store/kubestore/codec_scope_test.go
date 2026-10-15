package kubestore

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestImageScopeRoundTripsAndLegacyRecordsTakeTheDefault pins vws-compute#92:
// an image keeps its project and region, and a record written before scopes
// existed reads as the configured legacy scope (zero when none is set).
func TestImageScopeRoundTripsAndLegacyRecordsTakeTheDefault(t *testing.T) {
	id, err := compute.ParseImageID("ami-0123456789abcdef0")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := compute.ParseSnapshotRef("projects/p/regions/r/snapshots/s")
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := compute.NewImageScope("a111111111111", "vws-1")
	legacy, _ := compute.NewImageScope("a123456789012", "vws-1")
	j, err := imageTo(compute.Image{Spec: compute.ImageSpec{ID: id, Scope: owner, Source: compute.FromSnapshot{Ref: ref}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := imageFrom(j, legacy)
	if err != nil || got.Spec.Scope != owner {
		t.Fatalf("round trip scope %v, %v; want %v", got.Spec.Scope, err, owner)
	}
	j.Project, j.Region = "", ""
	if got, err = imageFrom(j, legacy); err != nil || got.Spec.Scope != legacy {
		t.Fatalf("legacy record scope %v, %v; want %v", got.Spec.Scope, err, legacy)
	}
	if got, err = imageFrom(j, compute.ImageScope{}); err != nil || !got.Spec.Scope.IsZero() {
		t.Fatalf("legacy record without a default: %v, %v", got.Spec.Scope, err)
	}
}
