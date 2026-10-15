package compute_test

import (
	"fmt"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// TestLegacyFingerprintMatchesThePreScopeRelease pins the review of
// vws-compute#93: LegacyFingerprintOf reproduces the CreateImage fingerprint
// release 07b591c persisted (golden value computed there), whatever scope
// the intent now carries, while FingerprintOf includes the scope.
func TestLegacyFingerprintMatchesThePreScopeRelease(t *testing.T) {
	const golden = "c7384983edda3c187c03bc5b3b899b09d344dd7d3407a980469c9dc12393478e"
	snap := ok(compute.ParseSnapshotRef("projects/a123456789012/regions/vws-1/snapshots/s"))
	for _, sc := range []compute.ImageScope{ok(compute.NewImageScope("a123456789012", "vws-1")), ok(compute.NewImageScope("a999999999999", "vws-2"))} {
		in := ok(compute.NewCopyImage("ami-0000000000000aaa1", sc, compute.FromSnapshot{Ref: snap}, "debian-13", "d", compute.Labels{}))
		fp, scope, isCopy := compute.LegacyFingerprintOf(in)
		if !isCopy || fmt.Sprintf("%x", fp) != golden || scope != sc {
			t.Fatalf("legacy %x %v %v, want %s", fp, scope, isCopy, golden)
		}
		if fmt.Sprintf("%x", compute.FingerprintOf(in)) == golden {
			t.Fatal("the current fingerprint must include the scope")
		}
	}
	if _, _, isCopy := compute.LegacyFingerprintOf(ok(compute.NewStopInstance("i-0123456789abcdef0"))); isCopy {
		t.Fatal("only CreateImage has a legacy fingerprint")
	}
}
