package compute_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestSourceRefScope(t *testing.T) {
	vol := ok(compute.ParseVolumeRef("projects/a111111111111/zones/vws-1a/volumes/vol-1"))
	if got := vol.Project(); got != "a111111111111" {
		t.Errorf("volume project = %q", got)
	}
	if got := vol.Zone(); got != "vws-1a" {
		t.Errorf("volume zone = %q", got)
	}
	snap := ok(compute.ParseSnapshotRef("projects/a222222222222/regions/vws-1/snapshots/snap-1"))
	if got := snap.Project(); got != "a222222222222" {
		t.Errorf("snapshot project = %q", got)
	}
	if got := snap.Region(); got != "vws-1" {
		t.Errorf("snapshot region = %q", got)
	}
	var zeroVol compute.VolumeRef
	var zeroSnap compute.SnapshotRef
	if zeroVol.Project() != "" || zeroVol.Zone() != "" || zeroSnap.Project() != "" || zeroSnap.Region() != "" {
		t.Error("a zero ref has a scope")
	}
}

func TestZoneInRegion(t *testing.T) {
	for _, tc := range []struct {
		zone, region string
		want         bool
	}{
		{"vws-1a", "vws-1", true},
		{"vws-1z", "vws-1", true},
		{"vws-2a", "vws-1", false},
		{"vws-10a", "vws-1", false},
		{"vws-1", "vws-1", false},
		{"vws-1ab", "vws-1", false},
		{"vws-11", "vws-1", false},
		{"vws-1-", "vws-1", false},
		{"vws-1A", "vws-1", false},
		{"a", "", false},
		{"", "", false},
		{"", "vws-1", false},
	} {
		if got := compute.ZoneInRegion(tc.zone, tc.region); got != tc.want {
			t.Errorf("ZoneInRegion(%q, %q) = %v, want %v", tc.zone, tc.region, got, tc.want)
		}
	}
}
