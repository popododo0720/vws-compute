package compute_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestZonesPlaceLocalAndWavelengthZonesInTheirParentRegion(t *testing.T) {
	zones := ok(compute.ParseZones(
		"vws-1a=vws-1, vws-1-lax-1a=vws-1, vws-1-wl1-bos-wlz-1=vws-1, vws-2a=vws-2, vws-2-lax-1a=vws-2"))
	for _, tc := range []struct {
		zone, region string
		want         bool
	}{
		{"vws-1a", "vws-1", true},
		{"vws-1a", "vws-2", false},
		{"vws-1-lax-1a", "vws-1", true},
		{"vws-1-lax-1a", "vws-2", false},
		{"vws-1-wl1-bos-wlz-1", "vws-1", true},
		{"vws-1-wl1-bos-wlz-1", "vws-2", false},
		{"vws-2-lax-1a", "vws-2", true},
		{"vws-2-lax-1a", "vws-1", false},
		{"vws-1b", "vws-1", false},
		{"vws-1-lax-1b", "vws-1", false},
		{"vws-1", "vws-1", false},
		{"", "vws-1", false},
		{"vws-1a", "", false},
		{"", "", false},
	} {
		if got := zones.InRegion(tc.zone, tc.region); got != tc.want {
			t.Errorf("InRegion(%q, %q) = %v, want %v", tc.zone, tc.region, got, tc.want)
		}
	}
	var none compute.Zones
	if none.InRegion("vws-1a", "vws-1") {
		t.Error("a nil catalog places a zone")
	}
}

func TestDefaultZonesIsVWSStoragesDefaultZone(t *testing.T) {
	d := compute.DefaultZones()
	if !d.InRegion("vws-1a", "vws-1") || len(d) != 1 {
		t.Fatalf("default zones = %v", d)
	}
	back := ok(compute.ParseZones(d.String()))
	if back.String() != "vws-1a=vws-1" {
		t.Fatalf("default zones round trip to %q", back.String())
	}
}

func TestParseZonesRefusesMalformedCatalogs(t *testing.T) {
	for _, s := range []string{
		"",
		" , ",
		"vws-1a",
		"vws-1a=",
		"=vws-1",
		"vws-1a=vws-1=x",
		"VWS-1a=vws-1",
		"vws-1a=vws_1",
		"vws-1a=vws-1,vws-1a=vws-1",
		"vws-1a=vws-1,vws-1a=vws-2",
		"a234567890123456789012345678901234567890123456789012345678901234=vws-1",
	} {
		if z, err := compute.ParseZones(s); err == nil {
			t.Errorf("ParseZones(%q) = %v, want an error", s, z)
		} else if compute.CodeOf(err) != compute.CodeInvalid {
			t.Errorf("ParseZones(%q) code = %v, want CodeInvalid", s, compute.CodeOf(err))
		}
	}
}
