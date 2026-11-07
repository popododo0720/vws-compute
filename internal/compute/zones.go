package compute

import (
	"slices"
	"strings"
)

// Zones is the operator's zone catalog: each availability zone and the region
// it belongs to. Region membership comes from the catalog, never from the
// zone name, so a Local Zone (vws-1-lax-1a) or a Wavelength Zone
// (vws-1-wl1-bos-wlz-1) belongs to its parent region as a standard zone does.
type Zones map[string]string

// DefaultZones is the catalog of a deployment that configures none: vws-1a in
// vws-1, the one zone vws-storage admits by default.
func DefaultZones() Zones { return Zones{"vws-1a": "vws-1"} }

// ParseZones parses comma-separated zone=region pairs. Both names use the
// resource id grammar, a zone is listed once, and the list is not empty. It
// fails with CodeInvalid.
func ParseZones(s string) (Zones, error) {
	z := Zones{}
	for _, pair := range strings.Split(s, ",") {
		if pair = strings.TrimSpace(pair); pair == "" {
			continue
		}
		zone, region, ok := strings.Cut(pair, "=")
		zone, region = strings.TrimSpace(zone), strings.TrimSpace(region)
		if !ok || !validResourceID(zone) || !validResourceID(region) {
			return nil, Errorf(CodeInvalid, "zone %q is not zone=region", pair)
		}
		if _, dup := z[zone]; dup {
			return nil, Errorf(CodeInvalid, "zone %q is listed twice", zone)
		}
		z[zone] = region
	}
	if len(z) == 0 {
		return nil, Errorf(CodeInvalid, "no zone listed")
	}
	return z, nil
}

// InRegion reports whether zone is a zone of region. A zone the catalog does
// not list is in no region.
func (z Zones) InRegion(zone, region string) bool {
	r, ok := z[zone]
	return ok && r == region
}

// String is the catalog as ParseZones reads it, sorted by zone.
func (z Zones) String() string {
	pairs := make([]string, 0, len(z))
	for zone, region := range z {
		pairs = append(pairs, zone+"="+region)
	}
	slices.Sort(pairs)
	return strings.Join(pairs, ",")
}
