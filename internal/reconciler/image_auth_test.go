package reconciler_test

import (
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/reconciler"
)

// TestImageAuthIgnoresImageState pins vws-compute#93: ImageAuth depends on
// the record's scope, its deregistration and the public list only. Every
// lifecycle state (pending, Available, parked) and every source, including a
// capture whose machine is gone, gets the same answer.
func TestImageAuthIgnoresImageState(t *testing.T) {
	id := ok(compute.ParseImageID("ami-0aaaaaaaaaaaaaaa1"))
	scope := ok(compute.NewImageScope("a123456789012", "vws-1"))
	pub := ok(compute.NewPublicImages([]string{id.String()}))
	owner := hypervisor.ImageAuth{Project: "a123456789012", Region: "vws-1"}
	public := hypervisor.ImageAuth{Project: "a123456789012", Region: "vws-1", Public: true}
	parked := compute.Observed{Generation: 1, Failure: &compute.Failure{Code: compute.CodeInternal, Message: "gave up"}}
	states := map[string]compute.ImageStatus{
		"pending":   {},
		"available": {Observed: compute.Observed{Generation: 1}, Present: true},
		"parked":    {Observed: parked},
	}
	sources := map[string]compute.ImageSource{
		"volume":   compute.FromVolume{},
		"instance": compute.FromInstance{Instance: ok(compute.ParseInstanceID("i-0123456789abcdef0"))},
	}
	for sn, st := range states {
		for srcn, src := range sources {
			for _, tc := range []struct {
				name         string
				scope        compute.ImageScope
				deregistered bool
				public       compute.PublicImages
				want         hypervisor.ImageAuth
			}{
				{"private", scope, false, nil, owner},
				{"public", scope, false, pub, public},
				{"deregistered public", scope, true, pub, hypervisor.ImageAuth{}},
				{"no project", compute.ImageScope{}, false, pub, hypervisor.ImageAuth{}},
			} {
				img := compute.Image{Spec: compute.ImageSpec{ID: id, Scope: tc.scope, Generation: 1, Source: src, Deregistered: tc.deregistered}, Status: st}
				if got := reconciler.ImageAuth(&img, tc.public); got != tc.want {
					t.Errorf("%s/%s/%s: got %+v, want %+v", sn, srcn, tc.name, got, tc.want)
				}
			}
		}
	}
	if got := reconciler.ImageAuth(nil, pub); got != (hypervisor.ImageAuth{}) {
		t.Errorf("no record: got %+v, want none", got)
	}
}
