package reconciler_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/reconciler"
)

type deleter struct {
	mu    sync.Mutex
	calls []string
	fail  int
}

func (d *deleter) delete(_ context.Context, vol string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, vol)
	if d.fail > 0 {
		d.fail--
		return errors.New("storage unavailable")
	}
	return nil
}

func (d *deleter) seen() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls)
}

// TestRemovedInstanceReleasesDeleteOnTerminationVolumes: once a driven
// instance is removed, its DeleteOnTermination volumes are deleted. A
// storage failure is retried on the next sweep, the release is recorded so a
// restarted reconciler does not ask again, a volume without the flag is
// kept, and a volume another live instance now holds is kept.
func TestRemovedInstanceReleasesDeleteOnTerminationVolumes(t *testing.T) {
	const vols = "projects/p/zones/z/volumes/"
	eachStore(t, func(h *harness) {
		d := &deleter{fail: 1}
		h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo),
			reconciler.WithVolumeDeleter(d.delete))
		h.ctl = newController(h)
		create := func(id string, v ...compute.VolumeAttachment) compute.Intent {
			return ok(compute.NewCreateInstance(compute.CreateInstanceParams{
				ID: id, Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning, Volumes: v,
			}))
		}
		att := func(vol, dev string, boot, dot bool) compute.VolumeAttachment {
			return compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(vols + vol)), DeviceName: dev, Boot: boot, DeleteOnTermination: dot}
		}
		h.submit("c1", create("web", att("root", "vda", true, true), att("data", "vdb", false, false)))
		h.submit("c2", create("db", att("db-root", "vda", true, true)))
		h.sweep()
		if got := d.seen(); len(got) != 0 {
			h.t.Fatalf("deletes while running = %v", got)
		}

		h.submit("t1", ok(compute.NewTerminateInstance("web")))
		h.sweep() // removes web
		h.sweep() // the first delete fails
		if got := d.seen(); !slices.Equal(got, []string{vols + "root"}) {
			h.t.Fatalf("deletes after terminate = %v", got)
		}
		h.sweep() // retried, succeeds, recorded
		h.sweep() // nothing more
		if got := d.seen(); !slices.Equal(got, []string{vols + "root", vols + "root"}) {
			h.t.Fatalf("deletes after retry = %v, want the root volume twice (one failure)", got)
		}
		if !h.instance(ok(compute.ParseInstanceID("web"))).Status.VolumesReleased {
			h.t.Fatal("release not recorded")
		}

		// A restart reads the record and asks nothing again.
		h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo),
			reconciler.WithVolumeDeleter(d.delete))
		h.sweep()
		if got := len(d.seen()); got != 2 {
			h.t.Fatalf("deletes after a restart = %d, want 2", got)
		}

		// db's root volume moves to a new instance before db's release runs.
		h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo))
		h.ctl = newController(h)
		h.submit("t2", ok(compute.NewTerminateInstance("db")))
		h.sweep()
		if !h.instance(ok(compute.ParseInstanceID("db"))).Removed() {
			h.t.Fatal("db is not removed")
		}
		h.submit("c3", create("db2", att("db-root", "vda", true, true)))
		h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo),
			reconciler.WithVolumeDeleter(d.delete))
		h.sweep()
		if got := len(d.seen()); got != 2 {
			h.t.Fatalf("deleted a volume a live instance holds: %v", d.seen())
		}
		if !h.instance(ok(compute.ParseInstanceID("db"))).Status.VolumesReleased {
			h.t.Fatal("db release not recorded after skipping its held volume")
		}
	})
}

func newController(h *harness) *controller.Controller {
	return controller.New(h.repo, controller.WithClock(h.clock.Now), controller.WithKick(h.rec.Kick))
}
