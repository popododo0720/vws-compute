package reconciler_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
)

const bootVolume = "projects/p/zones/z/volumes/root"

func createInstance(id string) compute.Intent {
	return createInstanceOn(id, bootVolume)
}

func createInstanceOn(id, volume string) compute.Intent {
	return ok(compute.NewCreateInstance(compute.CreateInstanceParams{
		ID: id, Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
		Volumes: []compute.VolumeAttachment{{Volume: ok(compute.ParseVolumeRef(volume)), DeviceName: "vda", Boot: true}},
	}))
}

func drivenHarness(h *harness) *harness {
	h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo))
	h.ctl = controller.New(h.repo, controller.WithClock(h.clock.Now), controller.WithKick(h.rec.Kick))
	return h
}

func TestDrivenCreateAndDeleteFinishThroughReports(t *testing.T) {
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		create := h.submit("create", createInstance("web"))
		id := ok(compute.ParseInstanceID(create.Target.ID))
		h.wantStates(compute.OperationPending, create)

		h.sweep()
		h.wantStates(compute.OperationSucceeded, create)
		if m, found := h.agent.Machine("web"); !found || m.Power != hypervisor.PowerOn {
			h.t.Fatalf("machine = %+v, %v, want running", m, found)
		}

		stop := h.submit("stop", ok(compute.NewStopInstance(id.String())))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, stop)
		if m, _ := h.agent.Machine("web"); m.Power != hypervisor.PowerOff {
			h.t.Fatalf("power = %d, want off", m.Power)
		}

		del := h.submit("delete", ok(compute.NewTerminateInstance(id.String())))
		h.wantStates(compute.OperationPending, del)
		h.sweep()
		h.wantStates(compute.OperationSucceeded, del)
		if !h.instance(id).Removed() {
			h.t.Fatal("instance is not removed after the DELETING report")
		}
		if _, found := h.agent.Machine("web"); found {
			h.t.Fatal("machine survived the delete")
		}
	})
}

func TestDrivenPermanentErrorParksAndTransientRetries(t *testing.T) {
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		h.agent.Fail("bad", hypervisor.Permanent(errors.New("no such disk")))
		bad := h.submit("bad", createInstance("bad"))
		h.sweep()
		got := ok(h.repo.Operation(context.Background(), bad.ID))
		if got.State() != compute.OperationFailed {
			h.t.Fatalf("permanent failure left the operation %s", got.State())
		}

		h.agent.Fail("slow", errors.New("api down"))
		slow := h.submit("slow", createInstanceOn("slow", "projects/p/zones/z/volumes/other"))
		if err := h.rec.Sweep(context.Background()); err == nil {
			h.t.Fatal("a transient failure was not reported by Sweep")
		}
		h.wantStates(compute.OperationPending, slow)
		h.sweep()
		h.wantStates(compute.OperationSucceeded, slow)
	})
}

func TestDrivenInstancesWaitWithoutReports(t *testing.T) {
	eachStore(t, func(h *harness) {
		create := h.submit("create", createInstance("web"))
		h.sweep()
		h.wantStates(compute.OperationPending, create)
		if h.agent.Calls() != 0 {
			h.t.Fatal("the reconciler touched a controller-driven instance without WithReports")
		}
	})
}

func TestDeleteFromErrorSucceedsWhileBootVolumeFailed(t *testing.T) {
	const dataVolume = "projects/p/zones/z/volumes/data"
	eachStore(t, func(h *harness) {
		d := &deleter{}
		ready := false
		h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo),
			reconciler.WithVolumeDeleter(d.delete),
			reconciler.WithClaims(func(_ context.Context, _ string) (string, error) {
				if ready {
					return "", hypervisor.Permanent(errors.New(
						"volume projects/p/zones/z/volumes/root failed in storage: clone failed"))
				}
				return "pvc-root", nil
			}))
		h.ctl = newController(h)
		boot := compute.VolumeAttachment{
			Volume: ok(compute.ParseVolumeRef(bootVolume)), DeviceName: "vda", Boot: true, DeleteOnTermination: true,
		}
		data := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(dataVolume)), DeviceName: "vdb"}
		create := h.submit("create", ok(compute.NewCreateInstance(compute.CreateInstanceParams{
			ID: "web", Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
			Volumes: []compute.VolumeAttachment{boot, data},
		})))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, create)
		if _, found := h.agent.Machine("web"); !found {
			h.t.Fatal("machine was not created")
		}
		ready = true

		del := h.submit("delete", ok(compute.NewTerminateInstance("web")))
		h.sweep()
		deleteOp := ok(h.repo.Operation(context.Background(), del.ID))
		if deleteOp.State() != compute.OperationSucceeded {
			h.t.Fatalf("delete operation is %s, want %s", deleteOp.State(), compute.OperationSucceeded)
		}
		if deleteOp.Failure() != nil {
			h.t.Fatalf("delete failure = %+v, want nil", deleteOp.Failure())
		}
		id := ok(compute.ParseInstanceID("web"))
		if !h.instance(id).Removed() {
			h.t.Fatal("instance is not removed")
		}
		if _, found := h.agent.Machine("web"); found {
			h.t.Fatal("machine survived the delete")
		}

		h.sweep()
		h.sweep()
		if got := d.seen(); !slices.Equal(got, []string{bootVolume}) {
			h.t.Fatalf("deleted volumes = %v, want [%s]", got, bootVolume)
		}
		if !h.instance(id).Status.VolumesReleased {
			h.t.Fatal("volumes not released")
		}
	})
}

func TestDrivenRestartRebuildsTheAgent(t *testing.T) {
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		h.submit("create", createInstance("web"))
		h.sweep()
		fresh := fakeagent.New()
		rec := reconciler.New(h.repo, fresh, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo))
		if err := rec.Restore(context.Background()); err != nil {
			h.t.Fatal(err)
		}
	})
}

// TestDrivenVolumesReachTheAgent: attached data volumes reach the agent by
// device name and storage claim, a detach removes them, and the boot entry
// is never a data volume.
func TestDrivenVolumesReachTheAgent(t *testing.T) {
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		h.submit("create", createInstance("web"))
		h.sweep()
		boot := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(bootVolume)), DeviceName: "vda", Boot: true}
		data := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef("projects/p/zones/z/volumes/data")), DeviceName: "vdb", Claim: "pvc-data"}

		attach := h.submit("attach", ok(compute.NewUpdateInstanceSpec("web", compute.PowerRunning, []compute.VolumeAttachment{boot, data}, compute.MetadataPatch{})))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, attach)
		m, _ := h.agent.Machine("web")
		if len(m.Volumes) != 1 || m.Volumes[0] != (hypervisor.Volume{Name: "vdb", Claim: "pvc-data"}) {
			h.t.Fatalf("agent volumes after attach = %+v", m.Volumes)
		}

		detach := h.submit("detach", ok(compute.NewUpdateInstanceSpec("web", compute.PowerRunning, []compute.VolumeAttachment{boot}, compute.MetadataPatch{})))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, detach)
		if m, _ := h.agent.Machine("web"); len(m.Volumes) != 0 {
			h.t.Fatalf("agent volumes after detach = %+v", m.Volumes)
		}
	})
}
