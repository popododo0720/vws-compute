package reconciler_test

import (
	"errors"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

const dataVolume = "projects/p/zones/z/volumes/data"

func updateVolumes(vols ...compute.VolumeAttachment) compute.Intent {
	return ok(compute.NewUpdateInstanceSpec("web", compute.PowerRunning, vols, compute.MetadataPatch{}))
}

var bootAttached = wantVolume{bootVolume, compute.AttachAttached, t0}

type wantVolume struct {
	volume string
	state  compute.AttachState
	at     time.Time
}

func (h *harness) wantVolumes(id string, want ...wantVolume) {
	h.t.Helper()
	got := h.instance(ok(compute.ParseInstanceID(id))).Status.Report.Volumes
	if len(got) != len(want) {
		h.t.Fatalf("volumes = %+v, want %+v", got, want)
	}
	for i, w := range want {
		g := got[i]
		if g.Volume.String() != w.volume || g.State != w.state || !g.StateTime.Equal(w.at) {
			h.t.Fatalf("volume %d = %+v, want %+v", i, g, w)
		}
	}
}

// TestVolumeIsAttachedOnlyAfterHotplugCompletes is vws-proto#56 AC2. An
// attach converges as soon as the machine lists the volume, which is before
// the hot-plug finishes, so the report says attaching until the agent says
// the volume is plugged, and attached from the sweep that sees it. A detach
// reads detaching until the machine lets go, then the entry is gone. The boot
// volume is attached once the machine converged. A settled instance is not
// reported again.
func TestVolumeIsAttachedOnlyAfterHotplugCompletes(t *testing.T) {
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		h.submit("create", createInstance("web"))
		h.sweep()
		h.wantVolumes("web", bootAttached)

		boot := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(bootVolume)), DeviceName: "vda", Boot: true}
		data := compute.VolumeAttachment{
			Volume: ok(compute.ParseVolumeRef(dataVolume)), DeviceName: "/dev/sdf", Claim: "pvc-data",
		}
		h.agent.SetVolumePhase("web", "sdf", hypervisor.VolumePlugging)
		h.clock.Advance(time.Minute)
		t1 := h.clock.Now()
		attach := h.submit("attach", updateVolumes(boot, data))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, attach)
		h.wantVolumes("web", bootAttached, wantVolume{dataVolume, compute.AttachAttaching, t1})

		// Still plugging: the next sweep keeps attaching and its time.
		h.clock.Advance(time.Minute)
		h.sweep()
		h.wantVolumes("web", bootAttached, wantVolume{dataVolume, compute.AttachAttaching, t1})

		h.agent.ClearVolumePhase("web", "sdf")
		h.clock.Advance(time.Minute)
		t3 := h.clock.Now()
		h.sweep()
		h.wantVolumes("web", bootAttached, wantVolume{dataVolume, compute.AttachAttached, t3})

		// Settled: a later sweep writes nothing.
		before := h.instance(ok(compute.ParseInstanceID("web"))).Status.UpdatedAt
		h.clock.Advance(time.Minute)
		h.sweep()
		if after := h.instance(ok(compute.ParseInstanceID("web"))).Status.UpdatedAt; !after.Equal(before) {
			h.t.Fatalf("a settled instance was reported again at %v", after)
		}

		h.agent.SetVolumePhase("web", "sdf", hypervisor.VolumeUnplugging)
		h.clock.Advance(time.Minute)
		t5 := h.clock.Now()
		detach := h.submit("detach", updateVolumes(boot))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, detach)
		h.wantVolumes("web", bootAttached, wantVolume{dataVolume, compute.AttachDetaching, t5})

		h.agent.ClearVolumePhase("web", "sdf")
		h.sweep()
		h.wantVolumes("web", bootAttached)
	})
}

// TestVolumeIsBusyWhileTheNodeRetries: a transient agent failure while a
// volume's hot-plug is pending reads busy, as EC2 does while an attachment
// is stuck, and attached once the retry plugs it. Not-ready (the guest still
// booting) is not a failure, so the volumes read attaching.
func TestVolumeIsBusyWhileTheNodeRetries(t *testing.T) {
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		h.agent.Fail("web", hypervisor.NotReady(errors.New("instance phase is \"Scheduling\"")))
		h.submit("create", createInstance("web"))
		if err := h.rec.Sweep(t.Context()); err == nil {
			h.t.Fatal("sweep of a booting machine succeeded")
		}
		h.wantVolumes("web", wantVolume{bootVolume, compute.AttachAttaching, t0})
		h.sweep()
		h.wantVolumes("web", bootAttached)

		boot := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(bootVolume)), DeviceName: "vda", Boot: true}
		data := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(dataVolume)), DeviceName: "vdb", Claim: "pvc-data"}
		h.agent.Fail("web", errors.New("patch VirtualMachine: connection refused"))
		h.clock.Advance(time.Minute)
		t1 := h.clock.Now()
		attach := h.submit("attach", updateVolumes(boot, data))
		if err := h.rec.Sweep(t.Context()); err == nil {
			h.t.Fatal("sweep with a failing agent succeeded")
		}
		h.wantStates(compute.OperationPending, attach)
		h.wantVolumes("web", bootAttached, wantVolume{dataVolume, compute.AttachBusy, t1})

		h.clock.Advance(time.Minute)
		t2 := h.clock.Now()
		h.sweep()
		h.wantStates(compute.OperationSucceeded, attach)
		h.wantVolumes("web", bootAttached, wantVolume{dataVolume, compute.AttachAttached, t2})
	})
}

// TestRefusedVolumeIsNotReported: a volume the agent permanently refused has
// no attach state; its attach operation failed.
func TestRefusedVolumeIsNotReported(t *testing.T) {
	eachStore(t, func(h *harness) {
		drivenHarness(h)
		h.submit("create", createInstance("web"))
		h.sweep()
		boot := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(bootVolume)), DeviceName: "vda", Boot: true}
		data := compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(dataVolume)), DeviceName: "vdb", Claim: "pvc-data"}
		refused := hypervisor.Volume{Name: "vdb", Claim: "pvc-data"}
		h.agent.Fail("web", hypervisor.Attachment(refused, errors.New("claim is not Block")))
		attach := h.submit("attach", updateVolumes(boot, data))
		h.sweep()
		h.wantStates(compute.OperationFailed, attach)
		h.wantVolumes("web", bootAttached)
		h.sweep()
		h.wantVolumes("web", bootAttached)
	})
}
