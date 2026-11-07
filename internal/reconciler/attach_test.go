package reconciler_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/reconciler"
)

// storageVolumes stands in for vws-storage behind reconciler.WithClaims, as
// grpcapi.Claims answers: a volume storage removed or is removing never gets
// a claim, which is a permanent error.
type storageVolumes struct {
	mu    sync.Mutex
	state map[string]string // "", "deleting", "missing", "down"
	// deleteOnceHeld makes a volume deleting the first time it is looked
	// up while the agent's machine lists its claim: a DeleteVolume storage
	// admitted after the attach was accepted and before the patch landed.
	deleteOnceHeld map[string]string
	held           func(claim string) bool
	calls          map[string]int
}

func (s *storageVolumes) claim(_ context.Context, volume string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[volume]++
	if c, ok := s.deleteOnceHeld[volume]; ok && s.held(c) {
		s.state[volume] = "deleting"
	}
	switch s.state[volume] {
	case "deleting":
		return "", hypervisor.Permanent(fmt.Errorf("volume %s is being deleted", volume))
	case "missing":
		return "", hypervisor.Permanent(fmt.Errorf("volume %s does not exist", volume))
	case "down":
		return "", errors.New("storage service unavailable")
	}
	return "pvc-data", nil
}

func (s *storageVolumes) set(volume, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[volume] = state
}

func (s *storageVolumes) lookups(volume string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[volume]
}

func storageHarness(h *harness) (*harness, *storageVolumes) {
	s := &storageVolumes{state: map[string]string{}, deleteOnceHeld: map[string]string{}, calls: map[string]int{}}
	s.held = func(claim string) bool {
		m, _ := h.agent.Machine("web")
		return slices.Contains(m.Volumes, hypervisor.Volume{Name: "vdb", Claim: claim})
	}
	h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo),
		reconciler.WithClaims(s.claim))
	h.ctl = controller.New(h.repo, controller.WithClock(h.clock.Now), controller.WithKick(h.rec.Kick))
	return h, s
}

var (
	bootEntry = compute.VolumeAttachment{Volume: ok(compute.ParseVolumeRef(bootVolume)), DeviceName: "vda", Boot: true}
	dataEntry = compute.VolumeAttachment{
		Volume: ok(compute.ParseVolumeRef(dataVolume)), DeviceName: "vdb", Claim: "pvc-data",
	}
)

// wantDetached checks that a refused attach left the machine and the desired
// state with the boot volume only, and that the failure names the volume.
func (h *harness) wantDetached(attach compute.Operation) {
	h.t.Helper()
	h.wantStates(compute.OperationFailed, attach)
	if m, _ := h.agent.Machine("web"); len(m.Volumes) != 0 {
		h.t.Fatalf("machine volumes = %+v, want none", m.Volumes)
	}
	h.wantVolumes("web", bootAttached)
	inst := h.instance(ok(compute.ParseInstanceID("web")))
	if f := inst.Status.Observed.AttachmentFailure; !f.Refused(dataEntry.Volume) {
		h.t.Fatalf("attachment failure = %+v, want the data volume refused", f)
	}
	if lc := inst.Lifecycle(); lc != compute.InstanceRunning {
		h.t.Fatalf("lifecycle = %d, want running", lc)
	}
}

// TestAttachOfAVolumeStorageIsRemovingFails: a volume verified when the
// attach was accepted but that storage reports deleting or missing by the
// time the agent would patch fails the attach permanently, and the machine
// never lists its claim.
func TestAttachOfAVolumeStorageIsRemovingFails(t *testing.T) {
	for _, state := range []string{"deleting", "missing"} {
		t.Run(state, func(t *testing.T) {
			eachStore(t, func(h *harness) {
				h, s := storageHarness(h)
				h.submit("create", createInstance("web"))
				h.sweep()
				s.set(dataVolume, state)
				attach := h.submit("attach", updateVolumes(bootEntry, dataEntry))
				h.sweep()
				h.wantDetached(attach)
				if n := h.agent.Creations("web"); n != 1 {
					h.t.Fatalf("machine created %d times, want 1", n)
				}
			})
		})
	}
}

// TestAttachRacingAStorageDeleteDetaches: storage admits a DeleteVolume after
// the attach passed its checks, while no VirtualMachine holds the claim, and
// commits Deleting before the agent's patch lands. The check after the patch
// sees it, so the claim leaves the machine again and the attach fails. A
// later sweep changes nothing.
func TestAttachRacingAStorageDeleteDetaches(t *testing.T) {
	eachStore(t, func(h *harness) {
		h, s := storageHarness(h)
		h.submit("create", createInstance("web"))
		h.sweep()
		s.deleteOnceHeld[dataVolume] = "pvc-data"
		attach := h.submit("attach", updateVolumes(bootEntry, dataEntry))
		h.sweep()
		h.wantDetached(attach)

		calls := h.agent.Calls()
		h.sweep()
		h.wantDetached(attach)
		if got := h.agent.Calls() - calls; got != 0 {
			h.t.Fatalf("a sweep after the refusal made %d agent calls, want 0", got)
		}
	})
}

// TestAttachWhoseCheckFailsAfterThePatchIsNotReportedAttached: storage cannot
// answer the check after the patch, nor the one before the retry, so the
// patched volume may be one it is deleting. It is not reported attached and
// the attach stays pending; the
// next converge, after a restart here, asks again and detaches it when
// storage says it is deleting.
func TestAttachWhoseCheckFailsAfterThePatchIsNotReportedAttached(t *testing.T) {
	eachStore(t, func(h *harness) {
		h, s := storageHarness(h)
		h.submit("create", createInstance("web"))
		h.sweep()
		s.held = func(claim string) bool {
			m, _ := h.agent.Machine("web")
			if slices.Contains(m.Volumes, hypervisor.Volume{Name: "vdb", Claim: claim}) {
				s.state[dataVolume] = "down"
			}
			return false
		}
		s.deleteOnceHeld[dataVolume] = "pvc-data"
		attach := h.submit("attach", updateVolumes(bootEntry, dataEntry))
		if err := h.rec.Sweep(context.Background()); err == nil {
			h.t.Fatal("sweep with storage down after the patch = nil, want the lookup error")
		}
		h.wantStates(compute.OperationPending, attach)
		h.wantVolumes("web", bootAttached)

		// Storage still down: the check before the next patch fails too,
		// and the volume the first patch added stays unreported.
		h.clock.Advance(reconciler.MaxBackoff)
		if err := h.rec.Sweep(context.Background()); err == nil {
			h.t.Fatal("sweep with storage still down = nil, want the lookup error")
		}
		h.wantStates(compute.OperationPending, attach)
		h.wantVolumes("web", bootAttached)

		delete(s.deleteOnceHeld, dataVolume)
		s.set(dataVolume, "deleting")
		h.rec = reconciler.New(h.repo, h.agent, reconciler.WithClock(h.clock.Now), reconciler.WithReports(h.repo),
			reconciler.WithClaims(s.claim))
		h.clock.Advance(reconciler.MaxBackoff)
		h.sweep()
		h.wantDetached(attach)
	})
}

// TestAttachedVolumeCostsNoStorageLookup: once a volume is reported
// attached, storage holds it against a delete itself, so neither a settled
// instance nor a later change asks storage about it again.
func TestAttachedVolumeCostsNoStorageLookup(t *testing.T) {
	eachStore(t, func(h *harness) {
		h, s := storageHarness(h)
		h.submit("create", createInstance("web"))
		h.sweep()
		attach := h.submit("attach", updateVolumes(bootEntry, dataEntry))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, attach)
		h.wantVolumes("web", bootAttached, wantVolume{dataVolume, compute.AttachAttached, t0})

		lookups := s.lookups(dataVolume)
		h.sweep()
		stop := h.submit("stop", ok(compute.NewSetPower("web", compute.PowerStopped, compute.MetadataPatch{})))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, stop)
		if got := s.lookups(dataVolume) - lookups; got != 0 {
			h.t.Fatalf("storage lookups after the volume was attached = %d, want 0", got)
		}
	})
}
