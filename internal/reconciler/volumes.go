package reconciler

import (
	"context"
	"log/slog"
	"slices"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// attempt is what one converge of a controller-driven instance did, as far
// as its volumes care.
type attempt struct {
	// converged is ConvergeMachine returning nil: the agent applied the
	// whole spec, which for a data volume only means the machine lists it.
	converged bool
	// retrying is a transient agent failure other than not-ready: the node
	// tried to apply the spec, failed, and tries again on the next sweep.
	retrying bool
	// refused are the volumes a permanent attachment error named. They are
	// left out: their attach operation failed and the planner drops them.
	refused []compute.VolumeRef
}

// observeVolumes is the attach state of every volume of spec, plus the
// volumes prev still shows that the machine has not let go of yet. EC2
// reports a volume attached only once the attachment is complete, so a data
// volume is attached only when the agent says its hot-plug finished
// (hypervisor.VolumePlugged), not when the spec reached the agent.
//
//	boot volume          attached once the machine converged, attaching before
//	data volume plugged  attached
//	data volume else     attaching, or busy while the node is retrying
//	left the spec        detaching (busy while retrying) until the machine drops it
//
// An agent that is not a hypervisor.VolumeObserver has no hot-plug phase:
// its nil error means the volume is applied, so a converged volume is
// attached and one that left the spec is gone. A failed phase lookup is
// logged and keeps prev, so a lookup error never reports a volume attached.
// A state that did not change keeps its time from prev.
func (r *Reconciler) observeVolumes(
	ctx context.Context, spec compute.InstanceSpec, prev []compute.ObservedVolume, at attempt,
) []compute.ObservedVolume {
	var phases map[string]hypervisor.VolumePhase
	obs, observed := r.agent.(hypervisor.VolumeObserver)
	if observed {
		var err error
		phases, err = obs.MachineVolumes(ctx, spec.ID.String())
		if err != nil {
			r.log.WarnContext(ctx, "machine volumes", slog.String("instance", spec.ID.String()), slog.String("err", err.Error()))
			return slices.Clone(prev)
		}
	}
	was := map[compute.VolumeRef]compute.ObservedVolume{}
	for _, v := range prev {
		was[v.Volume] = v
	}
	now := r.now()
	var out []compute.ObservedVolume
	add := func(v compute.VolumeRef, device string, state compute.AttachState) {
		t := now
		if p, ok := was[v]; ok && p.State == state && p.DeviceName == device {
			t = p.StateTime
		}
		out = append(out, compute.ObservedVolume{Volume: v, DeviceName: device, State: state, StateTime: t})
	}
	pending := compute.AttachAttaching
	if at.retrying {
		pending = compute.AttachBusy
	}
	inSpec := map[compute.VolumeRef]bool{}
	disks := map[string]bool{}
	for _, v := range spec.Volumes {
		inSpec[v.Volume] = true
		disks[diskName(v.DeviceName)] = true
		switch {
		case v.Boot:
			state := compute.AttachAttaching
			if at.converged || was[v.Volume].State == compute.AttachAttached {
				state = compute.AttachAttached
			}
			add(v.Volume, v.DeviceName, state)
		case slices.Contains(at.refused, v.Volume):
		case observed && phases[diskName(v.DeviceName)] == hypervisor.VolumePlugged,
			!observed && at.converged:
			add(v.Volume, v.DeviceName, compute.AttachAttached)
		default:
			add(v.Volume, v.DeviceName, pending)
		}
	}
	for _, p := range prev {
		if inSpec[p.Volume] {
			continue
		}
		disk := diskName(p.DeviceName)
		var onMachine bool
		if observed {
			// A spec volume that took over the disk name owns the phase.
			_, onMachine = phases[disk]
			onMachine = onMachine && !disks[disk]
		} else {
			onMachine = !at.converged
		}
		if !onMachine {
			continue
		}
		state := compute.AttachDetaching
		if at.retrying {
			state = compute.AttachBusy
		}
		add(p.Volume, p.DeviceName, state)
	}
	return out
}

// volumesSettled reports whether vols says every volume of spec but the
// refused one is attached and nothing else is left: a later observation
// cannot change it until the spec changes.
func volumesSettled(spec compute.InstanceSpec, vols []compute.ObservedVolume, refused []compute.VolumeRef) bool {
	attached := map[compute.VolumeRef]bool{}
	for _, v := range vols {
		if v.State != compute.AttachAttached {
			return false
		}
		attached[v.Volume] = true
	}
	want := 0
	for _, v := range spec.Volumes {
		if !v.Boot && slices.Contains(refused, v.Volume) {
			continue
		}
		if !attached[v.Volume] {
			return false
		}
		want++
	}
	return len(vols) == want
}

func sameVolumes(a, b []compute.ObservedVolume) bool {
	return slices.EqualFunc(a, b, func(x, y compute.ObservedVolume) bool {
		return x.Volume == y.Volume && x.DeviceName == y.DeviceName && x.State == y.State && x.StateTime.Equal(y.StateTime)
	})
}
