package reconciler

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// attachOutcome is what convergeAttaching did.
type attachOutcome struct {
	// reached is the agent being asked to converge at least once.
	reached bool
	// err is the agent result, with every volume refused along the way
	// joined in as an attachment error.
	err error
	// unchecked is a storage lookup that failed: a volume an earlier or
	// this converge patched may be one storage is deleting, so nothing new
	// is reported attached until a later converge checks it.
	unchecked bool
}

// convergeAttaching converges a controller-driven machine and closes the race
// between an attach and a storage DeleteVolume. Storage admits a delete only
// while no VirtualMachine spec holds the claim, and re-reads the attachments
// after it commits Deleting. The agent patches the VirtualMachine after the
// attach was accepted, so this side writes, then checks: each data volume not
// yet reported attached is looked up in storage before the patch, and again
// after it. One storage reports missing, deleting, or failed is left out of
// the machine and refused as an attachment, so the attach fails and the
// VirtualMachine keeps no claim of it, as EC2 leaves no attachment behind.
// Either storage's re-read sees the patch and waits for the detach, or the
// check here sees Deleting and detaches.
func (r *Reconciler) convergeAttaching(
	ctx context.Context, spec compute.InstanceSpec, prev []compute.ObservedVolume,
) attachOutcome {
	fresh := unattached(spec, prev)
	var refused refusals
	if err := r.refuseGone(ctx, fresh, &refused); err != nil {
		return attachOutcome{err: err, unchecked: true}
	}
	var out attachOutcome
	for {
		before := len(refused.volumes)
		machine, agentErr := r.machine(ctx, withoutRefused(spec, refused.volumes))
		if agentErr != nil {
			out.err = agentErr
			return out
		}
		out.reached = true
		agentErr = r.agent.ConvergeMachine(ctx, machine)
		out.err = nil
		if isAttachment(agentErr) {
			refused.add(attachmentFailure(agentErr, spec).Volumes, agentErr)
		} else {
			out.err = agentErr
		}
		if err := r.refuseGone(ctx, fresh, &refused); err != nil {
			out.unchecked = true
			if out.err == nil {
				out.err = err
			}
			return out
		}
		if len(refused.volumes) == before {
			break
		}
	}
	if out.err == nil {
		out.err = errors.Join(refused.errs...)
	}
	return out
}

func isAttachment(err error) bool {
	return hypervisor.IsPermanent(err) && hypervisor.CauseOf(err) == hypervisor.CauseAttachment
}

// refusals are the data volumes left out of the machine and the attachment
// errors that name them.
type refusals struct {
	volumes []compute.VolumeRef
	errs    []error
}

// add records err and those of vols not refused yet. An err that names no
// new volume is dropped, so a repeated refusal is reported once.
func (f *refusals) add(vols []compute.VolumeRef, err error) {
	added := false
	for _, v := range vols {
		if !slices.Contains(f.volumes, v) {
			f.volumes = append(f.volumes, v)
			added = true
		}
	}
	if added {
		f.errs = append(f.errs, err)
	}
}

// unattached are the data volumes of spec that prev does not report
// attached. A volume reported attached passed the storage check after its
// patch, so storage now sees it held and refuses to delete it. A terminated
// machine attaches nothing.
func unattached(spec compute.InstanceSpec, prev []compute.ObservedVolume) []compute.VolumeAttachment {
	if spec.Power == compute.PowerTerminated {
		return nil
	}
	attached := map[compute.VolumeRef]bool{}
	for _, v := range prev {
		if v.State == compute.AttachAttached {
			attached[v.Volume] = true
		}
	}
	var out []compute.VolumeAttachment
	for _, v := range spec.Volumes {
		if !v.Boot && !attached[v.Volume] {
			out = append(out, v)
		}
	}
	return out
}

// refuseGone looks up through WithClaims each of vols not yet refused and
// refuses each one that will never have a claim: storage reports it
// missing, deleting, or failed. Without WithClaims there is nothing to ask.
// A lookup storage could not answer is returned as a transient error.
func (r *Reconciler) refuseGone(ctx context.Context, vols []compute.VolumeAttachment, refused *refusals) error {
	if r.claims == nil {
		return nil
	}
	for _, v := range vols {
		if slices.Contains(refused.volumes, v.Volume) {
			continue
		}
		_, err := r.claims(ctx, v.Volume.String())
		switch {
		case err == nil:
		case hypervisor.IsPermanent(err):
			disk := hypervisor.Volume{Name: diskName(v.DeviceName), Claim: v.Claim}
			refused.add([]compute.VolumeRef{v.Volume}, hypervisor.Attachment(disk, err))
		default:
			return fmt.Errorf("volume %s: %w", v.Volume, err)
		}
	}
	return nil
}

// withoutRefused is spec without the refused data volumes.
func withoutRefused(spec compute.InstanceSpec, refused []compute.VolumeRef) compute.InstanceSpec {
	if len(refused) == 0 {
		return spec
	}
	var vols []compute.VolumeAttachment
	for _, v := range spec.Volumes {
		if v.Boot || !slices.Contains(refused, v.Volume) {
			vols = append(vols, v)
		}
	}
	spec.Volumes = vols
	return spec
}
