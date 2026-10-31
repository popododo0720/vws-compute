package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/store"
)

// WithReports makes the reconciler converge controller-driven instances
// itself and write the result through the report path, the same path an
// external controller uses. Without it those instances wait for an
// external controller.
func WithReports(s store.ReportStore) Option { return func(r *Reconciler) { r.reports = s } }

// sweepDriven converges every controller-driven instance whose spec
// generation is ahead of the observed one. Unconverged does not list them,
// so this pages the instances. A report is the only write, so a lost report
// is converged again and the agent makes that a no-op.
func (r *Reconciler) sweepDriven(ctx context.Context) error {
	if r.reports == nil {
		return nil
	}
	return eachPage(ctx, func(after string) (store.Page[compute.Instance], error) {
		return r.store.Instances(ctx, store.InstanceQuery{PageSize: store.MaxPageSize, After: after})
	}, func(inst compute.Instance) error {
		if inst.Spec.ControllerDriven() && inst.Removed() {
			return r.releaseVolumes(ctx, inst)
		}
		if !inst.Spec.ControllerDriven() || inst.Removed() {
			return nil
		}
		if inst.Status.Observed.Generation >= inst.Spec.Generation {
			return r.refresh(ctx, inst)
		}
		return r.convergeDriven(ctx, inst)
	})
}

func (r *Reconciler) convergeDriven(ctx context.Context, inst compute.Instance) error {
	spec := inst.Spec
	machine, agentErr := r.machine(ctx, spec)
	reached := agentErr == nil
	if reached {
		agentErr = r.agent.ConvergeMachine(ctx, machine)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	prevVolumes := inst.Status.Report.Volumes
	rep := compute.InstanceReport{Generation: spec.Generation, Power: spec.Power}
	switch {
	case agentErr == nil && spec.Power == compute.PowerTerminated:
		rep.State = compute.ReportDeleting
	case agentErr == nil:
		rep.State = compute.ReportReady
		rep.Interfaces = r.interfaces(ctx, spec)
		rep.Volumes = r.observeVolumes(ctx, spec, prevVolumes, attempt{converged: true})
	case hypervisor.IsPermanent(agentErr) && hypervisor.CauseOf(agentErr) == hypervisor.CauseAttachment:
		// The machine is healthy; only the attachment failed. As on EC2,
		// the instance keeps its state and the attachment operation fails.
		rep.State = compute.ReportReady
		rep.Node = inst.Status.Report.Node
		rep.Interfaces = inst.Status.Report.Interfaces
		rep.AttachmentFailure = attachmentFailure(agentErr, spec)
		rep.Volumes = r.observeVolumes(ctx, spec, prevVolumes, attempt{refused: rep.AttachmentFailure.Volume})
	case hypervisor.IsPermanent(agentErr):
		f := parkedFailure(agentErr)
		rep.State, rep.Message, rep.Code, rep.Reason = compute.ReportError, f.Message, f.Code, f.Reason
		rep.Volumes = prevVolumes
	default:
		// Reconciling records the failure and leaves the generation open,
		// so the next sweep tries again.
		rep.State, rep.Message = compute.ReportReconciling, agentErr.Error()
		if spec.Power != compute.PowerTerminated {
			rep.Volumes = r.observeVolumes(ctx, spec, prevVolumes,
				attempt{retrying: reached && !hypervisor.IsNotReady(agentErr)})
		}
	}
	_, err := r.reports.ReportInstanceStatus(ctx, spec.ID, rep, r.now())
	if err != nil && compute.CodeOf(err) == compute.CodeAborted {
		// The spec moved on while the agent ran. The next sweep reads it.
		return nil
	}
	if err != nil {
		return fmt.Errorf("report %s: %w", spec.ID, err)
	}
	if agentErr != nil && !hypervisor.IsPermanent(agentErr) {
		return fmt.Errorf("converge %s: %w", spec.ID, agentErr)
	}
	return nil
}

// attachmentFailure names the volume the agent refused, so the next plan can
// drop it from desired state instead of letting it block every later change.
func attachmentFailure(err error, spec compute.InstanceSpec) *compute.AttachmentFailure {
	f := parkedFailure(err)
	refused := &compute.AttachmentFailure{Failure: *f}
	volume, ok := hypervisor.AttachmentVolume(err)
	if !ok {
		return refused
	}
	for _, v := range spec.Volumes {
		if !v.Boot && diskName(v.DeviceName) == volume.Name {
			refused.Volume = v.Volume
			break
		}
	}
	return refused
}

// VolumeDeleter asks the storage service to delete a volume. It must be
// idempotent: a volume already gone or already being deleted is success.
type VolumeDeleter func(ctx context.Context, volume string) error

// WithVolumeDeleter makes the reconciler delete, once a controller-driven
// instance is removed, each of its volumes marked DeleteOnTermination.
// Terminate does not wait for it: the instance is already gone when this
// runs. Without it volumes are always kept.
func WithVolumeDeleter(d VolumeDeleter) Option { return func(r *Reconciler) { r.deleteVolume = d } }

// releaseVolumes deletes a removed instance's DeleteOnTermination volumes
// and then records that it did, so a restart or a later sweep does not ask
// again. A volume a live instance now holds is skipped: the store lets
// another instance take a terminated instance's volume. A failure is logged
// and retried on the next sweep; it never blocks other work.
func (r *Reconciler) releaseVolumes(ctx context.Context, inst compute.Instance) error {
	if r.deleteVolume == nil || inst.Status.VolumesReleased {
		return nil
	}
	var doomed []compute.VolumeAttachment
	for _, v := range inst.Spec.Volumes {
		if v.DeleteOnTermination {
			doomed = append(doomed, v)
		}
	}
	if len(doomed) > 0 {
		held, err := r.heldVolumes(ctx)
		if err != nil {
			return err
		}
		for _, v := range doomed {
			if holder, ok := held[v.Volume.String()]; ok {
				r.log.InfoContext(ctx, "keep volume on termination: another instance holds it",
					slog.String("instance", inst.Spec.ID.String()), slog.String("volume", v.Volume.String()), slog.String("holder", holder))
				continue
			}
			if err := r.deleteVolume(ctx, v.Volume.String()); err != nil {
				r.log.WarnContext(ctx, "delete volume on termination",
					slog.String("instance", inst.Spec.ID.String()), slog.String("volume", v.Volume.String()), slog.Any("err", err))
				return nil
			}
		}
	}
	if err := r.reports.ReleaseVolumes(ctx, inst.Spec.ID); err != nil {
		return fmt.Errorf("record released volumes of %s: %w", inst.Spec.ID, err)
	}
	return nil
}

// heldVolumes maps each volume a live instance lists to that instance.
func (r *Reconciler) heldVolumes(ctx context.Context) (map[string]string, error) {
	held := map[string]string{}
	err := eachPage(ctx, func(after string) (store.Page[compute.Instance], error) {
		return r.store.Instances(ctx, store.InstanceQuery{PageSize: store.MaxPageSize, After: after})
	}, func(inst compute.Instance) error {
		if inst.Spec.Power == compute.PowerTerminated {
			return nil
		}
		for _, v := range inst.Spec.Volumes {
			held[v.Volume.String()] = inst.Spec.ID.String()
		}
		return nil
	})
	return held, err
}

// interfaces is the instance's observed interface: its primary NIC (see
// primaryNIC) with the address the agent sees on the machine. KubeVirt's own
// name for that interface is not always the device name, so the spec's
// device name is used. It
// is nil when the agent cannot say, the instance has no NIC, or the machine
// has no address yet; a lookup error is logged and treated the same way.
func (r *Reconciler) interfaces(ctx context.Context, spec compute.InstanceSpec) []compute.ObservedInterface {
	obs, ok := r.agent.(hypervisor.AddressObserver)
	if !ok || len(spec.NICs) == 0 || spec.Power != compute.PowerRunning {
		return nil
	}
	ip, err := obs.MachineIPv4(ctx, spec.ID.String())
	if err != nil {
		r.log.WarnContext(ctx, "machine address", slog.String("instance", spec.ID.String()), slog.Any("err", err))
		return nil
	}
	if ip == "" {
		return nil
	}
	nic := primaryNIC(spec.NICs)
	return []compute.ObservedInterface{{DeviceName: nic.DeviceName, Subnet: nic.Subnet, PrivateIPv4: ip}}
}

// refresh reports a READY instance again when a later look at its machine
// says more than the last report did. KubeVirt often reports the guest Ready
// before the interface address shows, and a data volume's hot-plug finishes
// after the spec is applied, so the volume reads attached only on a later
// sweep. It reads the agent only while the address is missing or a volume is
// not yet settled, so a settled instance costs nothing, and it writes only
// when the report changes.
func (r *Reconciler) refresh(ctx context.Context, inst compute.Instance) error {
	st := inst.Status
	if st.Report.State != compute.ReportReady || st.Observed.Failure != nil || inst.Spec.Power == compute.PowerTerminated {
		return nil
	}
	var refused compute.VolumeRef
	if f := st.Observed.AttachmentFailure; f != nil {
		refused = f.Volume
	}
	needAddress := inst.Spec.Power == compute.PowerRunning &&
		(len(st.Report.Interfaces) == 0 || st.Report.Interfaces[0].PrivateIPv4 == "")
	needVolumes := !volumesSettled(inst.Spec, st.Report.Volumes, refused)
	if !needAddress && !needVolumes {
		return nil
	}
	ifaces, vols := st.Report.Interfaces, st.Report.Volumes
	if needAddress {
		if got := r.interfaces(ctx, inst.Spec); len(got) > 0 {
			ifaces = got
		}
	}
	if needVolumes {
		vols = r.observeVolumes(ctx, inst.Spec, st.Report.Volumes, attempt{converged: true, refused: refused})
	}
	if slices.Equal(ifaces, st.Report.Interfaces) && sameVolumes(vols, st.Report.Volumes) {
		return nil
	}
	rep := compute.InstanceReport{
		Node: st.Report.Node, Generation: inst.Spec.Generation, State: compute.ReportReady,
		Message: st.Report.Message, Power: st.Report.Power, Interfaces: ifaces, Volumes: vols,
		// Keep the refusal: this report is at the same generation, and the
		// planner drops the refused volume only while the generations match.
		AttachmentFailure: st.Observed.AttachmentFailure,
	}
	_, err := r.reports.ReportInstanceStatus(ctx, inst.Spec.ID, rep, r.now())
	if err != nil && compute.CodeOf(err) != compute.CodeAborted {
		return fmt.Errorf("report %s: %w", inst.Spec.ID, err)
	}
	return nil
}
