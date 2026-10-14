package reconciler

import (
	"context"
	"fmt"
	"log/slog"

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
		if !inst.Spec.ControllerDriven() || inst.Removed() {
			return nil
		}
		if inst.Status.Observed.Generation >= inst.Spec.Generation {
			return r.refreshAddress(ctx, inst)
		}
		return r.convergeDriven(ctx, inst)
	})
}

func (r *Reconciler) convergeDriven(ctx context.Context, inst compute.Instance) error {
	spec := inst.Spec
	machine, agentErr := r.machine(ctx, spec)
	if agentErr == nil {
		agentErr = r.agent.ConvergeMachine(ctx, machine)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	rep := compute.InstanceReport{Generation: spec.Generation, Power: spec.Power}
	switch {
	case agentErr == nil && spec.Power == compute.PowerTerminated:
		rep.State = compute.ReportDeleting
	case agentErr == nil:
		rep.State = compute.ReportReady
		rep.Interfaces = r.interfaces(ctx, spec)
	case hypervisor.IsPermanent(agentErr):
		rep.State, rep.Message, rep.Code = compute.ReportError, agentErr.Error(), compute.CodeFailedPrecondition
	default:
		// Reconciling records the failure and leaves the generation open,
		// so the next sweep tries again.
		rep.State, rep.Message = compute.ReportReconciling, agentErr.Error()
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

// interfaces is the instance's observed interface: its first NIC with the
// address the agent sees on the machine. KubeVirt's own name for that
// interface is not the device name, so the spec's device name is used. It
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
	nic := spec.NICs[0]
	return []compute.ObservedInterface{{DeviceName: nic.DeviceName, Subnet: nic.Subnet, PrivateIPv4: ip}}
}

// refreshAddress reports a READY running instance again once its machine
// has an address that the last report lacked: KubeVirt often reports the
// guest Ready before the interface address shows. It reads the agent only
// while the address is missing, so a settled instance costs nothing.
func (r *Reconciler) refreshAddress(ctx context.Context, inst compute.Instance) error {
	st := inst.Status
	if st.Report.State != compute.ReportReady || st.Observed.Failure != nil || inst.Spec.Power != compute.PowerRunning ||
		(len(st.Report.Interfaces) > 0 && st.Report.Interfaces[0].PrivateIPv4 != "") {
		return nil
	}
	ifaces := r.interfaces(ctx, inst.Spec)
	if len(ifaces) == 0 {
		return nil
	}
	rep := compute.InstanceReport{
		Node: st.Report.Node, Generation: inst.Spec.Generation, State: compute.ReportReady,
		Message: st.Report.Message, Power: st.Report.Power, Interfaces: ifaces,
	}
	_, err := r.reports.ReportInstanceStatus(ctx, inst.Spec.ID, rep, r.now())
	if err != nil && compute.CodeOf(err) != compute.CodeAborted {
		return fmt.Errorf("report %s address: %w", inst.Spec.ID, err)
	}
	return nil
}
