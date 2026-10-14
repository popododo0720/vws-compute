package reconciler

import (
	"context"
	"fmt"

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
		if !inst.Spec.ControllerDriven() || inst.Removed() || inst.Status.Observed.Generation >= inst.Spec.Generation {
			return nil
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
