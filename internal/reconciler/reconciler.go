// Package reconciler converges observed state toward desired state. It is
// the only package that calls a hypervisor.Agent or the Commit*Status
// methods of store.StatusStore.
//
// It is level-triggered and keeps no state of its own: each sweep asks the
// store which resources are unconverged and due, hands each one's full spec
// to the agent, and commits the verdict. A restart therefore needs only the
// repository. An agent success whose status commit is lost is simply
// converged again, which the agent contract makes a no-op.
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/store"
)

const (
	// DefaultInterval is the time between periodic sweeps.
	DefaultInterval = time.Second
	// MaxBackoff caps the delay before retrying a transient failure.
	MaxBackoff = 30 * time.Second
	// GiveUpAfter is how long after a spec generation was committed the
	// reconciler keeps retrying transient failures before parking it.
	GiveUpAfter = 15 * time.Minute
)

type Reconciler struct {
	store    store.StatusStore
	agent    hypervisor.Agent
	now      func() time.Time
	interval time.Duration
	log      *slog.Logger
	kick     chan struct{}
}

type Option func(*Reconciler)

func WithClock(now func() time.Time) Option { return func(r *Reconciler) { r.now = now } }
func WithInterval(d time.Duration) Option   { return func(r *Reconciler) { r.interval = d } }
func WithLogger(l *slog.Logger) Option      { return func(r *Reconciler) { r.log = l } }

func New(s store.StatusStore, a hypervisor.Agent, opts ...Option) *Reconciler {
	r := &Reconciler{
		store:    s,
		agent:    a,
		now:      time.Now,
		interval: DefaultInterval,
		log:      slog.Default(),
		kick:     make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Kick asks a running Run loop to sweep now instead of at the next tick. It
// never blocks, and kicks coalesce. It is an optimization: without it work
// is found by the periodic sweep.
func (r *Reconciler) Kick() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Run sweeps until ctx ends. Call it from exactly one goroutine.
func (r *Reconciler) Run(ctx context.Context) error {
	tick := time.NewTicker(r.interval)
	defer tick.Stop()
	for {
		if err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("reconcile sweep", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		case <-r.kick:
		}
	}
}

// Sweep converges every resource that is unconverged and due, one at a time.
// A failure on one resource does not stop the others.
func (r *Reconciler) Sweep(ctx context.Context) error {
	refs, err := r.store.Unconverged(ctx, r.now())
	if err != nil {
		return err
	}
	var errs []error
	for _, ref := range refs {
		if err := r.converge(ctx, ref); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, fmt.Errorf("%s: %w", ref, err))
		}
	}
	return errors.Join(errs...)
}

func (r *Reconciler) converge(ctx context.Context, ref compute.Ref) error {
	var err error
	switch ref.Kind {
	case compute.KindInstance:
		err = r.convergeInstance(ctx, ref)
	case compute.KindImage:
		err = r.convergeImage(ctx, ref)
	default:
		return compute.Errorf(compute.CodeInternal, "cannot converge %s", ref)
	}
	if errors.Is(err, store.ErrStale) {
		return nil
	}
	return err
}

func (r *Reconciler) convergeInstance(ctx context.Context, ref compute.Ref) error {
	id, err := compute.ParseInstanceID(ref.ID)
	if err != nil {
		return err
	}
	inst, err := r.store.Instance(ctx, id)
	if err != nil {
		return err
	}
	spec := inst.Spec
	if inst.Status.Observed.Generation >= spec.Generation {
		return nil
	}
	machine := machineSpec(spec)
	agentErr := r.agent.ConvergeMachine(ctx, machine)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	now := r.now()
	st := inst.Status
	st.Observed, st.Retry = verdict(st.Observed, st.Retry, spec.Generation, spec.ChangedAt, agentErr, now)
	if agentErr == nil {
		st.Machine, st.RebootEpoch = machineState(machine.Power), spec.RebootEpoch
	}
	st.UpdatedAt = now
	return r.store.CommitInstanceStatus(ctx, id, st)
}

func (r *Reconciler) convergeImage(ctx context.Context, ref compute.Ref) error {
	id, err := compute.ParseImageID(ref.ID)
	if err != nil {
		return err
	}
	img, err := r.store.Image(ctx, id)
	if err != nil {
		return err
	}
	spec := img.Spec
	if img.Status.Observed.Generation >= spec.Generation {
		return nil
	}
	want := imageSpec(spec)
	size, agentErr := r.agent.ConvergeImage(ctx, want)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	now := r.now()
	st := img.Status
	st.Observed, st.Retry = verdict(st.Observed, st.Retry, spec.Generation, spec.ChangedAt, agentErr, now)
	if agentErr == nil {
		st.Present, st.SizeBytes = want.Present, size
	}
	st.UpdatedAt = now
	return r.store.CommitImageStatus(ctx, id, st)
}

// verdict turns one agent result for spec generation gen into the next
// observation and retry state:
//
//	success              observed gen, retry cleared
//	permanent error      observed gen parked with FailedPrecondition
//	transient, deadline  observed gen parked with Internal
//	transient            observation unchanged, retry backed off
func verdict(prev compute.Observed, retry compute.Retry, gen int64, changedAt time.Time, err error, now time.Time) (compute.Observed, compute.Retry) {
	switch {
	case err == nil:
		return compute.Observed{Generation: gen}, compute.Retry{}
	case hypervisor.IsPermanent(err):
		return compute.Observed{Generation: gen, Failure: &compute.Failure{Code: compute.CodeFailedPrecondition, Message: err.Error()}}, compute.Retry{}
	case now.Sub(changedAt) >= GiveUpAfter:
		msg := fmt.Sprintf("gave up after %s of transient failures: %v", GiveUpAfter, err)
		return compute.Observed{Generation: gen, Failure: &compute.Failure{Code: compute.CodeInternal, Message: msg}}, compute.Retry{}
	}
	attempts := retry.Attempts + 1
	return prev, compute.Retry{Attempts: attempts, At: now.Add(backoff(attempts)), LastError: err.Error()}
}

// backoff is min(MaxBackoff, 2^min(attempts, 5) seconds).
func backoff(attempts int) time.Duration {
	d := time.Duration(1<<min(attempts, 5)) * time.Second
	return min(d, MaxBackoff)
}

func machineSpec(s compute.InstanceSpec) hypervisor.MachineSpec {
	m := hypervisor.MachineSpec{
		Name:        s.ID.String(),
		Image:       s.Image.String(),
		VCPUs:       s.Type.VCPUs(),
		MemoryMiB:   s.Type.MemoryMiB(),
		RebootEpoch: s.RebootEpoch,
	}
	if s.Key != nil {
		m.AuthorizedKeys = []string{s.Key.PublicKey.AuthorizedKey()}
	}
	switch s.Power {
	case compute.PowerRunning:
		m.Power = hypervisor.PowerOn
	case compute.PowerStopped:
		m.Power = hypervisor.PowerOff
	case compute.PowerTerminated:
		m.Power = hypervisor.PowerAbsent
	}
	return m
}

func machineState(p hypervisor.Power) compute.MachineState {
	switch p {
	case hypervisor.PowerOn:
		return compute.MachineOn
	case hypervisor.PowerOff:
		return compute.MachineOff
	}
	return compute.MachineAbsent
}

func imageSpec(s compute.ImageSpec) hypervisor.ImageSpec {
	i := hypervisor.ImageSpec{Name: s.ID.String(), Present: !s.Deregistered}
	switch src := s.Source.(type) {
	case compute.FromURL:
		i.SourceURL = src.URL.String()
	case compute.FromInstance:
		i.SourceMachine = src.Instance.String()
	}
	return i
}
