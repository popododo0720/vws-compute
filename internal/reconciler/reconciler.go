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
	"strings"
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
	public   compute.PublicImages
	store    store.StatusStore
	reports  store.ReportStore
	agent    hypervisor.Agent
	now      func() time.Time
	interval time.Duration
	log      *slog.Logger
	observe  func(result string)
	claims   ClaimFunc
	// deleteVolume is set by WithVolumeDeleter.
	deleteVolume VolumeDeleter
	kick         chan struct{}
}

type Option func(*Reconciler)

// WithPublicImages marks the listed images public in ImageAuth.
func WithPublicImages(p compute.PublicImages) Option { return func(r *Reconciler) { r.public = p } }

func WithClock(now func() time.Time) Option { return func(r *Reconciler) { r.now = now } }
func WithInterval(d time.Duration) Option   { return func(r *Reconciler) { r.interval = d } }
func WithLogger(l *slog.Logger) Option      { return func(r *Reconciler) { r.log = l } }

// ClaimFunc returns the claim storage reports for a volume. "" with a nil
// error means the volume is still provisioning. A hypervisor.Permanent error
// means it never will have one.
type ClaimFunc func(ctx context.Context, volume string) (string, error)

// WithClaims resolves, on every converge, the claim of each volume the
// instance was accepted without (storage was still provisioning it). Until
// it resolves the instance stays unconverged and is retried. Without it an
// empty claim reaches the agent as it is.
func WithClaims(f ClaimFunc) Option { return func(r *Reconciler) { r.claims = f } }

// Sweep results passed to the WithSweepObserver function.
const (
	SweepOK      = "ok"
	SweepWaiting = "waiting"
	SweepError   = "error"
)

// WithSweepObserver makes Run call observe after each sweep with SweepOK,
// SweepWaiting (only machines still booting), or SweepError. It must not
// block.
func WithSweepObserver(observe func(result string)) Option {
	return func(r *Reconciler) { r.observe = observe }
}

func New(s store.StatusStore, a hypervisor.Agent, opts ...Option) *Reconciler {
	r := &Reconciler{
		store:    s,
		agent:    a,
		now:      time.Now,
		interval: DefaultInterval,
		log:      slog.Default(),
		kick:     make(chan struct{}, 1),
		observe:  func(string) {},
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
// The first thing it does is re-apply every converged spec, so an agent
// that lost its memory (the in-memory fake, after a restart) catches up
// to the repository before new work is accepted.
func (r *Reconciler) Run(ctx context.Context) error {
	if err := r.Restore(ctx); err != nil && ctx.Err() == nil {
		r.log.WarnContext(ctx, "restore agent", slog.Any("err", err))
	}
	tick := time.NewTicker(r.interval)
	defer tick.Stop()
	for {
		err := r.Sweep(ctx)
		switch {
		case ctx.Err() != nil:
		case err == nil:
			r.observe(SweepOK)
		case onlyNotReady(err):
			// A machine that is only still booting fails every sweep until it
			// is up. That is progress, not a failure, so it logs at debug.
			r.log.DebugContext(ctx, "reconcile sweep: waiting for machines", slog.Any("err", err))
			r.observe(SweepWaiting)
		default:
			r.log.WarnContext(ctx, "reconcile sweep", slog.Any("err", err))
			r.observe(SweepError)
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
//
// Order is load-bearing. A capture runs before the source instance is
// destroyed, and a launch runs before its image is deregistered. One global
// kind order cannot do both.
func (r *Reconciler) Sweep(ctx context.Context) error {
	err := r.sweep(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Every sweep authorizes image artifacts, after the converge pass so a
	// DataSource created in it is authorized at once, and even when that
	// pass returned early: no failure elsewhere may keep a stale grant.
	aerr := r.authorizeImages(ctx)
	if aerr == nil {
		return err // unwrapped, so onlyNotReady still sees its errors
	}
	return errors.Join(err, aerr)
}

func (r *Reconciler) sweep(ctx context.Context) error {
	due, err := r.store.Unconverged(ctx, r.now())
	if err != nil {
		return err
	}
	lagging, err := r.store.Lagging(ctx)
	if err != nil {
		return err
	}
	held, err := r.load(ctx, lagging)
	if err != nil {
		return err
	}
	var errs []error
	if err := r.sweepDriven(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		errs = append(errs, err)
	}
	step := func(ref compute.Ref) error {
		if err := r.converge(ctx, ref); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, fmt.Errorf("%s: %w", ref, err))
			return nil
		}
		held.drop(ref)
		return nil
	}
	for _, ref := range due {
		if !held.creating(ref) {
			continue
		}
		if err := step(ref); err != nil {
			return err
		}
	}
	for _, ref := range due {
		if !held.launching(ref) {
			continue
		}
		if err := step(ref); err != nil {
			return err
		}
	}
	for _, ref := range due {
		if !held.terminating(ref) || held.captureNeeds(ref.ID) {
			continue
		}
		if err := step(ref); err != nil {
			return err
		}
	}
	for _, ref := range due {
		if !held.removing(ref) || held.launchNeeds(ref.ID) {
			continue
		}
		if err := step(ref); err != nil {
			return err
		}
	}
	return errors.Join(errs...)
}

// ImageAuth is the only source of an image artifact's clone authorization:
// it reads the store record and the public list and nothing else. No record
// (nil) or a deregistered one lets nobody clone; an image of no project
// belongs to nobody. Image state (pending, parked, Available, source gone)
// plays no part, so no state can keep a stale grant.
func ImageAuth(img *compute.Image, public compute.PublicImages) hypervisor.ImageAuth {
	if img == nil || img.Spec.Deregistered || img.Spec.Scope.IsZero() {
		return hypervisor.ImageAuth{}
	}
	return hypervisor.ImageAuth{Project: img.Spec.Scope.Project(), Region: img.Spec.Scope.Region(), Public: public.Has(img.Spec.ID)}
}

// authorizeImages applies ImageAuth to every image artifact the agent
// manages, including ones the store has no record of, on every sweep.
func (r *Reconciler) authorizeImages(ctx context.Context) error {
	az, ok := r.agent.(hypervisor.ImageAuthorizer)
	if !ok {
		return nil
	}
	imgs := map[string]compute.Image{}
	if err := r.eachImage(ctx, func(img compute.Image) error {
		imgs[img.Spec.ID.String()] = img
		return nil
	}); err != nil {
		return err
	}
	return az.AuthorizeImages(ctx, func(name string) hypervisor.ImageAuth {
		img, ok := imgs[name]
		if !ok {
			return ImageAuth(nil, r.public)
		}
		return ImageAuth(&img, r.public)
	})
}

// held is the lagging resources a sweep must not destroy out from under.
type held struct {
	inst map[string]compute.Instance
	img  map[string]compute.Image
}

func (r *Reconciler) load(ctx context.Context, refs []compute.Ref) (held, error) {
	h := held{inst: map[string]compute.Instance{}, img: map[string]compute.Image{}}
	for _, ref := range refs {
		switch ref.Kind {
		case compute.KindInstance:
			id, err := compute.ParseInstanceID(ref.ID)
			if err != nil {
				return held{}, err
			}
			inst, err := r.store.Instance(ctx, id)
			if err != nil {
				return held{}, err
			}
			h.inst[ref.ID] = inst
		case compute.KindImage:
			id, err := compute.ParseImageID(ref.ID)
			if err != nil {
				return held{}, err
			}
			img, err := r.store.Image(ctx, id)
			if err != nil {
				return held{}, err
			}
			h.img[ref.ID] = img
		default:
			return held{}, compute.Errorf(compute.CodeInternal, "cannot converge %s", ref)
		}
	}
	return h, nil
}

func (h held) drop(ref compute.Ref) {
	delete(h.inst, ref.ID)
	delete(h.img, ref.ID)
}

func (h held) creating(ref compute.Ref) bool {
	img, ok := h.img[ref.ID]
	return ok && !img.Spec.Deregistered
}

func (h held) launching(ref compute.Ref) bool {
	inst, ok := h.inst[ref.ID]
	return ok && inst.Spec.Power != compute.PowerTerminated
}

func (h held) terminating(ref compute.Ref) bool {
	inst, ok := h.inst[ref.ID]
	return ok && inst.Spec.Power == compute.PowerTerminated
}

func (h held) removing(ref compute.Ref) bool {
	img, ok := h.img[ref.ID]
	return ok && img.Spec.Deregistered
}

// captureNeeds reports whether a lagging capture still reads id's disk.
// An instance that was never observed has no disk to capture, so it does
// not hold its own terminate.
func (h held) captureNeeds(id string) bool {
	inst, ok := h.inst[id]
	if !ok || inst.Status.Observed.Generation == 0 {
		return false
	}
	for _, img := range h.img {
		if img.Spec.Deregistered {
			continue
		}
		src, ok := img.Spec.Source.(compute.FromInstance)
		if ok && src.Instance.String() == id {
			return true
		}
	}
	return false
}

// launchNeeds reports whether a not-yet-created instance still needs image id.
func (h held) launchNeeds(id string) bool {
	for _, inst := range h.inst {
		if inst.Spec.Power == compute.PowerTerminated || inst.Spec.Image.String() != id {
			continue
		}
		if inst.Status.Observed.Generation == 0 {
			return true
		}
	}
	return false
}

// Restore re-applies converged specs to the agent. It does not write the
// store: the repository already says these resources match, and the agent
// contract makes a second apply a no-op when the agent still has them.
func (r *Reconciler) Restore(ctx context.Context) error {
	var errs []error
	if err := r.eachImage(ctx, func(img compute.Image) error {
		if !img.Available() {
			return nil
		}
		_, err := r.agent.ConvergeImage(ctx, imageSpec(img.Spec))
		return err
	}); err != nil {
		errs = append(errs, err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := r.eachInstance(ctx, func(inst compute.Instance) error {
		if (inst.Spec.ControllerDriven() && r.reports == nil) || inst.Spec.Power == compute.PowerTerminated || inst.Lifecycle() == compute.InstanceError {
			return nil
		}
		if inst.Status.Observed.Generation != inst.Spec.Generation {
			return nil
		}
		m, err := r.machine(ctx, inst.Spec)
		if err != nil {
			return err
		}
		return r.agent.ConvergeMachine(ctx, m)
	}); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *Reconciler) eachInstance(ctx context.Context, fn func(compute.Instance) error) error {
	return eachPage(ctx, func(after string) (store.Page[compute.Instance], error) {
		return r.store.Instances(ctx, store.InstanceQuery{PageSize: store.MaxPageSize, After: after})
	}, fn)
}

func (r *Reconciler) eachImage(ctx context.Context, fn func(compute.Image) error) error {
	return eachPage(ctx, func(after string) (store.Page[compute.Image], error) {
		return r.store.Images(ctx, store.ImageQuery{PageSize: store.MaxPageSize, After: after})
	}, fn)
}

func eachPage[T any](ctx context.Context, page func(after string) (store.Page[T], error), fn func(T) error) error {
	var errs []error
	var after string
	for {
		p, err := page(after)
		if err != nil {
			return err
		}
		for _, item := range p.Items {
			if err := fn(item); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				errs = append(errs, err)
			}
		}
		if p.Next == "" {
			break
		}
		after = p.Next
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
	machine, agentErr := r.machine(ctx, spec)
	if agentErr == nil {
		agentErr = r.agent.ConvergeMachine(ctx, machine)
	}
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
		return compute.Observed{Generation: gen, Failure: parkedFailure(err)}, compute.Retry{}
	case now.Sub(changedAt) >= GiveUpAfter:
		msg := fmt.Sprintf("gave up after %s of transient failures: %v", GiveUpAfter, err)
		return compute.Observed{Generation: gen, Failure: &compute.Failure{Code: compute.CodeInternal, Message: msg}}, compute.Retry{}
	}
	attempts := retry.Attempts + 1
	return prev, compute.Retry{Attempts: attempts, At: now.Add(backoff(attempts)), LastError: err.Error()}
}

func parkedFailure(err error) *compute.Failure {
	code := compute.CodeInternal
	if hypervisor.IsPermanent(err) {
		code = compute.CodeFailedPrecondition
	}
	reason := compute.LaunchInternal
	if hypervisor.CauseOf(err) == hypervisor.CauseImageNotFound {
		reason = compute.LaunchImageNotFound
	}
	return &compute.Failure{Code: code, Message: err.Error(), Reason: reason}
}

// backoff is min(MaxBackoff, 2^min(attempts, 5) seconds).
func backoff(attempts int) time.Duration {
	d := time.Duration(1<<min(attempts, 5)) * time.Second
	return min(d, MaxBackoff)
}

// machine is machineSpec with every claim the instance was accepted
// without resolved through WithClaims. A volume still provisioning is a
// transient error, so the instance stays pending and is retried.
func (r *Reconciler) machine(ctx context.Context, s compute.InstanceSpec) (hypervisor.MachineSpec, error) {
	if s.Power == compute.PowerTerminated {
		return machineSpec(s), nil
	}
	s, err := r.resolveClaims(ctx, s)
	if err != nil {
		return hypervisor.MachineSpec{}, err
	}
	return machineSpec(s), nil
}

func (r *Reconciler) resolveClaims(ctx context.Context, s compute.InstanceSpec) (compute.InstanceSpec, error) {
	if r.claims == nil {
		return s, nil
	}
	vols := make([]compute.VolumeAttachment, len(s.Volumes))
	copy(vols, s.Volumes)
	for i, v := range vols {
		if v.Claim != "" {
			continue
		}
		claim, err := r.claims(ctx, v.Volume.String())
		if err != nil {
			if hypervisor.IsPermanent(err) {
				return compute.InstanceSpec{}, err
			}
			return compute.InstanceSpec{}, fmt.Errorf("volume %s: %w", v.Volume, err)
		}
		if claim == "" {
			return compute.InstanceSpec{}, fmt.Errorf("volume %s is still provisioning", v.Volume)
		}
		vols[i].Claim = claim
	}
	s.Volumes = vols
	return s, nil
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
	m.UserData = s.UserData
	// Data volumes go to the agent by device name and the claim storage
	// reported. The boot entry is the root disk, not a data volume.
	for _, v := range s.Volumes {
		if v.Boot {
			m.BootClaim = v.Claim
		}
		if !v.Boot {
			m.Volumes = append(m.Volumes, hypervisor.Volume{Name: v.DeviceName, Claim: v.Claim})
		}
	}
	// The first interface joins its subnet. The agent gets the subnet ID,
	// the last segment of projects/*/regions/*/vpcs/*/subnets/<id>, which
	// vws-network also uses as the Kube-OVN Subnet name. More interfaces
	// are not wired yet (vws-compute#55).
	if len(s.NICs) > 0 {
		m.Subnet = subnetID(s.NICs[0].Subnet)
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

// subnetID is the ID at the end of a subnet resource name, or "" when name
// has no subnets segment. The gRPC edge has already checked the shape.
func subnetID(name string) string {
	const seg = "/subnets/"
	i := strings.LastIndex(name, seg)
	if i < 0 {
		return ""
	}
	return name[i+len(seg):]
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
	case compute.FromFile:
		i.SourcePath = src.Path.String()
	case compute.FromInstance:
		i.SourceMachine = src.Instance.String()
	case compute.FromVolume:
		i.SourceVolume = src.Ref.String()
	case compute.FromSnapshot:
		i.SourceSnapshot = src.Ref.String()
	}
	return i
}

// onlyNotReady reports whether every error joined into err is a
// hypervisor.NotReadyError. A sweep error that also holds a real failure is
// not, so the failure is still logged as a warning.
func onlyNotReady(err error) bool {
	// Follow the single-error chain. If it reaches a join, every branch of
	// the join must be not-ready. If it does not, errors.As sees the whole
	// chain and nothing else.
	for e := err; e != nil; e = errors.Unwrap(e) {
		multi, ok := e.(interface{ Unwrap() []error }) //nolint:errorlint // looking for a join node itself, not a wrapped one
		if !ok {
			continue
		}
		errs := multi.Unwrap()
		for _, b := range errs {
			if !onlyNotReady(b) {
				return false
			}
		}
		return len(errs) > 0
	}
	return hypervisor.IsNotReady(err)
}
