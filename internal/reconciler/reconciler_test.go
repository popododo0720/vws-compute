package reconciler_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
	"github.com/popododo0720/vws-compute/internal/store/sqlitestore"
)

var t0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type harness struct {
	t     *testing.T
	repo  store.Repository
	agent *fakeagent.Agent
	clock *clock
	ctl   *controller.Controller
	rec   *reconciler.Reconciler
}

func newHarness(t *testing.T, repo store.Repository, agent *fakeagent.Agent, clk *clock) *harness {
	rec := reconciler.New(repo, agent, reconciler.WithClock(clk.Now))
	return &harness{
		t:     t,
		repo:  repo,
		agent: agent,
		clock: clk,
		ctl:   controller.New(repo, controller.WithClock(clk.Now), controller.WithKick(rec.Kick)),
		rec:   rec,
	}
}

func openSQLite(t *testing.T, path string) store.Repository {
	t.Helper()
	s, err := sqlitestore.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// eachStore runs fn against a fresh memstore and a fresh SQLite store.
func eachStore(t *testing.T, fn func(h *harness)) {
	t.Run("memstore", func(t *testing.T) {
		fn(newHarness(t, memstore.New(), fakeagent.New(), &clock{t: t0}))
	})
	t.Run("sqlitestore", func(t *testing.T) {
		repo := openSQLite(t, filepath.Join(t.TempDir(), "state.db"))
		t.Cleanup(func() { _ = repo.Close() })
		fn(newHarness(t, repo, fakeagent.New(), &clock{t: t0}))
	})
}

func (h *harness) submit(id string, in compute.Intent) compute.Operation {
	h.t.Helper()
	r, err := h.ctl.Submit(context.Background(), ok(compute.ParseRequestID(id)), in)
	if err != nil {
		h.t.Fatalf("Submit %s: %v", id, err)
	}
	return r.Operation
}

func (h *harness) sweep() {
	h.t.Helper()
	if err := h.rec.Sweep(context.Background()); err != nil {
		h.t.Fatalf("Sweep: %v", err)
	}
}

func (h *harness) state(op compute.Operation) compute.OperationState {
	h.t.Helper()
	return ok(h.repo.Operation(context.Background(), op.ID)).State()
}

func (h *harness) wantStates(want compute.OperationState, ops ...compute.Operation) {
	h.t.Helper()
	for _, op := range ops {
		if got := h.state(op); got != want {
			h.t.Fatalf("operation %s (%s) is %s, want %s", op.ID, op.Kind, got, want)
		}
	}
}

func (h *harness) instance(id compute.InstanceID) compute.Instance {
	h.t.Helper()
	return ok(h.repo.Instance(context.Background(), id))
}

// image registers an image and converges it.
func (h *harness) image() compute.ImageID {
	h.t.Helper()
	op := h.submit("register", ok(compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2")))
	h.sweep()
	h.wantStates(compute.OperationSucceeded, op)
	return ok(compute.ParseImageID(op.Target.ID))
}

// running launches an instance and converges it.
func (h *harness) running() compute.InstanceID {
	h.t.Helper()
	op := h.submit("run", ok(compute.NewRunInstance(h.image().String(), "vws.small", "")))
	h.sweep()
	h.wantStates(compute.OperationSucceeded, op)
	return ok(compute.ParseInstanceID(op.Target.ID))
}

func TestSubmitNeverCallsTheAgent(t *testing.T) {
	eachStore(t, func(h *harness) {
		id := h.running()
		before := h.agent.Calls()
		release := h.agent.Block()
		defer release()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		intents := []compute.Intent{
			ok(compute.NewRebootInstance(id.String())),
			ok(compute.NewStopInstance(id.String())),
			ok(compute.NewStartInstance(id.String())),
			ok(compute.NewCreateImage(id.String(), "snapshot-1")),
			ok(compute.NewRegisterImage("debian-14", "https://images.example/debian-14.qcow2")),
			ok(compute.NewCreateKeyPair("deploy")),
			ok(compute.NewTerminateInstance(id.String())),
		}
		for i, in := range intents {
			r, err := h.ctl.Submit(ctx, ok(compute.ParseRequestID("blocked-"+in.Kind().String())), in)
			if err != nil {
				h.t.Fatalf("intent %d (%s): %v", i, in.Kind(), err)
			}
			if r.Operation.ID.IsZero() {
				h.t.Fatalf("intent %d returned no operation", i)
			}
		}
		if got := h.agent.Calls(); got != before {
			h.t.Fatalf("agent calls during Submit = %d, want 0", got-before)
		}
	})
}

func TestStartThenStopEndsStoppedWithPowerOff(t *testing.T) {
	eachStore(t, func(h *harness) {
		id := h.running()
		h.submit("stop-1", ok(compute.NewStopInstance(id.String())))
		h.sweep()

		start := h.submit("start", ok(compute.NewStartInstance(id.String())))
		stop := h.submit("stop-2", ok(compute.NewStopInstance(id.String())))
		h.sweep()

		h.wantStates(compute.OperationSuperseded, start)
		h.wantStates(compute.OperationSucceeded, stop)
		inst := h.instance(id)
		if inst.Spec.Power != compute.PowerStopped || inst.Lifecycle() != compute.InstanceStopped || inst.Status.Machine != compute.MachineOff {
			h.t.Fatalf("instance = power %d lifecycle %d machine %d, want stopped/stopped/off", inst.Spec.Power, inst.Lifecycle(), inst.Status.Machine)
		}
		m, exists := h.agent.Machine(id.String())
		if !exists || m.Power != hypervisor.PowerOff {
			h.t.Fatalf("agent machine = %+v exists=%v, want powered off", m, exists)
		}
		if got := h.agent.Creations(id.String()); got != 1 {
			h.t.Fatalf("creations = %d, want 1", got)
		}
	})
}

func TestTerminateDestroysTheMachine(t *testing.T) {
	eachStore(t, func(h *harness) {
		id := h.running()
		term := h.submit("term", ok(compute.NewTerminateInstance(id.String())))
		if got := h.instance(id).Lifecycle(); got != compute.InstanceShuttingDown {
			h.t.Fatalf("lifecycle before sweep = %d, want shutting down", got)
		}
		h.sweep()
		h.wantStates(compute.OperationSucceeded, term)
		if _, exists := h.agent.Machine(id.String()); exists {
			h.t.Fatal("machine still exists after terminate converged")
		}
		inst := h.instance(id)
		if inst.Lifecycle() != compute.InstanceTerminated || inst.Status.Machine != compute.MachineAbsent {
			h.t.Fatalf("instance = lifecycle %d machine %d, want terminated/absent", inst.Lifecycle(), inst.Status.Machine)
		}
		_, err := h.ctl.Submit(context.Background(), ok(compute.ParseRequestID("start")), ok(compute.NewStartInstance(id.String())))
		if compute.CodeOf(err) != compute.CodeFailedPrecondition {
			h.t.Fatalf("start after terminate: err = %v, want FailedPrecondition", err)
		}
	})
}

func TestConcurrentRebootsCoalesceIntoOne(t *testing.T) {
	eachStore(t, func(h *harness) {
		id := h.running()
		first := h.submit("reboot-1", ok(compute.NewRebootInstance(id.String())))
		second := h.submit("reboot-2", ok(compute.NewRebootInstance(id.String())))
		if second.TargetGeneration != first.TargetGeneration+1 {
			h.t.Fatalf("second reboot targets generation %d, want %d", second.TargetGeneration, first.TargetGeneration+1)
		}
		h.sweep()
		h.wantStates(compute.OperationSuperseded, first)
		h.wantStates(compute.OperationSucceeded, second)
		if got := h.agent.Reboots(id.String()); got != 1 {
			h.t.Fatalf("reboots = %d, want 1", got)
		}

		third := h.submit("reboot-3", ok(compute.NewRebootInstance(id.String())))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, third)
		if got := h.agent.Reboots(id.String()); got != 2 {
			h.t.Fatalf("reboots after a later reboot = %d, want 2", got)
		}
	})
}

func TestPermanentAgentErrorParksUntilAskedAgain(t *testing.T) {
	eachStore(t, func(h *harness) {
		img := h.image()
		run := h.submit("run", ok(compute.NewRunInstance(img.String(), "vws.small", "")))
		id := ok(compute.ParseInstanceID(run.Target.ID))
		h.agent.Fail(id.String(), hypervisor.Permanent(errors.New("disk corrupt")))
		h.sweep()

		got := ok(h.repo.Operation(context.Background(), run.ID))
		if got.State() != compute.OperationFailed || *got.Failure() != (compute.Failure{Code: compute.CodeFailedPrecondition, Message: "disk corrupt"}) {
			h.t.Fatalf("run = %s %v, want Failed with FailedPrecondition: disk corrupt", got.State(), got.Failure())
		}
		if lc := h.instance(id).Lifecycle(); lc != compute.InstanceError {
			h.t.Fatalf("lifecycle = %d, want error", lc)
		}
		calls := h.agent.Calls()
		h.clock.Advance(time.Hour)
		h.sweep()
		if h.agent.Calls() != calls {
			h.t.Fatal("reconciler retried a parked generation")
		}

		start := h.submit("start", ok(compute.NewStartInstance(id.String())))
		if start.TargetGeneration != 2 {
			h.t.Fatalf("start on a parked instance targets generation %d, want a new generation 2", start.TargetGeneration)
		}
		h.sweep()
		h.wantStates(compute.OperationSucceeded, start)
		if lc := h.instance(id).Lifecycle(); lc != compute.InstanceRunning {
			h.t.Fatalf("lifecycle after retry = %d, want running", lc)
		}
	})
}

func TestTransientAgentErrorsBackOffThenConverge(t *testing.T) {
	eachStore(t, func(h *harness) {
		img := h.image()
		run := h.submit("run", ok(compute.NewRunInstance(img.String(), "vws.small", "")))
		id := ok(compute.ParseInstanceID(run.Target.ID))
		h.agent.Fail(id.String(), errors.New("agent busy"), errors.New("agent busy"))
		start := h.clock.Now()

		h.sweep()
		if got, want := h.instance(id).Status.Retry, (compute.Retry{Attempts: 1, At: start.Add(2 * time.Second), LastError: "agent busy"}); got != want {
			h.t.Fatalf("retry after first failure = %+v, want %+v", got, want)
		}
		h.wantStates(compute.OperationPending, run)

		calls := h.agent.Calls()
		h.clock.Advance(time.Second)
		h.sweep()
		if h.agent.Calls() != calls {
			h.t.Fatal("reconciler called the agent before the retry time")
		}

		h.clock.Advance(time.Second)
		h.sweep()
		if got, want := h.instance(id).Status.Retry, (compute.Retry{Attempts: 2, At: start.Add(6 * time.Second), LastError: "agent busy"}); got != want {
			h.t.Fatalf("retry after second failure = %+v, want %+v", got, want)
		}

		h.clock.Advance(4 * time.Second)
		h.sweep()
		h.wantStates(compute.OperationSucceeded, run)
		if got := h.instance(id).Status.Retry; got != (compute.Retry{}) {
			h.t.Fatalf("retry after success = %+v, want cleared", got)
		}
	})
}

func TestTransientErrorsParkAfterTheDeadline(t *testing.T) {
	eachStore(t, func(h *harness) {
		img := h.image()
		run := h.submit("run", ok(compute.NewRunInstance(img.String(), "vws.small", "")))
		id := ok(compute.ParseInstanceID(run.Target.ID))
		h.agent.Fail(id.String(), errors.New("agent unreachable"), errors.New("agent unreachable"))

		h.sweep()
		h.wantStates(compute.OperationPending, run)
		h.clock.Advance(reconciler.GiveUpAfter)
		h.sweep()

		got := ok(h.repo.Operation(context.Background(), run.ID))
		if got.State() != compute.OperationFailed || got.Failure().Code != compute.CodeInternal {
			h.t.Fatalf("run after deadline = %s %v, want Failed with Internal", got.State(), got.Failure())
		}
		if refs := ok(h.repo.Unconverged(context.Background(), h.clock.Now().Add(time.Hour))); len(refs) != 0 {
			h.t.Fatalf("unconverged after parking = %v, want none", refs)
		}
	})
}

func TestImageCaptureAndDeregister(t *testing.T) {
	eachStore(t, func(h *harness) {
		id := h.running()
		capture := h.submit("capture", ok(compute.NewCreateImage(id.String(), "snapshot-1")))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, capture)
		snap := ok(compute.ParseImageID(capture.Target.ID))
		img := ok(h.repo.Image(context.Background(), snap))
		if !img.Available() || img.Status.SizeBytes != fakeagent.ImageSize || !h.agent.HasImage(snap.String()) {
			h.t.Fatalf("captured image = %+v, want available with the agent's artifact", img.Status)
		}

		dereg := h.submit("dereg", ok(compute.NewDeregisterImage(snap.String())))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, dereg)
		if h.agent.HasImage(snap.String()) {
			h.t.Fatal("artifact still present after deregister converged")
		}
		if lc := ok(h.repo.Image(context.Background(), snap)).Lifecycle(); lc != compute.ImageDeregistered {
			h.t.Fatalf("lifecycle = %d, want deregistered", lc)
		}
	})
}

func TestCaptureBeforeTerminate(t *testing.T) {
	eachStore(t, func(h *harness) {
		id := h.running()
		capture := h.submit("capture", ok(compute.NewCreateImage(id.String(), "snapshot-1")))
		term := h.submit("term", ok(compute.NewTerminateInstance(id.String())))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, capture, term)
		snap := ok(compute.ParseImageID(capture.Target.ID))
		if !h.agent.HasImage(snap.String()) {
			h.t.Fatal("capture was lost because the instance was destroyed first")
		}
		if _, exists := h.agent.Machine(id.String()); exists {
			h.t.Fatal("machine still exists after terminate")
		}
	})
}

func TestLaunchBeforeDeregister(t *testing.T) {
	eachStore(t, func(h *harness) {
		img := h.image()
		run := h.submit("run", ok(compute.NewRunInstance(img.String(), "vws.small", "")))
		dereg := h.submit("dereg", ok(compute.NewDeregisterImage(img.String())))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, run, dereg)
		id := run.Target.ID
		if _, exists := h.agent.Machine(id); !exists {
			h.t.Fatal("instance was not created before its image was deregistered")
		}
		if h.agent.HasImage(img.String()) {
			h.t.Fatal("image artifact still present after deregister")
		}
	})
}

func TestNewGenerationDoesNotInheritBackoff(t *testing.T) {
	eachStore(t, func(h *harness) {
		id := h.running()
		h.agent.Fail(id.String(), errors.New("agent unreachable"))
		stop := h.submit("stop", ok(compute.NewStopInstance(id.String())))
		h.sweep()
		h.wantStates(compute.OperationPending, stop)

		term := h.submit("term", ok(compute.NewTerminateInstance(id.String())))
		h.sweep()
		h.wantStates(compute.OperationSuperseded, stop)
		h.wantStates(compute.OperationSucceeded, term)
		if _, exists := h.agent.Machine(id.String()); exists {
			h.t.Fatal("terminate waited out the previous backoff")
		}
	})
}

func TestRestartRebuildsTheAgent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	clk := &clock{t: t0}
	h := newHarness(t, openSQLite(t, path), fakeagent.New(), clk)
	id := h.running()
	img := h.instance(id).Spec.Image
	if err := h.repo.Close(); err != nil {
		t.Fatal(err)
	}

	repo := openSQLite(t, path)
	defer repo.Close()
	agent := fakeagent.New()
	if err := reconciler.New(repo, agent, reconciler.WithClock(clk.Now)).Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !agent.HasImage(img.String()) {
		t.Fatal("restore did not put the image artifact back")
	}
	m, exists := agent.Machine(id.String())
	if !exists || m.Power != hypervisor.PowerOn {
		t.Fatalf("machine after restore = %+v exists=%v, want powered on", m, exists)
	}

	ctl := controller.New(repo, controller.WithClock(clk.Now))
	run, err := ctl.Submit(context.Background(), ok(compute.ParseRequestID("run-2")), ok(compute.NewRunInstance(img.String(), "vws.small", "")))
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.New(repo, agent, reconciler.WithClock(clk.Now)).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := ok(repo.Operation(context.Background(), run.Operation.ID)).State(); got != compute.OperationSucceeded {
		t.Fatalf("run after restore = %s, want Succeeded", got)
	}
}

func TestRestartResumesFromTheRepositoryAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	agent := fakeagent.New()
	clk := &clock{t: t0}

	h := newHarness(t, openSQLite(t, path), agent, clk)
	img := h.image()
	run := h.submit("run", ok(compute.NewRunInstance(img.String(), "vws.medium", "")))
	id := run.Target.ID
	if err := h.repo.Close(); err != nil {
		t.Fatal(err)
	}
	if got := agent.Creations(id); got != 0 {
		t.Fatalf("creations before restart = %d, want 0", got)
	}

	repo := openSQLite(t, path)
	defer repo.Close()
	if err := reconciler.New(repo, agent, reconciler.WithClock(clk.Now)).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := ok(repo.Operation(context.Background(), run.ID)).State(); got != compute.OperationSucceeded {
		t.Fatalf("run after restart = %s, want Succeeded", got)
	}
	m, exists := agent.Machine(id)
	if !exists || m.Power != hypervisor.PowerOn || m.VCPUs != 2 || m.MemoryMiB != 4096 || m.Image != img.String() {
		t.Fatalf("machine after restart = %+v exists=%v, want vws.medium on %s powered on", m, exists, img)
	}
	if got := agent.Creations(id); got != 1 {
		t.Fatalf("creations = %d, want 1", got)
	}
}

// losesInstanceStatus drops the next CommitInstanceStatus, as if the process
// died after the agent applied the spec and before the status commit.
type losesInstanceStatus struct {
	store.StatusStore
	lost bool
}

func (s *losesInstanceStatus) CommitInstanceStatus(ctx context.Context, id compute.InstanceID, st compute.InstanceStatus) error {
	if !s.lost {
		s.lost = true
		return errors.New("process died before the status commit")
	}
	return s.StatusStore.CommitInstanceStatus(ctx, id, st)
}

func TestLostStatusCommitConvergesWithoutASecondCreation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	agent := fakeagent.New()
	clk := &clock{t: t0}

	h := newHarness(t, openSQLite(t, path), agent, clk)
	img := h.image()
	run := h.submit("run", ok(compute.NewRunInstance(img.String(), "vws.small", "")))
	id := run.Target.ID
	if err := reconciler.New(&losesInstanceStatus{StatusStore: h.repo}, agent, reconciler.WithClock(clk.Now)).Sweep(context.Background()); err == nil {
		t.Fatal("sweep with a lost status commit reported success")
	}
	if got := agent.Creations(id); got != 1 {
		t.Fatalf("creations after the agent call = %d, want 1", got)
	}
	h.wantStates(compute.OperationPending, run)
	if err := h.repo.Close(); err != nil {
		t.Fatal(err)
	}

	repo := openSQLite(t, path)
	defer repo.Close()
	if err := reconciler.New(repo, agent, reconciler.WithClock(clk.Now)).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := ok(repo.Operation(context.Background(), run.ID)).State(); got != compute.OperationSucceeded {
		t.Fatalf("run after restart = %s, want Succeeded", got)
	}
	if got := agent.Creations(id); got != 1 {
		t.Fatalf("creations after restart = %d, want still 1", got)
	}
}

func TestCanceledAgentCallCommitsNothing(t *testing.T) {
	h := newHarness(t, memstore.New(), fakeagent.New(), &clock{t: t0})
	img := h.image()
	run := h.submit("run", ok(compute.NewRunInstance(img.String(), "vws.small", "")))
	release := h.agent.Block()
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.rec.Sweep(ctx) }()
	for h.agent.Calls() < 2 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Sweep = %v, want context.Canceled", err)
	}
	inst := h.instance(ok(compute.ParseInstanceID(run.Target.ID)))
	if inst.Status.Retry != (compute.Retry{}) || inst.Status.Observed.Generation != 0 {
		t.Fatalf("status after a canceled call = %+v, want untouched", inst.Status)
	}
}

// countsSweeps counts completed Unconverged reads, one per sweep.
type countsSweeps struct {
	store.StatusStore
	n atomic.Int32
}

func (s *countsSweeps) Unconverged(ctx context.Context, now time.Time) ([]compute.Ref, error) {
	refs, err := s.StatusStore.Unconverged(ctx, now)
	s.n.Add(1)
	return refs, err
}

func TestRunConverges(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		kick     bool
	}{
		{"OnKick", time.Hour, true},
		{"OnInterval", 5 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := memstore.New()
			sweeps := &countsSweeps{StatusStore: repo}
			rec := reconciler.New(sweeps, fakeagent.New(), reconciler.WithInterval(tc.interval))
			var ctlOpts []controller.Option
			if tc.kick {
				ctlOpts = append(ctlOpts, controller.WithKick(rec.Kick))
			}
			ctl := controller.New(repo, ctlOpts...)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- rec.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Errorf("Run = %v, want context.Canceled", err)
				}
			}()

			for sweeps.n.Load() == 0 {
				time.Sleep(time.Millisecond)
			}
			r, err := ctl.Submit(ctx, ok(compute.ParseRequestID("reg")), ok(compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")))
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for ok(repo.Operation(ctx, r.Operation.ID)).State() != compute.OperationSucceeded {
				if time.Now().After(deadline) {
					t.Fatal("operation did not converge")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestMetadataUpdateDoesNotCallTheAgent(t *testing.T) {
	eachStore(t, func(h *harness) {
		imgID := h.image()
		before := h.agent.Calls()
		labels := ok(compute.ParseLabels(map[string]string{"vws-fingerprint": "fp"}))
		desc := ok(compute.ParseDescription("notes"))
		patch := compute.MetadataPatch{}.ReplaceLabels(labels).SetDescription(desc)
		op := h.submit("meta", ok(compute.NewUpdateImageMetadata(imgID.String(), patch)))
		if op.State() != compute.OperationSucceeded || !op.MetadataOnly {
			h.t.Fatalf("metadata update born %s metadata=%v, want Succeeded MetadataOnly", op.State(), op.MetadataOnly)
		}
		h.sweep()
		if h.agent.Calls() != before {
			h.t.Fatal("label and description update called the agent")
		}
		img := ok(h.repo.Image(context.Background(), imgID))
		if !img.Available() || img.Spec.Generation != 1 || !img.Meta.Labels.Equal(labels) || img.Meta.Description.String() != "notes" {
			h.t.Fatalf("image after metadata = available %v gen %d labels %v desc %q", img.Available(), img.Spec.Generation, img.Meta.Labels.Map(), img.Meta.Description.String())
		}
		run := h.submit("launch", ok(compute.NewRunInstance(imgID.String(), "vws.small", "")))
		h.sweep()
		h.wantStates(compute.OperationSucceeded, run)
		if !ok(h.repo.Image(context.Background(), imgID)).Available() {
			h.t.Fatal("image stopped being launchable after a metadata update")
		}
	})
}
