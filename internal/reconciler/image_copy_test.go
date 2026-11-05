package reconciler_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const (
	copyVolumeRef   = "projects/a123456789012/zones/vws-1a/volumes/vol-1"
	copySnapshotRef = "projects/a123456789012/regions/vws-1/snapshots/snap-1"
	reportedClaim   = "vol-9f3a2b7c41d05e68"
	reportedSnap    = "snap-5e1c0d93ab7742f0"
)

// imageRecorder is a fake agent that remembers every image spec it was given.
type imageRecorder struct {
	*fakeagent.Agent
	mu    sync.Mutex
	specs []hypervisor.ImageSpec
}

func (a *imageRecorder) ConvergeImage(ctx context.Context, spec hypervisor.ImageSpec) (int64, error) {
	a.mu.Lock()
	a.specs = append(a.specs, spec)
	a.mu.Unlock()
	return a.Agent.ConvergeImage(ctx, spec)
}

func (a *imageRecorder) calls() []hypervisor.ImageSpec {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]hypervisor.ImageSpec(nil), a.specs...)
}

// storageAnswers is a resolver fake: it returns the next answer for every
// call and records what it was asked.
type storageAnswers struct {
	mu      sync.Mutex
	answers []answer
	asked   []string
}

type answer struct {
	name string
	err  error
}

func (s *storageAnswers) resolve(_ context.Context, ref string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, ref)
	a := s.answers[min(len(s.asked), len(s.answers))-1]
	return a.name, a.err
}

type copyCase struct {
	name     string
	source   compute.CopySource
	ref      string
	reported string
	option   func(*storageAnswers) reconciler.Option
	field    func(hypervisor.ImageSpec) string
}

var copyCases = []copyCase{
	{
		name: "volume", ref: copyVolumeRef, reported: reportedClaim,
		source: compute.FromVolume{Ref: ok(compute.ParseVolumeRef(copyVolumeRef))},
		option: func(s *storageAnswers) reconciler.Option { return reconciler.WithClaims(s.resolve) },
		field:  func(i hypervisor.ImageSpec) string { return i.SourceClaim },
	},
	{
		name: "snapshot", ref: copySnapshotRef, reported: reportedSnap,
		source: compute.FromSnapshot{Ref: ok(compute.ParseSnapshotRef(copySnapshotRef))},
		option: func(s *storageAnswers) reconciler.Option { return reconciler.WithSnapshotObjects(s.resolve) },
		field:  func(i hypervisor.ImageSpec) string { return i.SourceVolumeSnapshot },
	},
}

type copyHarness struct {
	*harness
	agent *imageRecorder
}

func newCopyHarness(t *testing.T, opts ...reconciler.Option) *copyHarness {
	t.Helper()
	h := newHarness(t, memstore.New(), fakeagent.New(), &clock{t: t0})
	return h.withReconciler(opts...)
}

func (h *harness) withReconciler(opts ...reconciler.Option) *copyHarness {
	rec := &imageRecorder{Agent: h.agent}
	h.rec = reconciler.New(h.repo, rec, append([]reconciler.Option{reconciler.WithClock(h.clock.Now)}, opts...)...)
	h.ctl = newController(h)
	return &copyHarness{harness: h, agent: rec}
}

func (h *copyHarness) copyImage(req string, src compute.CopySource) compute.Operation {
	h.t.Helper()
	scope := ok(compute.NewImageScope("a123456789012", "vws-1"))
	return h.submit(req, ok(compute.NewCopyImage("ami-0000000000000aaa1", scope, src, "", "", compute.Labels{})))
}

func (h *copyHarness) image(op compute.Operation) compute.Image {
	h.t.Helper()
	return ok(h.repo.Image(context.Background(), ok(compute.ParseImageID(op.Target.ID))))
}

// TestCopiedImageClonesTheObjectStorageReported is vws-compute#24: the agent
// is handed the claim or VolumeSnapshot name that vws-storage reported, and
// the resource name the caller used reaches it nowhere.
func TestCopiedImageClonesTheObjectStorageReported(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			s := &storageAnswers{answers: []answer{{name: tc.reported}}}
			h := newCopyHarness(t, tc.option(s))
			op := h.copyImage("11111111-1111-4111-8111-111111111111", tc.source)
			h.sweep()
			h.wantStates(compute.OperationSucceeded, op)

			calls := h.agent.calls()
			if len(calls) != 1 {
				t.Fatalf("agent got %d image calls, want 1: %+v", len(calls), calls)
			}
			if got := tc.field(calls[0]); got != tc.reported {
				t.Fatalf("agent source = %q, want the reported %q", got, tc.reported)
			}
			dump := fmt.Sprintf("%+v", calls[0])
			for _, leaked := range []string{"snap-1", "vol-1", "projects/"} {
				if strings.Contains(dump, leaked) {
					t.Fatalf("the resource name reached the agent: %s", dump)
				}
			}
			if len(s.asked) != 1 || s.asked[0] != tc.ref {
				t.Fatalf("storage was asked %v, want [%s]", s.asked, tc.ref)
			}
		})
	}
}

func TestCopiedImageWaitsForStorageToNameItsSource(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			s := &storageAnswers{answers: []answer{{name: ""}, {name: tc.reported}}}
			h := newCopyHarness(t, tc.option(s))
			op := h.copyImage("22222222-2222-4222-8222-222222222222", tc.source)
			h.sweep()
			h.wantStates(compute.OperationPending, op)
			if n := len(h.agent.calls()); n != 0 {
				t.Fatalf("agent was called %d times before storage named the source", n)
			}
			if img := h.image(op); img.Status.Retry.Attempts != 1 || img.Status.Observed.Failure != nil {
				t.Fatalf("status = %+v, want one retry and no failure", img.Status)
			}
			h.clock.Advance(time.Minute)
			h.sweep()
			h.wantStates(compute.OperationSucceeded, op)
			if got := tc.field(h.agent.calls()[0]); got != tc.reported {
				t.Fatalf("agent source = %q, want %q", got, tc.reported)
			}
		})
	}
}

func TestCopiedImageParksWhenStorageLostItsSource(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			lost := hypervisor.Permanent(errors.New("the source is gone"))
			s := &storageAnswers{answers: []answer{{err: lost}}}
			h := newCopyHarness(t, tc.option(s))
			op := h.copyImage("33333333-3333-4333-8333-333333333333", tc.source)
			h.sweep()
			h.wantStates(compute.OperationFailed, op)
			f := h.image(op).Status.Observed.Failure
			if f == nil || f.Code != compute.CodeFailedPrecondition || !strings.Contains(f.Message, "the source is gone") {
				t.Fatalf("failure = %+v, want FailedPrecondition carrying storage's reason", f)
			}
			if n := len(h.agent.calls()); n != 0 {
				t.Fatalf("agent was called %d times for a lost source", n)
			}
		})
	}
}

func TestCopiedImageParksWithoutAStorageService(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCopyHarness(t)
			op := h.copyImage("44444444-4444-4444-8444-444444444444", tc.source)
			h.sweep()
			h.wantStates(compute.OperationFailed, op)
			f := h.image(op).Status.Observed.Failure
			if f == nil || f.Code != compute.CodeFailedPrecondition ||
				!strings.Contains(f.Message, "needs the storage service") {
				t.Fatalf("failure = %+v, want FailedPrecondition naming the missing storage service", f)
			}
			if n := len(h.agent.calls()); n != 0 {
				t.Fatalf("agent was called %d times with nothing to clone", n)
			}
		})
	}
}

func TestDeregisteredCopyNeedsNoStorage(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			s := &storageAnswers{answers: []answer{{name: tc.reported}}}
			h := newCopyHarness(t, tc.option(s))
			op := h.copyImage("55555555-5555-4555-8555-555555555555", tc.source)
			h.sweep()
			h.wantStates(compute.OperationSucceeded, op)
			id := op.Target.ID
			if !h.agent.HasImage(id) {
				t.Fatal("the copy was not created")
			}

			h = h.withReconciler()
			dereg := h.submit("66666666-6666-4666-8666-666666666666", ok(compute.NewDeregisterImage(id)))
			h.sweep()
			h.wantStates(compute.OperationSucceeded, dereg)
			if h.agent.HasImage(id) {
				t.Fatal("the artifact outlived its deregistration")
			}
		})
	}
}

// restarted is the harness after a restart: the same repository, a fresh
// agent that holds nothing, and a reconciler built with opts.
func (h *copyHarness) restarted(opts ...reconciler.Option) *copyHarness {
	fresh := *h.harness
	fresh.agent = fakeagent.New()
	return fresh.withReconciler(opts...)
}

// copiedAndConverged returns a harness whose copy of tc.source converged
// against storage reporting tc.reported.
func copiedAndConverged(t *testing.T, tc copyCase) (*copyHarness, compute.Operation) {
	t.Helper()
	h := newCopyHarness(t, tc.option(&storageAnswers{answers: []answer{{name: tc.reported}}}))
	op := h.copyImage("77777777-7777-4777-8777-777777777777", tc.source)
	h.sweep()
	h.wantStates(compute.OperationSucceeded, op)
	return h, op
}

func (h *copyHarness) wantLaunchSucceeds(req string, op compute.Operation) {
	h.t.Helper()
	run := h.submit(req, ok(compute.NewRunInstance(op.Target.ID, "vws.small", "")))
	h.sweep()
	h.wantStates(compute.OperationSucceeded, run)
}

// TestRestoreClonesTheStoredObject: the object storage reported is kept with
// the converged image, and restore hands the agent that name without asking
// storage.
func TestRestoreClonesTheStoredObject(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			h, op := copiedAndConverged(t, tc)
			if got := h.image(op).Status.SourceObject; got != tc.reported {
				t.Fatalf("stored source object = %q, want %q", got, tc.reported)
			}

			s := &storageAnswers{answers: []answer{{name: "vol-other"}}}
			restart := h.restarted(tc.option(s))
			if err := restart.rec.Restore(context.Background()); err != nil {
				t.Fatal(err)
			}
			calls := restart.agent.calls()
			if len(calls) != 1 || tc.field(calls[0]) != tc.reported {
				t.Fatalf("restore gave the agent %+v, want one image on %q", calls, tc.reported)
			}
			if len(s.asked) != 0 {
				t.Fatalf("restore asked storage %v, want nothing", s.asked)
			}
			restart.sweep()
			if n := len(restart.agent.calls()); n != 1 {
				t.Fatalf("agent got %d image calls after a sweep, want the restore's one", n)
			}
		})
	}
}

// TestRestoreSurvivesTheSourceBeingDeleted: deleting the volume or snapshot
// after imaging leaves the image available, as an AWS AMI outlives its
// snapshot. A restart must put its artifact back and keep launches working.
func TestRestoreSurvivesTheSourceBeingDeleted(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			h, op := copiedAndConverged(t, tc)
			gone := &storageAnswers{answers: []answer{{err: hypervisor.Permanent(errors.New("the source is gone"))}}}
			restart := h.restarted(tc.option(gone))
			if err := restart.rec.Restore(context.Background()); err != nil {
				t.Fatalf("restore: %v", err)
			}
			if !restart.agent.HasImage(op.Target.ID) {
				t.Fatal("restore did not put the copy back")
			}
			if !restart.image(op).Available() {
				t.Fatal("the image stopped being available when its source was deleted")
			}
			restart.wantLaunchSucceeds("99999999-9999-4999-8999-999999999999", op)
		})
	}
}

func TestRestoreSurvivesStorageBeingDown(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			h, op := copiedAndConverged(t, tc)
			down := &storageAnswers{answers: []answer{{err: errors.New("storage is down")}}}
			restart := h.restarted(tc.option(down))
			if err := restart.rec.Restore(context.Background()); err != nil {
				t.Fatalf("restore: %v", err)
			}
			if !restart.agent.HasImage(op.Target.ID) {
				t.Fatal("restore did not put the copy back")
			}
			restart.wantLaunchSucceeds("99999999-9999-4999-8999-999999999999", op)
		})
	}
}

// TestRestoreResolvesARecordWrittenBeforeTheObjectWasStored: an available copy
// with no stored object still asks storage, and does not call the agent
// without a resolved source.
func TestRestoreResolvesARecordWrittenBeforeTheObjectWasStored(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			h, op := copiedAndConverged(t, tc)
			old := h.image(op).Status
			old.SourceObject = ""
			if err := h.repo.CommitImageStatus(context.Background(), ok(compute.ParseImageID(op.Target.ID)), old); err != nil {
				t.Fatal(err)
			}

			s := &storageAnswers{answers: []answer{{name: tc.reported}}}
			restart := h.restarted(tc.option(s))
			if err := restart.rec.Restore(context.Background()); err != nil {
				t.Fatal(err)
			}
			calls := restart.agent.calls()
			if len(calls) != 1 || tc.field(calls[0]) != tc.reported || len(s.asked) != 1 || s.asked[0] != tc.ref {
				t.Fatalf("agent got %+v and storage was asked %v, want one image on %q from %s",
					calls, s.asked, tc.reported, tc.ref)
			}

			down := h.restarted(tc.option(&storageAnswers{answers: []answer{{err: errors.New("storage is down")}}}))
			if err := down.rec.Restore(context.Background()); err == nil || !strings.Contains(err.Error(), "storage is down") {
				t.Fatalf("restore with storage down = %v, want its error", err)
			}
			if n := len(down.agent.calls()); n != 0 {
				t.Fatalf("restore called the agent %d times without a resolved source", n)
			}
		})
	}
}

func TestCopiedImageReplayCreatesOnce(t *testing.T) {
	for _, tc := range copyCases {
		t.Run(tc.name, func(t *testing.T) {
			s := &storageAnswers{answers: []answer{{name: tc.reported}}}
			h := newCopyHarness(t, tc.option(s))
			const req = "88888888-8888-4888-8888-888888888888"
			op := h.copyImage(req, tc.source)
			h.sweep()
			h.sweep()
			again := h.copyImage(req, tc.source)
			h.sweep()
			if again.ID != op.ID {
				t.Fatalf("replay returned operation %s, want %s", again.ID, op.ID)
			}
			if n := len(h.agent.calls()); n != 1 {
				t.Fatalf("agent got %d image calls across sweeps and a replay, want 1", n)
			}
		})
	}
}
