// Package storetest is the conformance suite every store.Repository must
// pass. It is the executable form of the doc comments in package store: when
// the two disagree, fix the suite first.
package storetest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

// Run runs the suite. open must return a fresh, empty repository per call.
func Run(t *testing.T, open func(t *testing.T) store.Repository) {
	cases := []struct {
		name string
		fn   func(f fixture)
	}{
		{"ReplayReturnsOriginalOperation", replay},
		{"ReplayWithDifferentFingerprintMismatches", mismatch},
		{"ConcurrentReplayCreatesOneOperation", race},
		{"StaleExpectConflictsAndWritesNothing", conflict},
		{"NoOpOnConvergedTargetIsBornSucceeded", bornSettled},
		{"KeyPairsSettleAtCommitAndKeepLiveNamesUnique", keyPairs},
		{"ObservationSettlesAndNewerSpecSupersedes", settles},
		{"ObservationIsMonotonic", monotonic},
		{"UnconvergedListsDueDirtyResources", unconverged},
		{"ValuesRoundTrip", roundTrip},
		{"ReturnedValuesShareNoMemory", aliasing},
		{"ListingsPageAndFilter", paging},
		{"MetadataRoundTrips", metadataRoundTrip},
		{"GenerationWriteDoesNotClobberMetadata", generationLeavesMetadata},
		{"MetadataOnlyLeavesSiblingsPending", metadataLeavesSiblings},
		{"StaleMetadataRevisionConflicts", staleMetadata},
		{"CreateOverExistingImageConflicts", createOverExistingImage},
		{"ImageSourcesRoundTrip", imageSourcesRoundTrip},
		{"NodesSettleAtCommitAndPage", nodesRoundTrip},
		{"NodeDrivenInstanceRoundTripsAndOwnsItsVolumes", nodeDrivenRoundTrip},
		{"NodeDrivenInstancesAreNotUnconverged", nodeDrivenNotUnconverged},
		{"CreatePlacesOnAnEligibleNode", createPlaces},
		{"SyncNodeHeartbeatsAndPlacesOnce", syncPlaces},
		{"SyncNodeAcceptFailureWritesNothing", syncAccept},
		{"ReportSettlesAndKeepsAssignment", reportSettles},
		{"DeleteOfUnassignedSettlesAtOnceAndAssignedWaitsForNode", deleteFlows},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(fixture{t: t, s: open(t)}) })
	}
}

var (
	t0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Minute)
)

const rsa2048 = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC6agmoZNzykPn68/dYUyo0Fq+XPdJdOOGAyea7H8ztra3l7JssAKi47+DrZA3yFXddoBCBOeTuMVs+YVLsBNnLIcRTKVS9u+rt0pmmbikKemsToGCy5afWv1GiLIa7et+JL3a1tnRudrwmwgstwxaknap872dXuE0Cp61v++oWPYVneYTfcVCfJxxmIWIWgphksniGCdSX5S9tAgV167lsBil6aJVFTV6Q8a5E51rMzH4Tr2RGvk9PuTG/4X9B/Gl0MY9CDAx9X2Y0ARCTuG3fCyX+ajiV35+Hzt8H5zsBhy34i6ETPk1kLDG4me+WxCevvqexzmR0ULcSFUsHdJCR"

type fixture struct {
	t *testing.T
	s store.Repository
}

func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func instanceID(n int) compute.InstanceID {
	return ok(compute.ParseInstanceID(fmt.Sprintf("i-%017x", n)))
}
func imageID(n int) compute.ImageID { return ok(compute.ParseImageID(fmt.Sprintf("img-%017x", n))) }
func keyPairID(n int) compute.KeyPairID {
	return ok(compute.ParseKeyPairID(fmt.Sprintf("key-%017x", n)))
}

func instanceSpec(n int, gen int64, power compute.Power) compute.InstanceSpec {
	return compute.InstanceSpec{
		ID:         instanceID(n),
		Generation: gen,
		CreatedAt:  t0,
		ChangedAt:  t0.Add(time.Duration(gen) * time.Second),
		Image:      imageID(1),
		Type:       ok(compute.ParseInstanceType("vws.small")),
		Power:      power,
	}
}

func imageSpec(n int, gen int64) compute.ImageSpec {
	return compute.ImageSpec{
		ID:         imageID(n),
		Generation: gen,
		CreatedAt:  t0,
		ChangedAt:  t0,
		Name:       ok(compute.ParseImageName("debian-13")),
		Source:     compute.FromURL{URL: ok(compute.ParseImageURL("https://images.example/debian-13.qcow2"))},
	}
}

func keySpec(n int, name string, gen int64, deleted bool) compute.KeyPairSpec {
	return compute.KeyPairSpec{
		ID:         keyPairID(n),
		Generation: gen,
		CreatedAt:  t0,
		ChangedAt:  t0.Add(time.Duration(n*10+int(gen)) * time.Second),
		Name:       ok(compute.ParseKeyName(name)),
		PublicKey:  ok(compute.ParsePublicKey(rsa2048)),
		Deleted:    deleted,
	}
}

func runIntent(typ string) compute.Intent {
	return ok(compute.NewRunInstance(imageID(1).String(), typ, ""))
}

func stopIntent(n int) compute.Intent  { return ok(compute.NewStopInstance(instanceID(n).String())) }
func startIntent(n int) compute.Intent { return ok(compute.NewStartInstance(instanceID(n).String())) }

func (f fixture) commit(req string, in compute.Intent, spec compute.Spec, write bool, expect ...store.Version) (compute.Operation, error) {
	f.t.Helper()
	op, err := compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, ok(compute.ParseRequestID(req)), in,
		compute.Change{Spec: spec, Write: write})
	if err != nil {
		f.t.Fatal(err)
	}
	return f.s.CommitDesired(context.Background(), store.DesiredChange{Op: op, Spec: spec, Write: write, Expect: expect})
}

func (f fixture) mustCommit(req string, in compute.Intent, spec compute.Spec, write bool, expect ...store.Version) compute.Operation {
	f.t.Helper()
	op, err := f.commit(req, in, spec, write, expect...)
	if err != nil {
		f.t.Fatalf("commit %s: %v", req, err)
	}
	return op
}

func (f fixture) observeInstance(n int, st compute.InstanceStatus) {
	f.t.Helper()
	if err := f.s.CommitInstanceStatus(context.Background(), instanceID(n), st); err != nil {
		f.t.Fatalf("CommitInstanceStatus: %v", err)
	}
}

func (f fixture) op(id compute.OperationID) compute.Operation {
	f.t.Helper()
	return ok(f.s.Operation(context.Background(), id))
}

func (f fixture) wantState(op compute.Operation, want compute.OperationState) {
	f.t.Helper()
	if got := f.op(op.ID).State(); got != want {
		f.t.Fatalf("operation %s (%s) is %s, want %s", op.ID, op.Kind, got, want)
	}
}

func wantCode(t *testing.T, err error, want compute.Code) {
	t.Helper()
	if err == nil || compute.CodeOf(err) != want {
		t.Fatalf("err = %v, want code %s", err, want)
	}
}

func replay(f fixture) {
	ctx := context.Background()
	first := f.mustCommit("req-1", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	// A retry replans, so it mints a new instance id and a new operation id.
	again := f.mustCommit("req-1", runIntent("vws.small"), instanceSpec(2, 1, compute.PowerRunning), true)
	if again.ID != first.ID {
		f.t.Fatalf("replay returned operation %s, want %s", again.ID, first.ID)
	}
	all := ok(f.s.Instances(ctx, store.InstanceQuery{}))
	if len(all.Items) != 1 || all.Items[0].Spec.ID != instanceID(1) {
		f.t.Fatalf("instances after replay = %d, want only %s", len(all.Items), instanceID(1))
	}
	_, err := f.s.Instance(ctx, instanceID(2))
	wantCode(f.t, err, compute.CodeNotFound)
}

func mismatch(f fixture) {
	ctx := context.Background()
	first := f.mustCommit("req-1", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	_, err := f.commit("req-1", runIntent("vws.medium"), instanceSpec(2, 1, compute.PowerRunning), true)
	wantCode(f.t, err, compute.CodeRequestMismatch)
	_, err = f.s.Instance(ctx, instanceID(2))
	wantCode(f.t, err, compute.CodeNotFound)
	if got := ok(f.s.OperationByRequest(ctx, ok(compute.ParseRequestID("req-1")))); got.ID != first.ID {
		f.t.Fatalf("request req-1 maps to %s, want %s", got.ID, first.ID)
	}
}

func race(f fixture) {
	const n = 8
	ids := make([]compute.OperationID, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			op, err := f.commit("same", runIntent("vws.small"), instanceSpec(i+1, 1, compute.PowerRunning), true)
			ids[i], errs[i] = op.ID, err
		}(i)
	}
	wg.Wait()
	for i := range ids {
		if errs[i] != nil {
			f.t.Fatalf("racer %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			f.t.Fatalf("racers got operations %s and %s", ids[0], ids[i])
		}
	}
	if got := len(ok(f.s.Instances(context.Background(), store.InstanceQuery{})).Items); got != 1 {
		f.t.Fatalf("instances after race = %d, want 1", got)
	}
}

func conflict(f fixture) {
	ctx := context.Background()
	ref := instanceID(1).Ref()
	f.mustCommit("req-a", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)

	_, err := f.commit("req-b", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true, store.Version{Ref: ref, Generation: 0})
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("stale expect: err = %v, want ErrConflict", err)
	}
	_, err = f.commit("req-c", stopIntent(1), instanceSpec(1, 3, compute.PowerStopped), true, store.Version{Ref: ref, Generation: 1})
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("generation skip: err = %v, want ErrConflict", err)
	}
	_, err = f.commit("req-d", startIntent(1), instanceSpec(1, 2, compute.PowerRunning), false)
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("no-op on an unstored generation: err = %v, want ErrConflict", err)
	}

	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, Machine: compute.MachineOn, UpdatedAt: t0})
	_, err = f.commit("req-e", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true, store.Version{Ref: ref, Generation: 1})
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("stale observation: err = %v, want ErrConflict", err)
	}
	f.observeInstance(1, compute.InstanceStatus{
		Observed:  compute.Observed{Generation: 1, Failure: &compute.Failure{Code: compute.CodeInternal, Message: "parked"}},
		UpdatedAt: t0,
	})
	_, err = f.commit("req-f", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true, store.Version{Ref: ref, Generation: 1, Observed: 1})
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("stale parked bit: err = %v, want ErrConflict", err)
	}
	matched := store.Version{Ref: ref, Generation: 1, Observed: 1, Parked: true}
	if op := f.mustCommit("req-g", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true, matched); op.State() != compute.OperationPending {
		f.t.Fatalf("stop on a matching parked version born %s, want Pending", op.State())
	}

	inst := ok(f.s.Instance(ctx, instanceID(1)))
	if inst.Spec.Generation != 2 || inst.Spec.Power != compute.PowerStopped {
		f.t.Fatalf("instance after conflicts = gen %d power %d, want gen 2 stopped", inst.Spec.Generation, inst.Spec.Power)
	}
	for _, req := range []string{"req-b", "req-c", "req-d", "req-e", "req-f"} {
		_, err := f.s.OperationByRequest(ctx, ok(compute.ParseRequestID(req)))
		wantCode(f.t, err, compute.CodeNotFound)
	}
}

func bornSettled(f fixture) {
	run := f.mustCommit("req-1", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	f.wantState(run, compute.OperationPending)
	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, Machine: compute.MachineOn, UpdatedAt: t0.Add(time.Minute)})
	f.wantState(run, compute.OperationSucceeded)
	if got := f.op(run.ID).SettledAt(); !got.Equal(t0.Add(time.Minute)) {
		f.t.Fatalf("settledAt = %v, want observation time", got)
	}

	start := f.mustCommit("req-2", startIntent(1), instanceSpec(1, 1, compute.PowerRunning), false)
	if start.State() != compute.OperationSucceeded {
		f.t.Fatalf("no-op start on converged instance born %s, want Succeeded", start.State())
	}

	stop := f.mustCommit("req-3", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true)
	joined := f.mustCommit("req-4", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), false)
	if stop.State() != compute.OperationPending || joined.State() != compute.OperationPending {
		f.t.Fatalf("stop=%s joined=%s, want both Pending until observed", stop.State(), joined.State())
	}
}

func keyPairs(f fixture) {
	ctx := context.Background()
	create := ok(compute.NewImportKeyPair("deploy", rsa2048, compute.Labels{}))
	del := ok(compute.NewDeleteKeyPair("deploy"))

	first := f.mustCommit("k-1", create, keySpec(1, "deploy", 1, false), true)
	if first.State() != compute.OperationSucceeded {
		f.t.Fatalf("key pair create born %s, want Succeeded", first.State())
	}
	if _, err := f.commit("k-2", create, keySpec(2, "deploy", 1, false), true); !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("second live name: err = %v, want ErrConflict", err)
	}

	gone := f.mustCommit("k-3", del, keySpec(1, "deploy", 2, true), true)
	if gone.State() != compute.OperationSucceeded {
		f.t.Fatalf("delete born %s, want Succeeded", gone.State())
	}
	tomb := ok(f.s.KeyPairByName(ctx, ok(compute.ParseKeyName("deploy"))))
	if tomb.Spec.ID != keyPairID(1) || !tomb.Spec.Deleted || tomb.Spec.Generation != 2 {
		f.t.Fatalf("by name after delete = %+v, want tombstone key 1 gen 2", tomb.Spec)
	}
	if live := ok(f.s.KeyPairs(ctx, store.KeyPairQuery{})); len(live.Items) != 0 {
		f.t.Fatalf("live key pairs after delete = %d, want 0", len(live.Items))
	}
	noop := f.mustCommit("k-4", del, keySpec(1, "deploy", 2, true), false)
	if noop.State() != compute.OperationSucceeded || noop.TargetGeneration != 2 {
		f.t.Fatalf("delete of tombstone = %s gen %d, want Succeeded gen 2", noop.State(), noop.TargetGeneration)
	}

	f.mustCommit("k-5", create, keySpec(2, "deploy", 1, false), true)
	live := ok(f.s.KeyPairByName(ctx, ok(compute.ParseKeyName("deploy"))))
	if live.Spec.ID != keyPairID(2) || live.Spec.Deleted {
		f.t.Fatalf("by name after re-create = %+v, want live key 2", live.Spec)
	}
	_, err := f.s.KeyPairByName(ctx, ok(compute.ParseKeyName("never")))
	wantCode(f.t, err, compute.CodeNotFound)
}

func settles(f fixture) {
	run := f.mustCommit("r-1", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: t0})
	f.wantState(run, compute.OperationSucceeded)

	stop := f.mustCommit("r-2", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true)
	start := f.mustCommit("r-3", startIntent(1), instanceSpec(1, 3, compute.PowerRunning), true)
	f.wantState(stop, compute.OperationSuperseded)
	f.wantState(start, compute.OperationPending)

	boom := &compute.Failure{Code: compute.CodeFailedPrecondition, Message: "image artifact missing"}
	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 3, Failure: boom}, UpdatedAt: t0})
	f.wantState(start, compute.OperationFailed)
	if got := f.op(start.ID).Failure(); got == nil || *got != *boom {
		f.t.Fatalf("failure = %v, want %v", got, boom)
	}
}

func monotonic(f fixture) {
	ctx := context.Background()
	f.mustCommit("m-1", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	f.mustCommit("m-2", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true)
	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 2}, UpdatedAt: t1})

	err := f.s.CommitInstanceStatus(ctx, instanceID(1), compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: t1})
	if !errors.Is(err, store.ErrStale) {
		f.t.Fatalf("older observation: err = %v, want ErrStale", err)
	}
	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 2}, Machine: compute.MachineOff, UpdatedAt: t1})
	err = f.s.CommitInstanceStatus(ctx, instanceID(1), compute.InstanceStatus{Observed: compute.Observed{Generation: 3}, UpdatedAt: t1})
	wantCode(f.t, err, compute.CodeInternal)
	err = f.s.CommitInstanceStatus(ctx, instanceID(1), compute.InstanceStatus{Observed: compute.Observed{Generation: 2}, Machine: compute.MachineOn})
	wantCode(f.t, err, compute.CodeInternal)
	if got := ok(f.s.Instance(ctx, instanceID(1))).Status.Machine; got != compute.MachineOff {
		f.t.Fatalf("machine after rejected observations = %d, want off", got)
	}
	err = f.s.CommitInstanceStatus(ctx, instanceID(9), compute.InstanceStatus{UpdatedAt: t1})
	wantCode(f.t, err, compute.CodeNotFound)

	f.mustCommit("m-3", ok(compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")), imageSpec(1, 1), true)
	err = f.s.CommitImageStatus(ctx, imageID(1), compute.ImageStatus{Observed: compute.Observed{Generation: 2}, UpdatedAt: t1})
	wantCode(f.t, err, compute.CodeInternal)
	err = f.s.CommitImageStatus(ctx, imageID(1), compute.ImageStatus{Observed: compute.Observed{Generation: 1}, Present: true})
	wantCode(f.t, err, compute.CodeInternal)
	err = f.s.CommitImageStatus(ctx, imageID(9), compute.ImageStatus{UpdatedAt: t1})
	wantCode(f.t, err, compute.CodeNotFound)
}

func unconverged(f fixture) {
	ctx := context.Background()
	for n := 1; n <= 4; n++ {
		f.mustCommit(fmt.Sprintf("u-%d", n), runIntent("vws.small"), instanceSpec(n, 1, compute.PowerRunning), true)
	}
	f.observeInstance(2, compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: t1})
	f.observeInstance(3, compute.InstanceStatus{Observed: compute.Observed{Generation: 1, Failure: &compute.Failure{Code: compute.CodeInternal, Message: "parked"}}, UpdatedAt: t1})
	f.observeInstance(4, compute.InstanceStatus{Retry: compute.Retry{Attempts: 1, At: t0.Add(10 * time.Second), LastError: "busy"}, UpdatedAt: t0})
	f.mustCommit("u-img", ok(compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")), imageSpec(1, 1), true)
	f.mustCommit("u-key", ok(compute.NewImportKeyPair("k", rsa2048, compute.Labels{})), keySpec(1, "k", 1, false), true)

	want := func(at time.Time, refs ...compute.Ref) {
		f.t.Helper()
		got := ok(f.s.Unconverged(ctx, at))
		if len(got) == 0 && len(refs) == 0 {
			return
		}
		if !reflect.DeepEqual(got, refs) {
			f.t.Fatalf("Unconverged(%v) = %v, want %v", at.Sub(t0), got, refs)
		}
	}
	want(t0, instanceID(1).Ref(), imageID(1).Ref())
	want(t0.Add(10*time.Second), instanceID(1).Ref(), instanceID(4).Ref(), imageID(1).Ref())
	if got := ok(f.s.Lagging(ctx)); !reflect.DeepEqual(got, []compute.Ref{instanceID(1).Ref(), instanceID(4).Ref(), imageID(1).Ref()}) {
		f.t.Fatalf("Lagging = %v, want the backed-off instance included", got)
	}

	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: t1})
	f.observeInstance(4, compute.InstanceStatus{Observed: compute.Observed{Generation: 1}, UpdatedAt: t1})
	if err := f.s.CommitImageStatus(ctx, imageID(1), compute.ImageStatus{Observed: compute.Observed{Generation: 1}, Present: true, UpdatedAt: t1}); err != nil {
		f.t.Fatal(err)
	}
	want(t0.Add(time.Hour))
}

func roundTrip(f fixture) {
	ctx := context.Background()
	inst := instanceSpec(1, 1, compute.PowerRunning)
	inst.Type = ok(compute.ParseInstanceType("vws.medium"))
	inst.Key = &compute.AuthorizedKey{Name: ok(compute.ParseKeyName("deploy")), PublicKey: ok(compute.ParsePublicKey(rsa2048))}
	op := f.mustCommit("rt-1", runIntent("vws.medium"), inst, true)
	st := compute.InstanceStatus{
		Observed:    compute.Observed{Generation: 0},
		Machine:     compute.MachineOff,
		RebootEpoch: 2,
		Retry:       compute.Retry{Attempts: 3, At: t0.Add(8 * time.Second), LastError: "agent unreachable"},
		UpdatedAt:   t0.Add(time.Second),
	}
	f.observeInstance(1, st)
	if got, want := ok(f.s.Instance(ctx, instanceID(1))), (compute.Instance{Spec: inst, Status: st}); !reflect.DeepEqual(got, want) {
		f.t.Fatalf("instance round trip:\n got %+v\nwant %+v", got, want)
	}

	img := imageSpec(2, 1)
	img.Source = compute.FromInstance{Instance: instanceID(1)}
	f.mustCommit("rt-2", ok(compute.NewCreateImage(instanceID(1).String(), "snap-1")), img, true)
	ist := compute.ImageStatus{Observed: compute.Observed{Generation: 1}, Present: true, SizeBytes: 10 << 30, UpdatedAt: t0.Add(time.Minute)}
	if err := f.s.CommitImageStatus(ctx, imageID(2), ist); err != nil {
		f.t.Fatal(err)
	}
	if got, want := ok(f.s.Image(ctx, imageID(2))), (compute.Image{Spec: img, Status: ist}); !reflect.DeepEqual(got, want) {
		f.t.Fatalf("image round trip:\n got %+v\nwant %+v", got, want)
	}

	kp := keySpec(3, "deploy", 1, false)
	f.mustCommit("rt-3", ok(compute.NewImportKeyPair("deploy", rsa2048, compute.Labels{})), kp, true)
	if got := ok(f.s.KeyPair(ctx, keyPairID(3))); !reflect.DeepEqual(got.Spec, kp) {
		f.t.Fatalf("key pair round trip:\n got %+v\nwant %+v", got.Spec, kp)
	}

	got := f.op(op.ID)
	if got.Request != op.Request || got.Fingerprint != op.Fingerprint || got.Kind != compute.OpRunInstance ||
		got.Target != instanceID(1).Ref() || got.TargetGeneration != 1 || !got.CreatedAt.Equal(t0) || got.State() != compute.OperationPending {
		f.t.Fatalf("operation round trip: %+v", got)
	}
	_, err := f.s.Operation(ctx, ok(compute.ParseOperationID("op-00000000000000000000")))
	wantCode(f.t, err, compute.CodeNotFound)
}

func aliasing(f fixture) {
	ctx := context.Background()
	inst := instanceSpec(1, 1, compute.PowerRunning)
	inst.Key = &compute.AuthorizedKey{Name: ok(compute.ParseKeyName("deploy")), PublicKey: ok(compute.ParsePublicKey(rsa2048))}
	op := f.mustCommit("a-1", runIntent("vws.small"), inst, true)
	f.observeInstance(1, compute.InstanceStatus{Observed: compute.Observed{Generation: 1, Failure: &compute.Failure{Code: compute.CodeInternal, Message: "parked"}}, UpdatedAt: t1})

	got := ok(f.s.Instance(ctx, instanceID(1)))
	got.Spec.Key.Name = ok(compute.ParseKeyName("mutated"))
	got.Status.Observed.Failure.Message = "mutated"
	f.op(op.ID).Failure().Message = "mutated"

	again := ok(f.s.Instance(ctx, instanceID(1)))
	if again.Spec.Key.Name.String() != "deploy" || again.Status.Observed.Failure.Message != "parked" {
		f.t.Fatalf("store state changed through a returned value: key=%s failure=%s", again.Spec.Key.Name, again.Status.Observed.Failure.Message)
	}
	if msg := f.op(op.ID).Failure().Message; msg != "parked" {
		f.t.Fatalf("operation failure changed through a returned value: %s", msg)
	}
}

func paging(f fixture) {
	ctx := context.Background()
	for n := 1; n <= 3; n++ {
		f.mustCommit(fmt.Sprintf("p-%d", n), runIntent("vws.small"), instanceSpec(n, 1, compute.PowerRunning), true)
		f.mustCommit(fmt.Sprintf("pi-%d", n), ok(compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")), imageSpec(n, 1), true)
		f.mustCommit(fmt.Sprintf("pk-%d", n), ok(compute.NewImportKeyPair(fmt.Sprintf("key-%d", n), rsa2048, compute.Labels{})), keySpec(n, fmt.Sprintf("key-%d", n), 1, false), true)
	}
	p1 := ok(f.s.Instances(ctx, store.InstanceQuery{PageSize: 2}))
	p2 := ok(f.s.Instances(ctx, store.InstanceQuery{PageSize: 2, After: p1.Next}))
	if len(p1.Items) != 2 || p1.Next != instanceID(2).String() || len(p2.Items) != 1 || p2.Items[0].Spec.ID != instanceID(3) || p2.Next != "" {
		f.t.Fatalf("instance pages = %d items next %q, then %d items next %q", len(p1.Items), p1.Next, len(p2.Items), p2.Next)
	}
	sel := ok(f.s.Instances(ctx, store.InstanceQuery{IDs: []compute.InstanceID{instanceID(3), instanceID(1), instanceID(7)}}))
	if len(sel.Items) != 2 || sel.Items[0].Spec.ID != instanceID(1) || sel.Items[1].Spec.ID != instanceID(3) {
		f.t.Fatalf("instance filter returned %d items", len(sel.Items))
	}
	imgs := ok(f.s.Images(ctx, store.ImageQuery{IDs: []compute.ImageID{imageID(2)}}))
	if len(imgs.Items) != 1 || imgs.Items[0].Spec.ID != imageID(2) {
		f.t.Fatalf("image filter returned %d items", len(imgs.Items))
	}
	keys := ok(f.s.KeyPairs(ctx, store.KeyPairQuery{PageSize: 2, After: "key-1"}))
	if len(keys.Items) != 2 || keys.Items[0].Spec.Name.String() != "key-2" || keys.Next != "" {
		f.t.Fatalf("key pair page after key-1 = %d items next %q", len(keys.Items), keys.Next)
	}
}

func sampleLabels() compute.Labels {
	return ok(compute.ParseLabels(map[string]string{"env": "dev", "vws-fingerprint": "fp-1"}))
}

func (f fixture) commitChange(req string, in compute.Intent, ch compute.Change, expect ...store.Version) (compute.Operation, error) {
	f.t.Helper()
	op, err := compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, ok(compute.ParseRequestID(req)), in, ch)
	if err != nil {
		f.t.Fatal(err)
	}
	return f.s.CommitDesired(context.Background(), store.DesiredChange{
		Op: op, Spec: ch.Spec, Write: ch.Write, Meta: ch.Meta, Expect: expect,
	})
}

func (f fixture) mustChange(req string, in compute.Intent, ch compute.Change, expect ...store.Version) compute.Operation {
	f.t.Helper()
	op, err := f.commitChange(req, in, ch, expect...)
	if err != nil {
		f.t.Fatalf("commit %s: %v", req, err)
	}
	return op
}

func metadataRoundTrip(f fixture) {
	ctx := context.Background()
	labels := sampleLabels()
	desc := ok(compute.ParseDescription("debian\nstable\tbuild"))

	inst := instanceSpec(1, 1, compute.PowerRunning)
	born := &compute.Metadata{Labels: labels}
	f.mustChange("md-i", runIntent("vws.small"), compute.Change{Spec: inst, Write: true, Meta: born})
	got := ok(f.s.Instance(ctx, instanceID(1)))
	if !reflect.DeepEqual(got.Meta, *born) || !got.Spec.ChangedAt.Equal(inst.ChangedAt) {
		f.t.Fatalf("instance metadata = %+v changed %v, want %+v", got.Meta, got.Spec.ChangedAt, *born)
	}
	mut := got.Meta.Labels.Map()
	mut["env"] = "mutated"
	mut["extra"] = "nope"
	again := ok(f.s.Instance(ctx, instanceID(1)))
	if !again.Meta.Labels.Equal(labels) {
		f.t.Fatalf("mutating Map() changed stored labels to %v", again.Meta.Labels.Map())
	}

	plain := instanceSpec(2, 1, compute.PowerRunning)
	f.mustCommit("md-i0", runIntent("vws.small"), plain, true)
	if meta := ok(f.s.Instance(ctx, instanceID(2))).Meta; !reflect.DeepEqual(meta, compute.Metadata{}) || meta.Labels.Map() != nil {
		f.t.Fatalf("unset labels = %+v map %v, want the zero set", meta, meta.Labels.Map())
	}

	img := imageSpec(1, 1)
	imgMeta := &compute.Metadata{Labels: labels, Description: desc}
	f.mustChange("md-img", ok(compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")), compute.Change{Spec: img, Write: true, Meta: imgMeta})
	if got, want := ok(f.s.Image(ctx, imageID(1))).Meta, *imgMeta; !reflect.DeepEqual(got, want) {
		f.t.Fatalf("image metadata = %+v, want %+v", got, want)
	}

	kp := keySpec(1, "deploy", 1, false)
	kpMeta := &compute.Metadata{Labels: labels}
	f.mustChange("md-k", ok(compute.NewImportKeyPair("deploy", rsa2048, labels)), compute.Change{Spec: kp, Write: true, Meta: kpMeta})
	gotKey := ok(f.s.KeyPair(ctx, keyPairID(1)))
	if !reflect.DeepEqual(gotKey.Meta, *kpMeta) || gotKey.Meta.Description.String() != "" {
		f.t.Fatalf("key pair metadata = %+v, want labels only", gotKey.Meta)
	}
}

func generationLeavesMetadata(f fixture) {
	ctx := context.Background()
	labels := sampleLabels()
	meta := &compute.Metadata{Labels: labels}
	inst := instanceSpec(1, 1, compute.PowerRunning)
	f.mustChange("gen-1", runIntent("vws.small"), compute.Change{Spec: inst, Write: true, Meta: meta})

	stopped := instanceSpec(1, 2, compute.PowerStopped)
	f.mustCommit("gen-2", stopIntent(1), stopped, true)
	got := ok(f.s.Instance(ctx, instanceID(1)))
	if !got.Meta.Labels.Equal(labels) || got.Meta.Revision != 0 || !got.Meta.ChangedAt.IsZero() {
		f.t.Fatalf("generation write clobbered metadata: %+v", got.Meta)
	}
	if got.Spec.Generation != 2 || !got.Spec.ChangedAt.Equal(stopped.ChangedAt) {
		f.t.Fatalf("spec after stop = gen %d changed %v", got.Spec.Generation, got.Spec.ChangedAt)
	}
	refs := ok(f.s.Unconverged(ctx, t0))
	if !reflect.DeepEqual(refs, []compute.Ref{instanceID(1).Ref()}) {
		f.t.Fatalf("Unconverged = %v, want the stopped instance", refs)
	}
}

func metadataLeavesSiblings(f fixture) {
	ctx := context.Background()
	f.mustCommit("sib-1", runIntent("vws.small"), instanceSpec(1, 1, compute.PowerRunning), true)
	stop := f.mustCommit("sib-2", stopIntent(1), instanceSpec(1, 2, compute.PowerStopped), true)
	before := ok(f.s.Instance(ctx, instanceID(1)))

	labels := sampleLabels()
	patch := compute.MetadataPatch{}.ReplaceLabels(labels)
	meta := &compute.Metadata{Labels: labels, Revision: 1, ChangedAt: t1}
	op := f.mustChange("sib-3", ok(compute.NewUpdateInstanceMetadata(instanceID(1).String(), patch)), compute.Change{
		Spec: before.Spec, Meta: meta,
	})
	if op.State() != compute.OperationSucceeded || !op.MetadataOnly {
		f.t.Fatalf("label update born %s metadata=%v, want Succeeded MetadataOnly", op.State(), op.MetadataOnly)
	}
	f.wantState(stop, compute.OperationPending)
	got := ok(f.s.Instance(ctx, instanceID(1)))
	if got.Spec.Generation != before.Spec.Generation || !got.Spec.ChangedAt.Equal(before.Spec.ChangedAt) {
		f.t.Fatalf("spec moved on a label write: gen %d changed %v", got.Spec.Generation, got.Spec.ChangedAt)
	}
	if !reflect.DeepEqual(got.Meta, *meta) {
		f.t.Fatalf("metadata = %+v, want %+v", got.Meta, *meta)
	}
	refs := ok(f.s.Unconverged(ctx, t0))
	if !reflect.DeepEqual(refs, []compute.Ref{instanceID(1).Ref()}) {
		f.t.Fatalf("Unconverged = %v, want the instance still due", refs)
	}
}

func staleMetadata(f fixture) {
	ctx := context.Background()
	labels := sampleLabels()
	inst := instanceSpec(1, 1, compute.PowerRunning)
	f.mustChange("st-1", runIntent("vws.small"), compute.Change{Spec: inst, Write: true, Meta: &compute.Metadata{Labels: labels}})

	patch := compute.MetadataPatch{}.ReplaceLabels(sampleLabels())
	in := ok(compute.NewUpdateInstanceMetadata(instanceID(1).String(), patch))
	_, err := f.commitChange("st-skip", in, compute.Change{Spec: inst, Meta: &compute.Metadata{Labels: labels, Revision: 3, ChangedAt: t1}})
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("skipped revision: err = %v, want ErrConflict", err)
	}
	_, err = f.commitChange("st-repeat", in, compute.Change{Spec: inst, Meta: &compute.Metadata{Labels: labels}})
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("repeated revision: err = %v, want ErrConflict", err)
	}
	_, err = f.commitChange("st-bad", in, compute.Change{Spec: inst, Meta: &compute.Metadata{Revision: 0, ChangedAt: t1}})
	wantCode(f.t, err, compute.CodeInternal)

	other := instanceSpec(2, 1, compute.PowerRunning)
	_, err = f.commitChange("st-create", runIntent("vws.small"), compute.Change{
		Spec: other, Write: true, Meta: &compute.Metadata{Labels: labels, Revision: 1, ChangedAt: t1},
	})
	wantCode(f.t, err, compute.CodeInternal)

	got := ok(f.s.Instance(ctx, instanceID(1)))
	if !got.Meta.Labels.Equal(labels) || got.Meta.Revision != 0 {
		f.t.Fatalf("rejected metadata writes changed the row: %+v", got.Meta)
	}
	_, err = f.s.Instance(ctx, instanceID(2))
	wantCode(f.t, err, compute.CodeNotFound)
	for _, req := range []string{"st-skip", "st-repeat", "st-bad", "st-create"} {
		_, err := f.s.OperationByRequest(ctx, ok(compute.ParseRequestID(req)))
		wantCode(f.t, err, compute.CodeNotFound)
	}
}

func createOverExistingImage(f fixture) {
	ctx := context.Background()
	f.mustCommit("img-a", ok(compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")), imageSpec(1, 1), true)
	again := imageSpec(1, 1)
	again.Source = compute.FromFile{Path: ok(compute.ParseImagePath("/var/lib/vws/images/debian-13.qcow2"))}
	_, err := f.commit("img-b", ok(compute.NewRegisterFile(imageID(1).String(), "/var/lib/vws/images/debian-13.qcow2", "debian-13")), again, true)
	if !errors.Is(err, store.ErrConflict) {
		f.t.Fatalf("second generation-1 image: err = %v, want ErrConflict", err)
	}
	_, err = f.s.OperationByRequest(ctx, ok(compute.ParseRequestID("img-b")))
	wantCode(f.t, err, compute.CodeNotFound)
	got := ok(f.s.Image(ctx, imageID(1)))
	if _, ok := got.Spec.Source.(compute.FromURL); !ok {
		f.t.Fatalf("source after conflict = %#v, want FromURL", got.Spec.Source)
	}
}

func imageSourcesRoundTrip(f fixture) {
	ctx := context.Background()
	fileSpec := imageSpec(4, 1)
	fileSpec.Source = compute.FromFile{Path: ok(compute.ParseImagePath("/var/lib/vws/images/debian-12.qcow2"))}
	vol := ok(compute.ParseVolumeRef("projects/demo/zones/demo/volumes/root"))
	snap := ok(compute.ParseSnapshotRef("projects/demo/regions/demo/snapshots/snap"))
	volSpec := imageSpec(5, 1)
	volSpec.Name = compute.ImageName{}
	volSpec.Source = compute.FromVolume{Ref: vol}
	snapSpec := imageSpec(6, 1)
	snapSpec.Source = compute.FromSnapshot{Ref: snap}
	f.mustCommit("src-file", ok(compute.NewRegisterFile(imageID(4).String(), "/var/lib/vws/images/debian-12.qcow2", "debian-13")), fileSpec, true)
	f.mustCommit("src-vol", ok(compute.NewCopyImage(imageID(5).String(), compute.FromVolume{Ref: vol}, "", "", compute.Labels{})), volSpec, true)
	f.mustCommit("src-snap", ok(compute.NewCopyImage(imageID(6).String(), compute.FromSnapshot{Ref: snap}, "debian-13", "", compute.Labels{})), snapSpec, true)
	for _, spec := range []compute.ImageSpec{fileSpec, volSpec, snapSpec} {
		got := ok(f.s.Image(ctx, spec.ID))
		if !reflect.DeepEqual(got.Spec, spec) {
			f.t.Fatalf("source round trip:\n got %+v\nwant %+v", got.Spec, spec)
		}
	}
}
