package kubestore_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

var t0 = time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

// ok unwraps a constructor result. A failure is a bug in the test.
func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func runSpec(_ *testing.T, n int, gen int64) compute.InstanceSpec {
	return compute.InstanceSpec{
		ID:         ok(compute.ParseInstanceID(fmt.Sprintf("i-%017x", n))),
		Generation: gen, CreatedAt: t0, ChangedAt: t0,
		Image: ok(compute.ParseImageID("img-00000000000000001")),
		Type:  ok(compute.ParseInstanceType("vws.small")),
		Power: compute.PowerRunning,
	}
}

func change(t *testing.T, req string, spec compute.InstanceSpec, expect ...store.Version) store.DesiredChange {
	t.Helper()
	in := ok(compute.NewRunInstance("img-00000000000000001", "vws.small", ""))
	op := ok(compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, ok(compute.ParseRequestID(req)), in,
		compute.Change{Spec: spec, Write: true}))
	return store.DesiredChange{Op: op, Spec: spec, Write: true, Expect: expect}
}

func configMaps(t *testing.T, c client.Client, ns, kind string) []corev1.ConfigMap {
	t.Helper()
	var l corev1.ConfigMapList
	if err := c.List(context.Background(), &l, client.InNamespace(ns), client.MatchingLabels{"vws.io/kind": kind}); err != nil {
		t.Fatal(err)
	}
	return l.Items
}

// hook wraps c so tests can run code before a write reaches the apiserver.
func hook(c client.WithWatch, f interceptor.Funcs) client.Client {
	raw := c
	return interceptor.NewClient(raw, f)
}

func TestSameRequestIDSameOperationAcrossRestart(t *testing.T) {
	c, ns := kubetest.Namespace(t)
	ctx := context.Background()
	first := kubestore.New(c, ns)
	spec := runSpec(t, 1, 1)
	got, err := first.CommitDesired(ctx, change(t, "token-1", spec))
	if err != nil {
		t.Fatal(err)
	}
	// A restart is a new Store over the same cluster.
	second := kubestore.New(c, ns)
	if _, err := second.Repair(ctx); err != nil {
		t.Fatal(err)
	}
	again, err := second.CommitDesired(ctx, change(t, "token-1", spec))
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != got.ID {
		t.Fatalf("operation after restart = %s, want %s", again.ID, got.ID)
	}
	byReq, err := second.OperationByRequest(ctx, got.Request)
	if err != nil || byReq.ID != got.ID {
		t.Fatalf("OperationByRequest = %v, %v", byReq.ID, err)
	}
	if n := len(configMaps(t, c, ns, "instance")); n != 1 {
		t.Fatalf("instance objects = %d, want 1", n)
	}
}

func TestMismatchedReplayAcrossRestartIsRejected(t *testing.T) {
	c, ns := kubetest.Namespace(t)
	ctx := context.Background()
	if _, err := kubestore.New(c, ns).CommitDesired(ctx, change(t, "token-1", runSpec(t, 1, 1))); err != nil {
		t.Fatal(err)
	}
	other := change(t, "token-1", runSpec(t, 1, 1))
	other.Op.Fingerprint[0] ^= 1
	_, err := kubestore.New(c, ns).CommitDesired(ctx, other)
	if compute.CodeOf(err) != compute.CodeRequestMismatch {
		t.Fatalf("err = %v, want RequestMismatch", err)
	}
}

// A writer that lands between our read and our update makes the update fail
// with a conflict. The store rereads and applies the rule again.
func TestCASConflictRetriesOnFreshState(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	seed := kubestore.New(raw, ns)
	spec := runSpec(t, 1, 1)
	if _, err := seed.CommitDesired(ctx, change(t, "r-1", spec)); err != nil {
		t.Fatal(err)
	}

	var updates int
	var once sync.Once
	c := hook(raw, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
		updates++
		once.Do(func() {
			// A competing status commit through another store.
			rival := kubestore.New(raw, ns)
			err := rival.CommitInstanceStatus(ctx, spec.ID, compute.InstanceStatus{
				Observed: compute.Observed{Generation: 1}, Machine: compute.MachineOn, UpdatedAt: t0.Add(time.Second),
			})
			if err != nil {
				t.Error(err)
			}
		})
		return c.Update(ctx, o, opts...)
	}})
	s := kubestore.New(c, ns)
	err := s.CommitInstanceStatus(ctx, spec.ID, compute.InstanceStatus{
		Observed: compute.Observed{Generation: 1}, Machine: compute.MachineOff, UpdatedAt: t0.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if updates != 2 {
		t.Fatalf("update attempts = %d, want 2 (one conflict, one success)", updates)
	}
	got, _ := s.Instance(ctx, spec.ID)
	if got.Status.Machine != compute.MachineOff {
		t.Fatalf("machine = %d, want the later commit to win", got.Status.Machine)
	}
}

// A plan read before a competing commit must not land after it, and a
// refused commit leaves no request index behind.
func TestStaleExpectLosesToACompetingCommit(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	seed := kubestore.New(raw, ns)
	spec := runSpec(t, 1, 1)
	if _, err := seed.CommitDesired(ctx, change(t, "r-1", spec)); err != nil {
		t.Fatal(err)
	}
	read := store.Version{Ref: spec.ID.Ref(), Generation: 1}

	stop := runSpec(t, 1, 2)
	stop.Power = compute.PowerStopped
	var once sync.Once
	c := hook(raw, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
		once.Do(func() {
			rival := kubestore.New(raw, ns)
			if _, err := rival.CommitDesired(ctx, change(t, "r-rival", stop, read)); err != nil {
				t.Error(err)
			}
		})
		return c.Update(ctx, o, opts...)
	}})
	next := runSpec(t, 1, 2)
	next.Power = compute.PowerTerminated
	_, err := kubestore.New(c, ns).CommitDesired(ctx, change(t, "r-2", next, read))
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	got, _ := seed.Instance(ctx, spec.ID)
	if got.Spec.Generation != 2 || got.Spec.Power != compute.PowerStopped {
		t.Fatalf("stored generation %d power %d, want the rival's commit", got.Spec.Generation, got.Spec.Power)
	}
	if _, err := seed.OperationByRequest(ctx, ok(compute.ParseRequestID("r-2"))); compute.CodeOf(err) != compute.CodeNotFound {
		t.Fatalf("refused request has an operation: %v", err)
	}
	for _, cm := range configMaps(t, raw, ns, "request") {
		if cm.Data["request"] == "r-2" {
			t.Fatal("refused request left an index")
		}
	}
}

// crash simulates a process that dies right after it claimed the index: the
// resource write fails and so does every later call.
func crash(raw client.WithWatch) client.Client {
	var dead bool
	boom := errors.New("process died")
	return hook(raw, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
			if dead || strings.HasPrefix(o.GetName(), "vws-instance-") {
				dead = true
				return boom
			}
			return c.Create(ctx, o, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
			if dead {
				return boom
			}
			return c.Update(ctx, o, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.DeleteOption) error {
			if dead {
				return boom
			}
			return c.Delete(ctx, o, opts...)
		},
	})
}

func TestRepairRemovesAnOrphanedIndexAndTheRetrySucceeds(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	spec := runSpec(t, 1, 1)
	if _, err := kubestore.New(crash(raw), ns).CommitDesired(ctx, change(t, "r-1", spec)); err == nil {
		t.Fatal("commit survived the crash")
	}
	if n := len(configMaps(t, raw, ns, "request")); n != 1 {
		t.Fatalf("indexes after crash = %d, want 1 orphan", n)
	}
	if n := len(configMaps(t, raw, ns, "instance")); n != 0 {
		t.Fatalf("instances after crash = %d, want 0", n)
	}

	s := kubestore.New(raw, ns)
	r, err := s.Repair(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.OrphanIndexes != 1 {
		t.Fatalf("repair = %+v, want one orphan index removed", r)
	}
	if n := len(configMaps(t, raw, ns, "request")); n != 0 {
		t.Fatalf("indexes after repair = %d, want 0", n)
	}
	again, err := s.Repair(ctx)
	if err != nil || again != (kubestore.Repaired{}) {
		t.Fatalf("second repair = %+v, %v, want nothing to do", again, err)
	}
	op, err := s.CommitDesired(ctx, change(t, "r-1", spec))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Operation(ctx, op.ID); err != nil || got.Request != op.Request {
		t.Fatalf("operation by id = %+v", got)
	}
}

// Without a repair pass, a retry with the same request id takes over the
// orphan and finishes the write.
func TestRetryAdoptsAnOrphanedIndex(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	spec := runSpec(t, 1, 1)
	if _, err := kubestore.New(crash(raw), ns).CommitDesired(ctx, change(t, "r-1", spec)); err == nil {
		t.Fatal("commit survived the crash")
	}
	s := kubestore.New(raw, ns)
	retry := change(t, "r-1", spec)
	op, err := s.CommitDesired(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if op.ID != retry.Op.ID {
		t.Fatalf("operation = %s, want the retry's %s", op.ID, retry.Op.ID)
	}
	byID, err := s.Operation(ctx, op.ID)
	if err != nil || byID.ID != op.ID {
		t.Fatalf("Operation(%s) = %v, %v", op.ID, byID.ID, err)
	}
	if n := len(configMaps(t, raw, ns, "request")); n != 1 {
		t.Fatalf("indexes = %d, want 1", n)
	}
}

func TestRepairRestoresAMissingIndex(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	s := kubestore.New(raw, ns)
	op, err := s.CommitDesired(ctx, change(t, "r-1", runSpec(t, 1, 1)))
	if err != nil {
		t.Fatal(err)
	}
	for _, cm := range configMaps(t, raw, ns, "request") {
		if err := raw.Delete(ctx, &cm); err != nil {
			t.Fatal(err)
		}
	}
	r, err := s.Repair(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.RestoredIndexes != 1 || r.Pending != 1 {
		t.Fatalf("repair = %+v, want one restored index and one pending operation", r)
	}
	got, err := s.OperationByRequest(ctx, op.Request)
	if err != nil || got.ID != op.ID {
		t.Fatalf("OperationByRequest = %v, %v", got.ID, err)
	}
	if _, err := s.CommitDesired(ctx, change(t, "r-1", runSpec(t, 1, 1))); err != nil {
		t.Fatal(err)
	}
	if n := len(configMaps(t, raw, ns, "instance")); n != 1 {
		t.Fatalf("replay made %d instance objects, want 1", n)
	}
}

func TestRepairReleasesAClaimWhoseHolderNeverWasWritten(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	key := compute.KeyPairSpec{
		ID:         ok(compute.ParseKeyPairID("key-00000000000000001")),
		Generation: 1, CreatedAt: t0, ChangedAt: t0,
		Name:      ok(compute.ParseKeyName("deploy")),
		PublicKey: ok(compute.ParsePublicKey("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl")),
	}
	in := ok(compute.NewCreateKeyPair("deploy"))
	op := ok(compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, ok(compute.ParseRequestID("k-1")), in, compute.Change{Spec: key, Write: true}))
	dc := store.DesiredChange{Op: op, Spec: key, Write: true}

	// The crash hits the key pair's own create, after the name claim.
	dead := false
	boom := errors.New("process died")
	c := hook(raw, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
			if dead || strings.HasPrefix(o.GetName(), "vws-keypair-") {
				dead = true
				return boom
			}
			return c.Create(ctx, o, opts...)
		},
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return boom },
	})
	if _, err := kubestore.New(c, ns).CommitDesired(ctx, dc); err == nil {
		t.Fatal("commit survived the crash")
	}
	if n := len(configMaps(t, raw, ns, "claim-keyname")); n != 1 {
		t.Fatalf("claims after crash = %d, want 1", n)
	}
	s := kubestore.New(raw, ns)
	r, err := s.Repair(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.StaleClaims != 1 || r.OrphanIndexes != 1 {
		t.Fatalf("repair = %+v, want one stale claim and one orphan index", r)
	}
	other := key
	other.ID = ok(compute.ParseKeyPairID("key-00000000000000002"))
	op2 := ok(compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, ok(compute.ParseRequestID("k-2")), in, compute.Change{Spec: other, Write: true}))
	if _, err := s.CommitDesired(ctx, store.DesiredChange{Op: op2, Spec: other, Write: true}); err != nil {
		t.Fatalf("name is still blocked after repair: %v", err)
	}
}
