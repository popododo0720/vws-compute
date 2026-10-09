package controller_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

var t0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func req(s string) compute.RequestID { return ok(compute.ParseRequestID(s)) }

func newController(s store.DesiredStore) *controller.Controller {
	return controller.New(s, controller.WithClock(func() time.Time { return t0 }))
}

func submit(t *testing.T, c *controller.Controller, id string, in compute.Intent) controller.Receipt {
	t.Helper()
	r, err := c.Submit(context.Background(), req(id), in)
	if err != nil {
		t.Fatalf("Submit %s: %v", id, err)
	}
	return r
}

func wantCode(t *testing.T, err error, want compute.Code) {
	t.Helper()
	if err == nil || compute.CodeOf(err) != want {
		t.Fatalf("err = %v, want code %s", err, want)
	}
}

// availableImage registers an image and records it as present, standing in
// for a reconciler.
func availableImage(t *testing.T, repo store.Repository, c *controller.Controller) compute.ImageID {
	t.Helper()
	r := submit(t, c, "image", ok(compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2")))
	id := ok(compute.ParseImageID(r.Operation.Target.ID))
	st := compute.ImageStatus{Observed: compute.Observed{Generation: 1}, Present: true, SizeBytes: 1, UpdatedAt: t0}
	if err := repo.CommitImageStatus(context.Background(), id, st); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSameRequestTwiceReturnsStoredOperationAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	c := newController(repo)
	img := availableImage(t, repo, c)
	run := ok(compute.NewRunInstance(img.String(), "vws.small", ""))

	first := submit(t, c, "run-1", run)
	again := submit(t, c, "run-1", run)
	if again.Operation.ID != first.Operation.ID || again.Operation.Target != first.Operation.Target {
		t.Fatalf("replay = %s on %s, want %s on %s", again.Operation.ID, again.Operation.Target, first.Operation.ID, first.Operation.Target)
	}
	if n := len(ok(repo.Instances(ctx, store.InstanceQuery{})).Items); n != 1 {
		t.Fatalf("instances after replay = %d, want 1", n)
	}
	if again.Operation.State() != compute.OperationPending {
		t.Fatalf("replayed operation is %s, want Pending", again.Operation.State())
	}
}

func TestReusedRequestIDWithDifferentIntentMismatches(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	c := newController(repo)
	img := availableImage(t, repo, c)

	first := submit(t, c, "run-1", ok(compute.NewRunInstance(img.String(), "vws.small", "")))
	for _, in := range []compute.Intent{
		ok(compute.NewRunInstance(img.String(), "vws.medium", "")),
		ok(compute.NewStopInstance(first.Operation.Target.ID)),
		ok(compute.NewCreateKeyPair("deploy")),
	} {
		_, err := c.Submit(ctx, req("run-1"), in)
		wantCode(t, err, compute.CodeRequestMismatch)
	}
	if n := len(ok(repo.Instances(ctx, store.InstanceQuery{})).Items); n != 1 {
		t.Fatalf("instances = %d, want 1", n)
	}
	if n := len(ok(repo.KeyPairs(ctx, store.KeyPairQuery{})).Items); n != 0 {
		t.Fatalf("key pairs = %d, want 0", n)
	}
}

func TestCreateKeyPairReturnsPrivateKeyOnlyOnInsert(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	c := newController(repo)
	create := ok(compute.NewCreateKeyPair("deploy"))

	first := submit(t, c, "key-1", create)
	if first.PrivateKey == nil || first.Operation.State() != compute.OperationSucceeded {
		t.Fatalf("create = key %v state %s, want a key and Succeeded", first.PrivateKey != nil, first.Operation.State())
	}
	signer, err := ssh.ParsePrivateKey(first.PrivateKey.OpenSSH())
	if err != nil {
		t.Fatalf("private key does not parse as OpenSSH: %v", err)
	}
	stored := ok(repo.KeyPairByName(ctx, ok(compute.ParseKeyName("deploy"))))
	if got := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))); got != stored.Spec.PublicKey.AuthorizedKey() {
		t.Fatalf("private key's public half %q does not match stored %q", got, stored.Spec.PublicKey.AuthorizedKey())
	}
	if !strings.HasPrefix(stored.Spec.PublicKey.Fingerprint(), "SHA256:") {
		t.Fatalf("fingerprint %q", stored.Spec.PublicKey.Fingerprint())
	}

	again := submit(t, c, "key-1", create)
	if again.Operation.ID != first.Operation.ID || again.PrivateKey != nil {
		t.Fatalf("replay = op %s key %v, want op %s and no key", again.Operation.ID, again.PrivateKey != nil, first.Operation.ID)
	}

	_, err = c.Submit(ctx, req("key-2"), create)
	wantCode(t, err, compute.CodeAlreadyExists)
}

func TestConcurrentCreateKeyPairRetriesShareOneOperationAndOneKey(t *testing.T) {
	repo := memstore.New()
	c := newController(repo)
	create := ok(compute.NewCreateKeyPair("deploy"))
	const n = 8
	receipts := make([]controller.Receipt, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			receipts[i], errs[i] = c.Submit(context.Background(), req("same"), create)
		}(i)
	}
	wg.Wait()
	keys := 0
	for i, r := range receipts {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if r.Operation.ID != receipts[0].Operation.ID {
			t.Fatalf("callers got operations %s and %s", receipts[0].Operation.ID, r.Operation.ID)
		}
		if r.PrivateKey != nil {
			keys++
		}
	}
	if keys != 1 {
		t.Fatalf("%d callers received the private key, want exactly 1", keys)
	}
}

// lookupRacesCommit reports the first request lookup as missing, as when a
// concurrent call with the same request commits just after the lookup.
type lookupRacesCommit struct {
	store.DesiredStore
	raced bool
}

func (s *lookupRacesCommit) OperationByRequest(ctx context.Context, r compute.RequestID) (compute.Operation, error) {
	if !s.raced {
		s.raced = true
		return compute.Operation{}, compute.Errorf(compute.CodeNotFound, "no operation for request %s", r)
	}
	return s.DesiredStore.OperationByRequest(ctx, r)
}

func TestRetryThatLosesTheLookupRaceStillReplays(t *testing.T) {
	repo := memstore.New()
	create := ok(compute.NewCreateKeyPair("deploy"))
	first := submit(t, newController(repo), "key-1", create)

	again := submit(t, newController(&lookupRacesCommit{DesiredStore: repo}), "key-1", create)
	if again.Operation.ID != first.Operation.ID || again.PrivateKey != nil {
		t.Fatalf("retry = op %s key %v, want op %s and no key", again.Operation.ID, again.PrivateKey != nil, first.Operation.ID)
	}
}

func TestDeleteKeyPair(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	c := newController(repo)
	del := ok(compute.NewDeleteKeyPair("deploy"))

	_, err := c.Submit(ctx, req("del-0"), del)
	wantCode(t, err, compute.CodeNotFound)

	created := submit(t, c, "create-1", ok(compute.NewCreateKeyPair("deploy")))
	gone := submit(t, c, "del-1", del)
	if gone.Operation.State() != compute.OperationSucceeded || gone.Operation.TargetGeneration != 2 {
		t.Fatalf("delete = %s gen %d, want Succeeded gen 2", gone.Operation.State(), gone.Operation.TargetGeneration)
	}
	again := submit(t, c, "del-2", del)
	if again.Operation.ID == gone.Operation.ID || again.Operation.State() != compute.OperationSucceeded ||
		again.Operation.Target != created.Operation.Target || again.Operation.TargetGeneration != 2 {
		t.Fatalf("second delete = %s %s on %s gen %d, want a new Succeeded no-op on %s gen 2",
			again.Operation.ID, again.Operation.State(), again.Operation.Target, again.Operation.TargetGeneration, created.Operation.Target)
	}

	recreated := submit(t, c, "create-2", ok(compute.NewCreateKeyPair("deploy")))
	if recreated.Operation.Target == created.Operation.Target {
		t.Fatalf("re-created key pair reused id %s", created.Operation.Target)
	}
	if n := len(ok(repo.KeyPairs(ctx, store.KeyPairQuery{})).Items); n != 1 {
		t.Fatalf("live key pairs = %d, want 1", n)
	}
}

// conflicting fails the first n commits with store.ErrConflict, and runs
// race before delegating the commit after that.
type conflicting struct {
	store.DesiredStore
	n    int
	race func()
}

func (c *conflicting) CommitDesired(ctx context.Context, ch store.DesiredChange) (compute.Operation, error) {
	if c.n > 0 {
		c.n--
		return compute.Operation{}, store.ErrConflict
	}
	if c.race != nil {
		race := c.race
		c.race = nil
		race()
	}
	return c.DesiredStore.CommitDesired(ctx, ch)
}

func TestSubmitReplansUpToMaxReplansThenAborts(t *testing.T) {
	ctx := context.Background()
	register := ok(compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2"))

	repo := memstore.New()
	r := submit(t, newController(&conflicting{DesiredStore: repo, n: controller.MaxReplans}), "reg", register)
	if n := len(ok(repo.Images(ctx, store.ImageQuery{})).Items); n != 1 || r.Operation.Target.ID != ok(repo.Images(ctx, store.ImageQuery{})).Items[0].Spec.ID.String() {
		t.Fatalf("after %d conflicts: %d images, want the one the operation targets", controller.MaxReplans, n)
	}

	repo = memstore.New()
	_, err := newController(&conflicting{DesiredStore: repo, n: controller.MaxReplans + 1}).Submit(ctx, req("reg"), register)
	wantCode(t, err, compute.CodeAborted)
	if n := len(ok(repo.Images(ctx, store.ImageQuery{})).Items); n != 0 {
		t.Fatalf("aborted submit stored %d images", n)
	}
	_, err = repo.OperationByRequest(ctx, req("reg"))
	wantCode(t, err, compute.CodeNotFound)
}

func TestSubmitReplansAgainstStateThatMovedUnderThePlan(t *testing.T) {
	ctx := context.Background()
	repo := memstore.New()
	c := newController(repo)
	img := availableImage(t, repo, c)

	racing := newController(&conflicting{DesiredStore: repo, race: func() {
		submit(t, c, "dereg", ok(compute.NewDeregisterImage(img.String())))
	}})
	_, err := racing.Submit(ctx, req("run"), ok(compute.NewRunInstance(img.String(), "vws.small", "")))
	wantCode(t, err, compute.CodeFailedPrecondition)
	if n := len(ok(repo.Instances(ctx, store.InstanceQuery{})).Items); n != 0 {
		t.Fatalf("instances = %d, want 0: the plan read an image that was deregistered before commit", n)
	}
}

func TestSubmitValidatesArguments(t *testing.T) {
	c := newController(memstore.New())
	_, err := c.Submit(context.Background(), compute.RequestID{}, ok(compute.NewCreateKeyPair("k")))
	wantCode(t, err, compute.CodeInvalid)
	_, err = c.Submit(context.Background(), req("r"), nil)
	wantCode(t, err, compute.CodeInvalid)
}
