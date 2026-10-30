package grpcapi_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestKeyPairReincarnation pins issue #115: a key name that is deleted and
// created again names a new key pair, as an EC2 KeyPairId is unique per key
// pair. Each create after a delete gets a new uid (the "key-" KeyPairId) and
// the new key; Get and List return only that incarnation; a replayed or
// polled operation of an older incarnation still answers with that older,
// deleted record and never with the live one. Concurrent re-creates of a
// freed name have one winner. The kubestore run reopens the store between
// every step, so nothing depends on process memory.
func TestKeyPairReincarnation(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		repo := memstore.New()
		keyPairReincarnation(t, func() store.Repository { return repo }, false)
	})
	t.Run("kubestore", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		keyPairReincarnation(t, func() store.Repository { return kubetest.Open(t, kc, ns) }, true)
	})
}

// incarnation is what one successful create returned.
type incarnation struct {
	request string
	op      *longrunningpb.Operation
	uid     string
	fp      string
}

func keyPairReincarnation(t *testing.T, open func() store.Repository, reopen bool) {
	ctx := context.Background()
	parent := "projects/a12345/regions/us-east1"
	name := parent + "/keyPairs/deploy"
	h := serveRepo(t, open())
	restart := func() {
		if reopen {
			h.stop()
			h = serveRepo(t, open())
		}
	}
	seq := 0
	requestID := func() string {
		seq++
		return fmt.Sprintf("bbbbbbbb-bbbb-4bbb-8bbb-%012d", seq)
	}
	createReq := func(req, line string) *computev1.CreateKeyPairRequest {
		return &computev1.CreateKeyPairRequest{
			Parent: parent, KeyPairId: "deploy", RequestId: req,
			KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
		}
	}
	create := func(line, fp string) incarnation {
		t.Helper()
		req := requestID()
		op, err := h.keys.CreateKeyPair(ctx, createReq(req, line))
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		got := unpackKeyPair(t, op)
		if !op.GetDone() || got.GetName() != name || got.GetGeneration() != 1 || got.GetDeleteTime() != nil ||
			!strings.HasPrefix(got.GetUid(), "key-") || got.GetStatus().GetFingerprintSha256() != fp {
			t.Fatalf("create = done %v %v, want live %s gen 1 key-* uid fingerprint %s", op.GetDone(), got, name, fp)
		}
		return incarnation{request: req, op: op, uid: got.GetUid(), fp: fp}
	}
	remove := func() {
		t.Helper()
		op, err := h.keys.DeleteKeyPair(ctx, &computev1.DeleteKeyPairRequest{Name: name, RequestId: requestID()})
		if err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
		if !op.GetDone() || op.GetError() != nil {
			t.Fatalf("delete = done %v err %v", op.GetDone(), op.GetError())
		}
	}
	// wantOnly checks that Get and List see exactly live, and that every
	// older incarnation still answers its own create replay and operation
	// poll with its own deleted record.
	wantOnly := func(live incarnation, old ...incarnation) {
		t.Helper()
		got, err := h.keys.GetKeyPair(ctx, &computev1.GetKeyPairRequest{Name: name})
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if got.GetUid() != live.uid || got.GetStatus().GetFingerprintSha256() != live.fp ||
			got.GetDeleteTime() != nil || got.GetGeneration() != 1 {
			t.Fatalf("get = %v, want live uid %s fingerprint %s gen 1", got, live.uid, live.fp)
		}
		list, err := h.keys.ListKeyPairs(ctx, &computev1.ListKeyPairsRequest{Parent: parent})
		if err != nil {
			t.Fatalf("list %s: %v", parent, err)
		}
		items := list.GetKeyPairs()
		if len(items) != 1 || items[0].GetUid() != live.uid || items[0].GetDeleteTime() != nil {
			t.Fatalf("list = %v, want only uid %s", list.GetKeyPairs(), live.uid)
		}
		for _, o := range old {
			if o.uid == live.uid {
				t.Fatalf("live key pair reuses the deleted uid %s", o.uid)
			}
			polled, err := h.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: o.op.GetName()})
			if err != nil {
				t.Fatalf("poll %s: %v", o.op.GetName(), err)
			}
			if kp := unpackKeyPair(t, polled); kp.GetUid() != o.uid || kp.GetDeleteTime() == nil {
				t.Fatalf("poll of old create = %v, want deleted uid %s", kp, o.uid)
			}
		}
	}

	lineA, fpA := ed25519Line(t)
	a := create(lineA, fpA)
	remove()
	restart()

	lineB, fpB := ed25519Line(t)
	b := create(lineB, fpB)
	if b.fp == a.fp {
		t.Fatal("test keys share a fingerprint")
	}
	wantOnly(b, a)
	restart()
	wantOnly(b, a)

	// A replay of the first create's request id returns that request's
	// operation and record, not the live key pair, and writes nothing.
	replay, err := h.keys.CreateKeyPair(ctx, createReq(a.request, lineA))
	if err != nil {
		t.Fatalf("replay of first create: %v", err)
	}
	if replay.GetName() != a.op.GetName() {
		t.Fatalf("replay operation = %s, want %s", replay.GetName(), a.op.GetName())
	}
	if kp := unpackKeyPair(t, replay); kp.GetUid() != a.uid || kp.GetDeleteTime() == nil {
		t.Fatalf("replay record = %v, want deleted uid %s", kp, a.uid)
	}
	wantOnly(b, a)

	// Importing the first material again is still a new key pair: identity
	// is the KeyPairId, not the key.
	remove()
	restart()
	c := create(lineA, fpA)
	if c.uid == a.uid || c.uid == b.uid {
		t.Fatalf("re-import of the first key got uid %s, want one unlike %s and %s", c.uid, a.uid, b.uid)
	}
	wantOnly(c, a, b)

	// Concurrent re-creates of the freed name: one winner with a new uid.
	remove()
	restart()
	const n = 16
	type attempt struct {
		req, line, fp string
		op            *longrunningpb.Operation
		err           error
	}
	attempts := make([]attempt, n)
	for i := range attempts {
		line, fp := ed25519Line(t)
		attempts[i] = attempt{req: requestID(), line: line, fp: fp}
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			attempts[i].op, attempts[i].err = h.keys.CreateKeyPair(ctx, createReq(attempts[i].req, attempts[i].line))
		}()
	}
	close(start)
	wg.Wait()
	var winners []incarnation
	for _, at := range attempts {
		if at.err != nil {
			wantCode(t, at.err, codes.AlreadyExists)
			wantReason(t, at.err, "ALREADY_EXISTS")
			continue
		}
		winners = append(winners, incarnation{request: at.req, op: at.op, uid: unpackKeyPair(t, at.op).GetUid(), fp: at.fp})
	}
	if len(winners) != 1 {
		t.Fatalf("%d of %d concurrent re-creates succeeded, want 1", len(winners), n)
	}
	d := winners[0]
	if kp := unpackKeyPair(t, d.op); kp.GetStatus().GetFingerprintSha256() != d.fp || kp.GetDeleteTime() != nil {
		t.Fatalf("winner = %v, want its own key %s", kp, d.fp)
	}
	wantOnly(d, a, b, c)
	restart()
	wantOnly(d, a, b, c)
}
