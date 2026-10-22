package grpcapi_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestKeyPairNameHasOneHolder pins EC2 InvalidKeyPair.Duplicate on the
// resource name: of concurrent creators of one name, with distinct request
// ids and keys, exactly one succeeds and the rest see ALREADY_EXISTS. Delete
// frees the name, and a reopened store still refuses a duplicate.
func TestKeyPairNameHasOneHolder(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		repo := memstore.New()
		keyPairNameHasOneHolder(t, func() store.Repository { return repo }, false)
	})
	t.Run("kubestore", func(t *testing.T) {
		kc, ns := kubetest.Namespace(t)
		keyPairNameHasOneHolder(t, func() store.Repository { return kubetest.Open(t, kc, ns) }, true)
	})
}

func keyPairNameHasOneHolder(t *testing.T, open func() store.Repository, reopen bool) {
	ctx := context.Background()
	parent := "projects/a12345/regions/us-east1"
	name := parent + "/keyPairs/deploy"
	h := serveRepo(t, open())
	seq := 0
	// next is a create of name with a new request id and a new key.
	next := func() *computev1.CreateKeyPairRequest {
		seq++
		line, _ := ed25519Line(t)
		return &computev1.CreateKeyPairRequest{
			Parent: parent, KeyPairId: "deploy", RequestId: fmt.Sprintf("aaaaaaaa-aaaa-4aaa-8aaa-%012d", seq),
			KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
		}
	}
	create := func(h *harness) error {
		_, err := h.keys.CreateKeyPair(ctx, next())
		return err
	}

	const n = 16
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		req := next()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = h.keys.CreateKeyPair(ctx, req)
		}()
	}
	close(start)
	wg.Wait()
	won := 0
	for _, err := range errs {
		if err == nil {
			won++
			continue
		}
		wantCode(t, err, codes.AlreadyExists)
		wantReason(t, err, "ALREADY_EXISTS")
	}
	if won != 1 {
		t.Fatalf("%d of %d concurrent creates succeeded, want 1: %v", won, n, errs)
	}
	list, err := h.keys.ListKeyPairs(ctx, &computev1.ListKeyPairsRequest{Parent: parent})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetKeyPairs()) != 1 || list.GetKeyPairs()[0].GetName() != name {
		t.Fatalf("list = %v, want only %s", list.GetKeyPairs(), name)
	}

	if reopen {
		h.stop()
		h = serveRepo(t, open())
		err := create(h)
		wantCode(t, err, codes.AlreadyExists)
	}

	del, err := h.keys.DeleteKeyPair(ctx, &computev1.DeleteKeyPairRequest{
		Name: name, RequestId: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !del.GetDone() || del.GetError() != nil {
		t.Fatalf("delete = done %v err %v", del.GetDone(), del.GetError())
	}
	if err := create(h); err != nil {
		t.Fatalf("create after delete = %v, want the freed name", err)
	}
	wantCode(t, create(h), codes.AlreadyExists)
}
