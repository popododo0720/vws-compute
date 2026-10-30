package reconciler_test

import (
	"context"
	"testing"
	"time"

	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

func TestRunReportsEachSweepToTheObserver(t *testing.T) {
	results := make(chan string, 8)
	rec := reconciler.New(memstore.New(), fakeagent.New(),
		reconciler.WithInterval(time.Hour),
		reconciler.WithSweepObserver(func(_ context.Context, r string, _ time.Duration, _ error) { results <- r }))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rec.Run(ctx) }()
	select {
	case r := <-results:
		if r != reconciler.SweepOK {
			t.Fatalf("first sweep of an empty repository observed %q, want %q", r, reconciler.SweepOK)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no sweep observed")
	}
	cancel()
	<-done
}
