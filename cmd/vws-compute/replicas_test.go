package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// countingAgent is one replica's view of the shared cluster: every machine
// write goes to the one fake agent and is counted per replica.
type countingAgent struct {
	hypervisor.Agent
	writes atomic.Int64
}

func (a *countingAgent) ConvergeMachine(ctx context.Context, spec hypervisor.MachineSpec) error {
	a.writes.Add(1)
	return a.Agent.ConvergeMachine(ctx, spec) //nolint:wrapcheck // a test pass-through
}

type testReplica struct {
	repo  *kubestore.Store
	ctl   *controller.Controller
	agent *countingAgent
	stop  func()
}

func startReplica(t *testing.T, c client.Client, ns, id string, cluster hypervisor.Agent) *testReplica {
	t.Helper()
	repo := kubestore.New(c, ns).WithLease(2*time.Second, time.Second, 250*time.Millisecond)
	agent := &countingAgent{Agent: cluster}
	rec := reconciler.New(repo, agent, reconciler.WithInterval(50*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- recovered(func() error { return lead(ctx, repo, id, rec.Run) }) }()
	var once sync.Once
	r := &testReplica{repo: repo, ctl: controller.New(repo, controller.WithKick(rec.Kick)), agent: agent}
	r.stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("replica %s = %v, want context.Canceled", id, err)
			}
		})
	}
	t.Cleanup(r.stop)
	return r
}

func (r *testReplica) submit(t *testing.T, req string, in compute.Intent) compute.Operation {
	t.Helper()
	got, err := r.ctl.Submit(context.Background(), must(compute.ParseRequestID(req)), in)
	if err != nil {
		t.Fatalf("Submit %s: %v", req, err)
	}
	return got.Operation
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func waitSucceeded(t *testing.T, repo store.Repository, ops ...compute.Operation) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d operations succeed", len(ops)), func() bool {
		for _, op := range ops {
			got, err := repo.Operation(context.Background(), op.ID)
			if err != nil || got.State() != compute.OperationSucceeded {
				return false
			}
		}
		return true
	})
}

// TestTwoReplicasReconcileOnceAndFailOver runs two replicas against one
// apiserver and one cluster. Both accept writes. Only the leader writes the
// cluster, so every machine is created once. A burst in flight when the
// leader shuts down is finished by the follower, still one creation each.
func TestTwoReplicasReconcileOnceAndFailOver(t *testing.T) {
	c, ns := kubetest.Namespace(t)
	cluster := fakeagent.New()
	a := startReplica(t, c, ns, "a", cluster)
	register := must(compute.NewRegisterFile("debian-13", "/var/lib/vws/images/debian-13.qcow2", "debian-13"))
	img := a.submit(t, "register", register)
	waitSucceeded(t, a.repo, img)
	b := startReplica(t, c, ns, "b", cluster)

	run := func(from *testReplica, batch, n int) []compute.Operation {
		var ops []compute.Operation
		for i := range n {
			in := must(compute.NewRunInstance(img.Target.ID, "vws.small", ""))
			ops = append(ops, from.submit(t, fmt.Sprintf("run-%d-%d", batch, i), in))
		}
		return ops
	}
	first := run(b, 1, 5)
	waitSucceeded(t, b.repo, first...)
	if n := b.agent.writes.Load(); n != 0 {
		t.Fatalf("follower machine writes = %d, want 0", n)
	}
	if a.agent.writes.Load() == 0 {
		t.Fatal("leader wrote no machine")
	}

	second := run(b, 2, 5)
	a.stop()
	third := run(b, 3, 5)
	all := append(append(first, second...), third...)
	waitSucceeded(t, b.repo, all...)
	if b.agent.writes.Load() == 0 {
		t.Fatal("follower did not take over")
	}
	for _, op := range all {
		if n := cluster.Creations(op.Target.ID); n != 1 {
			t.Errorf("machine %s created %d times, want 1", op.Target.ID, n)
		}
	}
}
