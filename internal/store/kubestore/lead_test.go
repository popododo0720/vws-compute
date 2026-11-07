package kubestore_test

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// Short timings keep the tests fast. leaseDuration > renewDeadline +
// retryPeriod, so a deposed leader stops before a follower may take over.
const (
	leaseDuration = 2 * time.Second
	renewDeadline = time.Second
	retryPeriod   = 250 * time.Millisecond
)

// terms records which replicas are inside a term right now.
type terms struct {
	mu        sync.Mutex
	active    map[string]bool
	maxActive int
	started   map[string]chan struct{}
	stopped   map[string]chan struct{}
}

func newTerms(ids ...string) *terms {
	t := &terms{active: map[string]bool{}, started: map[string]chan struct{}{}, stopped: map[string]chan struct{}{}}
	for _, id := range ids {
		t.started[id] = make(chan struct{})
		t.stopped[id] = make(chan struct{})
	}
	return t
}

// term is a leader-only loop that runs until its context ends. Only its
// first term per replica signals started and stopped.
func (t *terms) term(id string) func(context.Context) error {
	var first sync.Once
	return func(ctx context.Context) error {
		t.mu.Lock()
		t.active[id] = true
		t.maxActive = max(t.maxActive, len(t.active))
		t.mu.Unlock()
		once := false
		first.Do(func() { once = true; close(t.started[id]) })
		<-ctx.Done()
		t.mu.Lock()
		delete(t.active, id)
		t.mu.Unlock()
		if once {
			close(t.stopped[id])
		}
		return ctx.Err()
	}
}

func (t *terms) max() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maxActive
}

func within(t *testing.T, what string, ch <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("%s did not happen within %s", what, d)
	}
}

func replica(t *testing.T, c client.Client, ns, id string, term func(context.Context) error) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s := kubestore.New(c, ns).WithLease(leaseDuration, renewDeadline, retryPeriod)
	go func() { done <- s.Lead(ctx, id, term) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("Lead(%s) = %v, want context.Canceled", id, err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// TestOneReplicaLeadsAndAFollowerTakesOver runs two replicas on one
// namespace. Only one is ever in a term. When the leader shuts down it
// releases the Lease, so the follower leads well within leaseDuration +
// retryPeriod.
func TestOneReplicaLeadsAndAFollowerTakesOver(t *testing.T) {
	c, ns := kubetest.Namespace(t)
	tr := newTerms("a", "b")
	stopA := replica(t, c, ns, "a", tr.term("a"))
	within(t, "a leads", tr.started["a"], 10*time.Second)
	replica(t, c, ns, "b", tr.term("b"))

	time.Sleep(2 * leaseDuration)
	select {
	case <-tr.started["b"]:
		t.Fatal("b led while a held the Lease")
	default:
	}

	stopped := time.Now()
	stopA()
	within(t, "b takes over", tr.started["b"], leaseDuration+retryPeriod)
	t.Logf("takeover after shutdown: %s", time.Since(stopped))
	if n := tr.max(); n != 1 {
		t.Fatalf("replicas leading at once = %d, want 1", n)
	}

	var l coordinationv1.Lease
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: kubestore.LeaseName}, &l); err != nil {
		t.Fatal(err)
	}
	if h := l.Spec.HolderIdentity; h == nil || *h != "b" {
		t.Fatalf("Lease holder = %v, want b", h)
	}
}

// TestLostLeaseStopsTheTermWithinTheRenewDeadline cuts the leader off from
// its Lease. Its term ends one renew deadline after its next renew attempt,
// before the follower may take over, and the follower leads about
// leaseDuration after the last renew. The bounds are loose for slow
// runners. The logged times and the overlap check are the measure.
func TestLostLeaseStopsTheTermWithinTheRenewDeadline(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	var cut atomic.Bool
	partitioned := errors.New("partitioned from the apiserver")
	isLease := func(o client.Object) bool { _, ok := o.(*coordinationv1.Lease); return ok }
	c := interceptor.NewClient(raw, interceptor.Funcs{
		Get: func(
			ctx context.Context, c client.WithWatch, k client.ObjectKey, o client.Object, opts ...client.GetOption,
		) error {
			if cut.Load() && isLease(o) {
				return partitioned
			}
			return c.Get(ctx, k, o, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
			if cut.Load() && isLease(o) {
				return partitioned
			}
			return c.Update(ctx, o, opts...)
		},
	})
	tr := newTerms("a", "b")
	replica(t, c, ns, "a", tr.term("a"))
	within(t, "a leads", tr.started["a"], 10*time.Second)
	replica(t, raw, ns, "b", tr.term("b"))
	time.Sleep(leaseDuration)

	cut.Store(true)
	cutAt := time.Now()
	within(t, "a stops leading", tr.stopped["a"], 3*(retryPeriod+renewDeadline))
	t.Logf("a stopped %s after the cut", time.Since(cutAt))
	within(t, "b takes over", tr.started["b"], 3*(leaseDuration+retryPeriod))
	t.Logf("b led %s after the cut", time.Since(cutAt))
	if n := tr.max(); n != 1 {
		t.Fatalf("replicas leading at once = %d, want 1", n)
	}
}

// TestLeaderRepairSparesFreshWrites leaves an orphaned index the way a
// crash between the index and the resource write does. The leader's repair
// keeps it while it is younger than any commit in flight could be, since a
// follower may be between its two writes, and removes it once it is old.
func TestLeaderRepairSparesFreshWrites(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	if _, err := kubestore.New(crash(raw), ns).CommitDesired(ctx, change(t, "r-1", runSpec(t, 1, 1))); err == nil {
		t.Fatal("commit survived the crash")
	}
	repaired := make(chan struct{}, 4)
	c := interceptor.NewClient(raw, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			err := c.List(ctx, l, opts...)
			var lo client.ListOptions
			lo.ApplyOptions(opts)
			if lo.LabelSelector != nil && lo.LabelSelector.String() == "vws.io/kind=claim-volume" {
				repaired <- struct{}{} // the last list of one repair pass
			}
			return err
		},
	})
	idle := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

	stop := replica(t, c, ns, "a", idle)
	within(t, "first repair", repaired, 10*time.Second)
	stop()
	orphans := configMaps(t, raw, ns, "request")
	if len(orphans) != 1 {
		t.Fatalf("indexes after a repair beside a fresh write = %d, want the fresh one kept", len(orphans))
	}

	old := orphans[0]
	old.Data["written"] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if err := raw.Update(ctx, &old); err != nil {
		t.Fatal(err)
	}
	replica(t, c, ns, "a", idle)
	within(t, "second repair", repaired, 10*time.Second)
	if n := len(configMaps(t, raw, ns, "request")); n != 0 {
		t.Fatalf("indexes after the repair of an old orphan = %d, want 0", n)
	}
}

// TestTermErrorEndsLead returns an error from the term while Lead's context
// is live. Lead stops with it instead of holding the Lease with no worker.
func TestTermErrorEndsLead(t *testing.T) {
	c, ns := kubetest.Namespace(t)
	boom := errors.New("reconciler failed")
	done := make(chan error, 1)
	s := kubestore.New(c, ns).WithLease(leaseDuration, renewDeadline, retryPeriod)
	go func() { done <- s.Lead(context.Background(), "a", func(context.Context) error { return boom }) }()
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("Lead = %v, want the term error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Lead did not return after its term failed")
	}
}

const testVolumes = "projects/p/zones/z/volumes/"

func drivenSpec(gen int64, data bool) compute.InstanceSpec {
	root := ok(compute.ParseVolumeRef(testVolumes + "root"))
	vols := []compute.VolumeAttachment{{Volume: root, DeviceName: "vda", Boot: true}}
	if data {
		vols = append(vols, compute.VolumeAttachment{
			Volume: ok(compute.ParseVolumeRef(testVolumes + "data")), DeviceName: "vdb", Claim: "pvc-data",
		})
	}
	return compute.InstanceSpec{
		ID: ok(compute.ParseInstanceID("web-1")), Generation: gen, CreatedAt: t0, ChangedAt: t0,
		Type: ok(compute.ParseMachineType("n1.medium")), Power: compute.PowerRunning,
		Project: "p", Zone: "z", Volumes: vols,
		NICs: []compute.NetworkInterface{{DeviceName: "eth0", Subnet: "projects/p/regions/r/vpcs/v/subnets/s"}},
	}
}

func commitDriven(ctx context.Context, t *testing.T, s *kubestore.Store, req string, spec compute.InstanceSpec) {
	t.Helper()
	var in compute.Intent
	if spec.Generation == 1 {
		in = ok(compute.NewCreateInstance(compute.CreateInstanceParams{
			ID: "web-1", Project: "p", Zone: "z", MachineType: "vws.small", Power: compute.PowerRunning,
			Volumes: spec.Volumes, NICs: spec.NICs,
		}))
	} else {
		in = ok(compute.NewUpdateInstanceSpec("web-1", compute.PowerRunning, spec.Volumes, compute.MetadataPatch{}))
	}
	op := ok(compute.NewOperation(compute.Env{Now: t0, Rand: rand.Reader}, ok(compute.ParseRequestID(req)), in,
		compute.Change{Spec: spec, Write: true}))
	if _, err := s.CommitDesired(ctx, store.DesiredChange{Op: op, Spec: spec, Write: true}); err != nil {
		t.Fatalf("commit %s: %v", req, err)
	}
}

// TestReattachSurvivesARepairThatReadItsClaimAsStale detaches a volume, so
// its old claim is stale, and attaches it again to the same instance while
// the leader's Repair, which read the instance without the volume, is about
// to delete that claim. The claim stays, so no other instance can take the
// attached volume.
func TestReattachSurvivesARepairThatReadItsClaimAsStale(t *testing.T) {
	raw, ns := kubetest.Namespace(t)
	ctx := context.Background()
	follower := kubestore.New(raw, ns)
	commitDriven(ctx, t, follower, "c-1", drivenSpec(1, false))
	commitDriven(ctx, t, follower, "c-2", drivenSpec(2, true))
	commitDriven(ctx, t, follower, "c-3", drivenSpec(3, false))
	claims := configMaps(t, raw, ns, "claim-volume")
	var data *corev1.ConfigMap
	for i := range claims {
		if claims[i].Data["subject"] == testVolumes+"data" {
			data = &claims[i]
		}
	}
	if data == nil {
		t.Fatal("no claim for the detached data volume")
	}
	data.Data["written"] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if err := raw.Update(ctx, data); err != nil {
		t.Fatal(err)
	}

	repaired := make(chan struct{}, 1)
	var once sync.Once
	c := interceptor.NewClient(raw, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			err := c.List(ctx, l, opts...)
			var lo client.ListOptions
			lo.ApplyOptions(opts)
			if lo.LabelSelector != nil && lo.LabelSelector.String() == "vws.io/kind=claim-volume" {
				once.Do(func() { commitDriven(ctx, t, follower, "c-4", drivenSpec(4, true)) })
			}
			return err
		},
		Delete: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.DeleteOption) error {
			err := c.Delete(ctx, o, opts...)
			if o.GetName() == data.Name {
				repaired <- struct{}{}
			}
			return err
		},
	})
	replica(t, c, ns, "a", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	within(t, "repair tries the stale claim", repaired, 10*time.Second)

	var cur corev1.ConfigMap
	if err := raw.Get(ctx, client.ObjectKey{Namespace: ns, Name: data.Name}, &cur); err != nil {
		t.Fatalf("claim of the attached volume after repair: %v", err)
	}
	if cur.Data["holder"] != "web-1" {
		t.Fatalf("claim holder = %q, want web-1", cur.Data["holder"])
	}
}
