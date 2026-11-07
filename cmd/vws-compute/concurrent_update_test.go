package main

import (
	"context"
	"sync/atomic"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

// raceOnUpdate runs rival once per armed update of ConfigMap name, before
// that update reaches the apiserver, and counts those updates. The update
// then carries a stale resourceVersion and the apiserver refuses it.
type raceOnUpdate struct {
	name    string
	armed   atomic.Bool
	updates atomic.Int64
	// every runs rival before every armed update instead of only the first.
	every bool
	rival func()
}

func (r *raceOnUpdate) client(c client.WithWatch) client.WithWatch {
	return interceptor.NewClient(c, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
			if r.armed.Load() && o.GetName() == r.name {
				r.updates.Add(1)
				if !r.every {
					r.armed.Store(false)
				}
				r.rival()
			}
			return c.Update(ctx, o, opts...) //nolint:wrapcheck // a test pass-through
		},
	})
}

func labelPatch(t *testing.T, key, value string) compute.MetadataPatch {
	t.Helper()
	l := must(compute.ParseLabels(map[string]string{key: value}))
	return must(compute.MetadataPatch{}.EditLabels(l, nil))
}

// TestConcurrentUpdatesOfOneInstanceLoseNothing has two replicas edit the
// labels of one instance at the same time. Replica a commits between
// replica b's read and b's write, so the apiserver refuses b's stale
// resourceVersion. b rereads, its plan no longer matches the stored
// metadata revision, and it replans on a's labels: both edits are stored.
// A writer that keeps losing gets compute.CodeAborted and stores nothing.
func TestConcurrentUpdatesOfOneInstanceLoseNothing(t *testing.T) {
	ctx := context.Background()
	c, ns := kubetest.Namespace(t)
	cluster := fakeagent.New()
	a := startReplica(t, c, ns, "a", cluster)
	register := must(compute.NewRegisterFile("debian-13", "/var/lib/vws/images/debian-13.qcow2", "debian-13"))
	img := a.submit(t, "register", register)
	waitSucceeded(t, a.repo, img)
	run := a.submit(t, "run", must(compute.NewRunInstance(img.Target.ID, "vws.small", "")))
	waitSucceeded(t, a.repo, run)
	id := run.Target.ID

	race := &raceOnUpdate{name: "vws-instance-" + id}
	b := startReplica(t, race.client(c), ns, "b", cluster)
	edit := func(r *testReplica, req, key, value string) (controller.Receipt, error) {
		in := must(compute.NewUpdateInstanceMetadata(id, labelPatch(t, key, value)))
		return r.ctl.Submit(ctx, must(compute.ParseRequestID(req)), in)
	}

	t.Run("the loser replans and both edits are stored", func(t *testing.T) {
		race.rival = func() {
			if _, err := edit(a, "edit-a", "team", "a"); err != nil {
				t.Errorf("replica a: %v", err)
			}
		}
		race.armed.Store(true)
		got, err := edit(b, "edit-b", "owner", "b")
		if err != nil {
			t.Fatalf("replica b: %v", err)
		}
		if got.Operation.State() != compute.OperationSucceeded {
			t.Fatalf("replica b operation = %s, want Succeeded", got.Operation.State())
		}
		if n := race.updates.Load(); n != 1 {
			t.Fatalf("raced updates = %d, want 1", n)
		}
		inst := must(b.repo.Instance(ctx, must(compute.ParseInstanceID(id))))
		labels := inst.Meta.Labels.Map()
		if labels["team"] != "a" || labels["owner"] != "b" || len(labels) != 2 {
			t.Fatalf("labels = %v, want both edits", labels)
		}
		if inst.Meta.Revision != 2 {
			t.Fatalf("metadata revision = %d, want 2: one per edit", inst.Meta.Revision)
		}
		again, err := edit(b, "edit-b", "owner", "b")
		if err != nil || again.Operation.ID != got.Operation.ID {
			t.Fatalf("replay = %v %v, want operation %s", again.Operation.ID, err, got.Operation.ID)
		}
	})

	t.Run("a writer that always loses gets Aborted and stores nothing", func(t *testing.T) {
		var rivals atomic.Int64
		race.updates.Store(0)
		race.every = true
		race.rival = func() {
			n := rivals.Add(1)
			req := "rival-" + string(rune('a'+n))
			if _, err := edit(a, req, "rival", req); err != nil {
				t.Errorf("replica a: %v", err)
			}
		}
		race.armed.Store(true)
		_, err := edit(b, "edit-lost", "lost", "b")
		race.armed.Store(false)
		if compute.CodeOf(err) != compute.CodeAborted {
			t.Fatalf("replica b err = %v, want Aborted", err)
		}
		if n := race.updates.Load(); n != controller.MaxReplans+1 {
			t.Fatalf("raced updates = %d, want %d", n, controller.MaxReplans+1)
		}
		inst := must(a.repo.Instance(ctx, must(compute.ParseInstanceID(id))))
		labels := inst.Meta.Labels.Map()
		if _, ok := labels["lost"]; ok {
			t.Fatalf("labels = %v, an aborted edit was stored", labels)
		}
		if labels["team"] != "a" || labels["owner"] != "b" {
			t.Fatalf("labels = %v, the earlier edits were lost", labels)
		}
		_, err = a.repo.OperationByRequest(ctx, must(compute.ParseRequestID("edit-lost")))
		if compute.CodeOf(err) != compute.CodeNotFound {
			t.Fatalf("aborted request has an operation: %v", err)
		}
		if _, err := edit(b, "edit-lost", "lost", "b"); err != nil {
			t.Fatalf("retry with the same request id: %v", err)
		}
		if _, ok := must(b.repo.Instance(ctx, must(compute.ParseInstanceID(id)))).Meta.Labels.Get("lost"); !ok {
			t.Fatal("the retried edit was not stored")
		}
	})
}
