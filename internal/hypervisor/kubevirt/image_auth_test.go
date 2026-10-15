package kubevirt

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// The tests in this file pin the review of vws-compute#93 (ae8fbeb): a
// DataSource's authorization labels follow the store record and the public
// list on every sweep, whatever state the image is in. Each one fails on
// ae8fbeb, where only a converging or restored Available image was labeled.

var errTransient = errors.New("still working")

type authClock struct{ t time.Time }

func (c *authClock) Now() time.Time { return c.t }

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// seedPublicDataSource makes the managed DataSource an earlier sweep left
// for a public image of project a123456789012 in vws-1.
func seedPublicDataSource(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	ds := &unstructured.Unstructured{}
	ds.SetGroupVersionKind(dsGVK)
	ds.SetNamespace(ns)
	ds.SetName(name)
	ds.SetLabels(map[string]string{LabelManaged: "true", LabelProject: "a123456789012", LabelRegion: "vws-1", LabelPublic: "true"})
	ds.Object["spec"] = map[string]any{"source": map[string]any{"pvc": map[string]any{"name": name, "namespace": ns}}}
	if err := c.Create(context.Background(), ds); err != nil {
		t.Fatal(err)
	}
}

func labelsOf(t *testing.T, c client.Client, ns, name string) map[string]string {
	t.Helper()
	ds := &unstructured.Unstructured{}
	ds.SetGroupVersionKind(dsGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, ds); err != nil {
		t.Fatal(err)
	}
	return ds.GetLabels()
}

// sweepUnpublished runs a reconciler without the image on its public list
// against the kubevirt agent: a startup Restore and then one sweep.
func sweepUnpublished(t *testing.T, repo *memstore.Store, c client.Client, ns string, clk *authClock) {
	t.Helper()
	ctx := context.Background()
	rec := reconciler.New(repo, New(c, Config{Namespace: ns, ImageNamespace: ns, ImportSize: "10Gi"}), reconciler.WithClock(clk.Now))
	_ = rec.Restore(ctx)
	_ = rec.Sweep(ctx)
}

func TestParkedImageLosesThePublicLabel(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	clk := &authClock{t: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
	repo := memstore.New()
	ctl := controller.New(repo, controller.WithClock(clk.Now))
	scope := must(compute.NewImageScope("a123456789012", "vws-1"))
	src := compute.FromVolume{Ref: must(compute.ParseVolumeRef("projects/a123456789012/zones/vws-1a/volumes/vol-1"))}
	const id = "ami-0aaaaaaaaaaaaaaa1"
	if _, err := ctl.Submit(ctx, must(compute.ParseRequestID("copy-1")), must(compute.NewCopyImage(id, scope, src, "", "", compute.Labels{}))); err != nil {
		t.Fatal(err)
	}
	// The image never converges and is parked after GiveUpAfter.
	agent := fakeagent.New()
	agent.Fail(id, errTransient, errTransient)
	fake := reconciler.New(repo, agent, reconciler.WithClock(clk.Now))
	_ = fake.Sweep(ctx)
	clk.t = clk.t.Add(reconciler.GiveUpAfter + time.Minute)
	_ = fake.Sweep(ctx)
	img := must(repo.Image(ctx, must(compute.ParseImageID(id))))
	if !img.Status.Observed.Parked(img.Spec.Generation) {
		t.Fatalf("setup: image not parked: %+v", img.Status)
	}

	seedPublicDataSource(t, c, ns, id)
	sweepUnpublished(t, repo, c, ns, clk)
	l := labelsOf(t, c, ns, id)
	if _, has := l[LabelPublic]; has || l[LabelProject] != "a123456789012" || l[LabelRegion] != "vws-1" {
		t.Fatalf("parked image labels %v, want the owner without vws.io/public", l)
	}
}

func TestImageWithoutSourceMachineLosesItsLabels(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	clk := &authClock{t: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
	repo := memstore.New()
	ctl := controller.New(repo, controller.WithClock(clk.Now))
	agent := fakeagent.New()
	fake := reconciler.New(repo, agent, reconciler.WithClock(clk.Now))
	reg := must(ctl.Submit(ctx, must(compute.ParseRequestID("reg-1")), must(compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2"))))
	_ = fake.Sweep(ctx)
	run := must(ctl.Submit(ctx, must(compute.ParseRequestID("run-1")), must(compute.NewRunInstance(reg.Operation.Target.ID, "vws.small", ""))))
	_ = fake.Sweep(ctx)
	capture := must(ctl.Submit(ctx, must(compute.ParseRequestID("cap-1")), must(compute.NewCreateImage(run.Operation.Target.ID, "snap-1"))))
	id := capture.Operation.Target.ID
	agent.Fail(id, errTransient)
	_ = fake.Sweep(ctx)

	// The capture's source machine has no VirtualMachine here, and the
	// image has no project, so its DataSource must carry no owner at all.
	seedPublicDataSource(t, c, ns, id)
	sweepUnpublished(t, repo, c, ns, clk)
	l := labelsOf(t, c, ns, id)
	for _, k := range []string{LabelProject, LabelRegion, LabelPublic} {
		if _, has := l[k]; has {
			t.Fatalf("label %s kept on an image with no source machine: %v", k, l)
		}
	}
}
