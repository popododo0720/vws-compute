package kubevirt

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

const imageID = "ami-0123456789abcdef0"

func urlImage() hypervisor.ImageSpec {
	return hypervisor.ImageSpec{Name: imageID, Present: true, SourceURL: "https://example.test/disk.qcow2"}
}

func imageAgent(c client.Client, ns string) *Agent {
	return New(c, Config{Namespace: ns, ImportSize: "10Gi"})
}

func exists(t *testing.T, c client.Client, gvk string, ns, name string) bool {
	t.Helper()
	u := &unstructured.Unstructured{}
	switch gvk {
	case "DataVolume":
		u.SetGroupVersionKind(dvGVK)
	case "DataSource":
		u.SetGroupVersionKind(dsGVK)
	}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, u)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// cdiFinishes plays CDI: it binds a claim of the given capacity and marks the
// DataVolume Succeeded.
func cdiFinishes(t *testing.T, c client.Client, ns, name, capacity string) {
	t.Helper()
	ctx := context.Background()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	if err := c.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)}
	if err := c.Status().Update(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	dv := &unstructured.Unstructured{}
	dv.SetGroupVersionKind(dvGVK)
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, dv); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(dv.Object, "Succeeded", "status", "phase"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, dv); err != nil {
		t.Fatal(err)
	}
}

func TestImageIsCreatedOnceAndSizedByTheClaim(t *testing.T) {
	c, ns, w := newClient(t)
	ctx := context.Background()
	spec := urlImage()

	if _, err := imageAgent(c, ns).ConvergeImage(ctx, spec); err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("while CDI imports: got %v, want a transient error", err)
	}
	if !exists(t, c, "DataVolume", ns, imageID) || !exists(t, c, "DataSource", ns, imageID) {
		t.Fatal("DataVolume and DataSource were not both created")
	}
	before := w.n.Load()
	// A restarted controller has a fresh agent and asks again.
	if _, err := imageAgent(c, ns).ConvergeImage(ctx, spec); err == nil {
		t.Fatal("still importing, want an error")
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("repeat wrote %d times, want 0", got)
	}

	cdiFinishes(t, c, ns, imageID, "3Gi")
	size, err := imageAgent(c, ns).ConvergeImage(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(3) << 30; size != want {
		t.Fatalf("size %d, want %d from the claim, not the 10Gi import request", size, want)
	}
}

func TestImageSizeNeedsASucceededDataVolume(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := imageAgent(c, ns)
	if _, err := a.ConvergeImage(ctx, urlImage()); err == nil {
		t.Fatal("want an error before CDI finishes")
	}
	// A claim with capacity does not count until the DataVolume succeeded.
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: imageID},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	if err := c.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")}
	if err := c.Status().Update(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	if size, err := a.ConvergeImage(ctx, urlImage()); err == nil {
		t.Fatalf("got size %d with the DataVolume unfinished, want an error", size)
	}
}

func TestRemoveImageIsIdempotent(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := imageAgent(c, ns)
	gone := hypervisor.ImageSpec{Name: imageID}
	if size, err := a.ConvergeImage(ctx, gone); err != nil || size != 0 {
		t.Fatalf("remove a missing image: size %d err %v", size, err)
	}
	if _, err := a.ConvergeImage(ctx, urlImage()); err == nil {
		t.Fatal("want an error while importing")
	}
	for range 2 {
		if size, err := a.ConvergeImage(ctx, gone); err != nil || size != 0 {
			t.Fatalf("remove: size %d err %v", size, err)
		}
	}
	if exists(t, c, "DataVolume", ns, imageID) || exists(t, c, "DataSource", ns, imageID) {
		t.Fatal("image objects remain")
	}
}

func TestRemoveImageRefusedWhileAMachineClonesIt(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := imageAgent(c, ns)
	if _, err := a.ConvergeImage(ctx, urlImage()); err == nil {
		t.Fatal("want an error while importing")
	}
	m := spec("i-0123456789abcdef0", hypervisor.PowerOff)
	m.Image = imageID
	if err := a.ConvergeMachine(ctx, m); err != nil {
		t.Fatal(err)
	}
	gone := hypervisor.ImageSpec{Name: imageID}
	if _, err := a.ConvergeImage(ctx, gone); err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("got %v, want a transient refusal", err)
	}
	if !exists(t, c, "DataVolume", ns, imageID) || !exists(t, c, "DataSource", ns, imageID) {
		t.Fatal("a refused removal deleted image objects")
	}
	m.Power = hypervisor.PowerAbsent
	if err := a.ConvergeMachine(ctx, m); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ConvergeImage(ctx, gone); err != nil {
		t.Fatal(err)
	}
	if exists(t, c, "DataVolume", ns, imageID) {
		t.Fatal("image remains after its last user is gone")
	}
}

func TestImageSources(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := imageAgent(c, ns)

	source := func(name string) map[string]any {
		dv := &unstructured.Unstructured{}
		dv.SetGroupVersionKind(dvGVK)
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, dv); err != nil {
			t.Fatal(err)
		}
		m, _, _ := unstructured.NestedMap(dv.Object, "spec", "source")
		return m
	}
	pvcName := func(m map[string]any, kind string) any {
		inner, _ := m[kind].(map[string]any)
		return inner["name"]
	}

	if _, err := a.ConvergeImage(ctx, hypervisor.ImageSpec{Name: "ami-1", Present: true, SourceVolume: "volumes/vol-abc"}); err == nil {
		t.Fatal("want an error until CDI finishes")
	}
	if got := pvcName(source("ami-1"), "pvc"); got != "vol-abc" {
		t.Fatalf("volume source claim %v, want vol-abc", got)
	}
	if _, err := a.ConvergeImage(ctx, hypervisor.ImageSpec{Name: "ami-2", Present: true, SourceSnapshot: "snapshots/snap-abc"}); err == nil {
		t.Fatal("want an error until CDI finishes")
	}
	if got := pvcName(source("ami-2"), "snapshot"); got != "snap-abc" {
		t.Fatalf("snapshot source %v, want snap-abc", got)
	}

	capture := hypervisor.ImageSpec{Name: "ami-3", Present: true, SourceMachine: "i-0123456789abcdef0"}
	if _, err := a.ConvergeImage(ctx, capture); err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("capture of a missing machine: got %v, want a transient error", err)
	}
	if exists(t, c, "DataVolume", ns, "ami-3") {
		t.Fatal("a capture of a missing machine created a DataVolume")
	}
	if err := a.ConvergeMachine(ctx, spec("i-0123456789abcdef0", hypervisor.PowerOn)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ConvergeImage(ctx, capture); err == nil {
		t.Fatal("want an error until CDI finishes")
	}
	if got := pvcName(source("ami-3"), "pvc"); got != "i-0123456789abcdef0-root" {
		t.Fatalf("capture source claim %v, want the machine's root disk", got)
	}
}

func TestImagePermanentErrors(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	a := imageAgent(c, ns)
	noSize := New(c, Config{Namespace: ns})
	for name, tc := range map[string]struct {
		a *Agent
		s hypervisor.ImageSpec
	}{
		"bad name":         {a, hypervisor.ImageSpec{Name: "Not_A_Name", Present: true, SourceURL: "https://x"}},
		"no source":        {a, hypervisor.ImageSpec{Name: "ami-4", Present: true}},
		"two sources":      {a, hypervisor.ImageSpec{Name: "ami-4", Present: true, SourceURL: "https://x", SourceVolume: "v"}},
		"host file":        {a, hypervisor.ImageSpec{Name: "ami-4", Present: true, SourcePath: "/var/disk.img"}},
		"url without size": {noSize, hypervisor.ImageSpec{Name: "ami-4", Present: true, SourceURL: "https://x"}},
	} {
		if _, err := tc.a.ConvergeImage(ctx, tc.s); !hypervisor.IsPermanent(err) {
			t.Errorf("%s: got %v, want a permanent error", name, err)
		}
	}
	if exists(t, c, "DataVolume", ns, "ami-4") {
		t.Fatal("a rejected spec created a DataVolume")
	}
}

func dataSourceLabels(t *testing.T, c client.Client, ns string) map[string]string {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(dsGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: imageID}, u); err != nil {
		t.Fatal(err)
	}
	return u.GetLabels()
}

// TestDataSourceCarriesItsOwner pins vws-compute#92: the DataSource carries
// the image's project and region, which vws-storage requires before it clones
// it. A DataSource made before the labels gets them with one patch, and a
// repeat writes nothing.
func TestDataSourceCarriesItsOwner(t *testing.T) {
	c, ns, w := newClient(t)
	ctx := context.Background()
	unowned := urlImage()
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, unowned)
	if l := dataSourceLabels(t, c, ns); l[LabelProject] != "" || l[LabelRegion] != "" {
		t.Fatalf("an image of no project got owner labels %v", l)
	}

	owned := unowned
	owned.Project, owned.Region = "a123456789012", "vws-1"
	before := w.n.Load()
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, owned)
	if got := w.n.Load() - before; got != 1 {
		t.Fatalf("labeling wrote %d times, want 1", got)
	}
	if l := dataSourceLabels(t, c, ns); l[LabelProject] != "a123456789012" || l[LabelRegion] != "vws-1" || l[LabelManaged] != "true" {
		t.Fatalf("labels %v", l)
	}
	before = w.n.Load()
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, owned)
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("repeat wrote %d times, want 0", got)
	}
}

func TestPublicImageDataSourceIsLabeledPublic(t *testing.T) {
	c, ns, w := newClient(t)
	ctx := context.Background()
	spec := urlImage()
	spec.Project, spec.Region = "a123456789012", "vws-1"
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, spec)
	if l := dataSourceLabels(t, c, ns); l[LabelPublic] != "" {
		t.Fatalf("private image labeled public: %v", l)
	}
	spec.Public = true
	before := w.n.Load()
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, spec)
	if got := w.n.Load() - before; got != 1 {
		t.Fatalf("public label wrote %d times, want 1", got)
	}
	if l := dataSourceLabels(t, c, ns); l[LabelPublic] != "true" || l[LabelProject] != "a123456789012" {
		t.Fatalf("labels %v", l)
	}
	before = w.n.Load()
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, spec)
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("repeat wrote %d times, want 0", got)
	}
}

// TestPublicLabelIsRemovedWhenTheImageIsNoLongerPublic pins the review of
// vws-compute#93: an image taken out of -public-images loses vws.io/public
// with one patch, keeps its owner labels, and a repeat writes nothing.
func TestPublicLabelIsRemovedWhenTheImageIsNoLongerPublic(t *testing.T) {
	c, ns, w := newClient(t)
	ctx := context.Background()
	spec := urlImage()
	spec.Project, spec.Region, spec.Public = "a123456789012", "vws-1", true
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, spec)
	if l := dataSourceLabels(t, c, ns); l[LabelPublic] != "true" {
		t.Fatalf("labels %v", l)
	}
	spec.Public = false
	before := w.n.Load()
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, spec)
	if got := w.n.Load() - before; got != 1 {
		t.Fatalf("unpublishing wrote %d times, want 1", got)
	}
	l := dataSourceLabels(t, c, ns)
	if _, has := l[LabelPublic]; has || l[LabelProject] != "a123456789012" || l[LabelRegion] != "vws-1" || l[LabelManaged] != "true" {
		t.Fatalf("labels %v", l)
	}
	before = w.n.Load()
	_, _ = imageAgent(c, ns).ConvergeImage(ctx, spec)
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("repeat wrote %d times, want 0", got)
	}
}

// TestDeletingAnImageAMachineClonesRevokesItsLabelsFirst pins the review of
// vws-compute#93 (ce6fc94): while a VirtualMachine still blocks removal, the
// DataSource already lost its authorization labels, so vws-storage clones it
// for nobody new; the retried removal writes nothing more.
func TestDeletingAnImageAMachineClonesRevokesItsLabelsFirst(t *testing.T) {
	c, ns, w := newClient(t)
	ctx := context.Background()
	a := imageAgent(c, ns)
	img := urlImage()
	img.Project, img.Region, img.Public = "a123456789012", "vws-1", true
	_, _ = a.ConvergeImage(ctx, img)
	m := spec("i-0123456789abcdef0", hypervisor.PowerOff)
	m.Image = imageID
	if err := a.ConvergeMachine(ctx, m); err != nil {
		t.Fatal(err)
	}
	gone := hypervisor.ImageSpec{Name: imageID}
	if _, err := a.ConvergeImage(ctx, gone); err == nil || hypervisor.IsPermanent(err) {
		t.Fatalf("got %v, want a transient refusal", err)
	}
	l := dataSourceLabels(t, c, ns)
	for _, k := range []string{LabelProject, LabelRegion, LabelPublic} {
		if _, has := l[k]; has {
			t.Fatalf("label %s kept while removal is blocked: %v", k, l)
		}
	}
	if l[LabelManaged] != "true" {
		t.Fatalf("managed label lost: %v", l)
	}
	before := w.n.Load()
	if _, err := a.ConvergeImage(ctx, gone); err == nil {
		t.Fatal("removal must still be refused")
	}
	if got := w.n.Load() - before; got != 0 {
		t.Fatalf("retried removal wrote %d times, want 0", got)
	}
}
