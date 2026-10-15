package kubevirt

import (
	"context"
	"fmt"
	"path"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

var (
	dvGVK  = schema.GroupVersionKind{Group: "cdi.kubevirt.io", Version: "v1beta1", Kind: "DataVolume"}
	dsGVK  = schema.GroupVersionKind{Group: "cdi.kubevirt.io", Version: "v1beta1", Kind: "DataSource"}
	pvcGVK = schema.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"}
)

// An image is two CDI objects with the image ID as their name. The
// DataVolume imports or clones the disk into a claim of the same name. The
// DataSource points at that claim, and a machine's root disk clones from it.

// ConvergeImage makes the image's DataVolume and DataSource exist and returns
// the size of the claim CDI filled. While CDI is still working it returns a
// transient error, and the caller asks again. Removing an image a
// VirtualMachine still clones from fails and leaves it in place.
func (a *Agent) ConvergeImage(ctx context.Context, spec hypervisor.ImageSpec) (int64, error) {
	if errs := validation.IsDNS1123Label(spec.Name); len(errs) > 0 {
		return 0, hypervisor.Permanent(fmt.Errorf("image %q: not a valid object name: %s", spec.Name, errs[0]))
	}
	if !spec.Present {
		return 0, a.removeImage(ctx, spec.Name)
	}
	source, err := a.imageSource(ctx, spec)
	if err != nil {
		return 0, err
	}
	if err := a.ensure(ctx, dvGVK, spec.Name, map[string]any{"source": source, "storage": a.imageStorage(spec)}, nil, nil); err != nil {
		return 0, err
	}
	pvc := map[string]any{"name": spec.Name, "namespace": a.cfg.ImageNamespace}
	if err := a.ensure(ctx, dsGVK, spec.Name, map[string]any{"source": map[string]any{"pvc": pvc}}, ownerLabels(spec), authLabels); err != nil {
		return 0, err
	}
	return a.imageSize(ctx, spec.Name)
}

func (a *Agent) object(gvk schema.GroupVersionKind, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(a.cfg.ImageNamespace)
	u.SetName(name)
	return u
}

// LabelProject and LabelRegion name the owner of an image DataSource.
// vws-storage clones a DataSource only for the project and region on these
// labels (vws-storage#89).
const (
	LabelProject = "vws.io/project"
	LabelRegion  = "vws.io/region"
	LabelPublic  = "vws.io/public"
)

// ownerLabels are the owner labels of the image's DataSource, with
// vws.io/public=true for a public image. An image of no
// project has none, so vws-storage clones it for nobody.
func ownerLabels(spec hypervisor.ImageSpec) map[string]string {
	if spec.Project == "" {
		return nil
	}
	l := map[string]string{LabelProject: spec.Project, LabelRegion: spec.Region}
	if spec.Public {
		l[LabelPublic] = "true"
	}
	return l
}

// authLabels are the DataSource labels vws-storage authorizes a clone by.
// Compute owns them: one that the image no longer wants, such as
// vws.io/public after the image left -public-images, is removed.
var authLabels = []string{LabelProject, LabelRegion, LabelPublic}

// ensure creates the object unless it exists, with labels besides the
// managed one. An existing image object keeps its spec, since an image's
// source never changes. Its labels are brought to labels with one patch:
// one it lacks or holds with another value is set, and a key in owned that
// labels lacks is removed, so DataSources made before owner labels get them
// and an image made private loses vws.io/public. A repeat writes nothing.
func (a *Agent) ensure(ctx context.Context, gvk schema.GroupVersionKind, name string, spec map[string]any, labels map[string]string, owned []string) error {
	have := a.object(gvk, name)
	err := a.c.Get(ctx, client.ObjectKeyFromObject(have), have)
	switch {
	case err == nil:
		cur := have.GetLabels()
		if cur[LabelManaged] != "true" {
			return hypervisor.Permanent(fmt.Errorf("image %s: a %s with that name exists and is not managed here", name, gvk.Kind))
		}
		stale := false
		for k, v := range labels {
			if cur[k] != v {
				stale = true
			}
		}
		for _, k := range owned {
			if _, want := labels[k]; !want {
				if _, has := cur[k]; has {
					stale = true
				}
			}
		}
		if !stale {
			return nil
		}
		patch := client.MergeFrom(have.DeepCopy())
		for k, v := range labels {
			cur[k] = v
		}
		for _, k := range owned {
			if _, want := labels[k]; !want {
				delete(cur, k)
			}
		}
		have.SetLabels(cur)
		if err := a.c.Patch(ctx, have, patch); err != nil {
			return fmt.Errorf("image %s: label %s: %w", name, gvk.Kind, err)
		}
		return nil
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("image %s: get %s: %w", name, gvk.Kind, err)
	}
	want := a.object(gvk, name)
	want.Object["spec"] = spec
	all := map[string]string{LabelManaged: "true"}
	for k, v := range labels {
		all[k] = v
	}
	want.SetLabels(all)
	// Losing a create race means another caller made the same object.
	if err := client.IgnoreAlreadyExists(a.c.Create(ctx, want)); err != nil {
		return fmt.Errorf("image %s: create %s: %w", name, gvk.Kind, err)
	}
	return nil
}

// imageSource maps exactly one ImageSpec source to a DataVolume source.
func (a *Agent) imageSource(ctx context.Context, spec hypervisor.ImageSpec) (map[string]any, error) {
	n := 0
	for _, s := range []string{spec.SourceURL, spec.SourcePath, spec.SourceMachine, spec.SourceVolume, spec.SourceSnapshot} {
		if s != "" {
			n++
		}
	}
	if n != 1 {
		return nil, hypervisor.Permanent(fmt.Errorf("image %s: need exactly one source", spec.Name))
	}
	ns := a.cfg.Namespace
	switch {
	case spec.SourceURL != "":
		if a.cfg.ImportSize == "" {
			return nil, hypervisor.Permanent(fmt.Errorf("image %s: importing from a URL needs a configured import size", spec.Name))
		}
		return map[string]any{"http": map[string]any{"url": spec.SourceURL}}, nil
	case spec.SourcePath != "":
		return nil, hypervisor.Permanent(fmt.Errorf("image %s: a disk file on a host has no meaning in Kubernetes", spec.Name))
	case spec.SourceMachine != "":
		root := a.object(vmGVK, spec.SourceMachine)
		root.SetNamespace(ns)
		if err := a.c.Get(ctx, client.ObjectKeyFromObject(root), root); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("image %s: source machine %s does not exist yet", spec.Name, spec.SourceMachine)
			}
			return nil, fmt.Errorf("image %s: get source machine %s: %w", spec.Name, spec.SourceMachine, err)
		}
		return map[string]any{"pvc": map[string]any{"namespace": ns, "name": spec.SourceMachine + "-root"}}, nil
	case spec.SourceVolume != "":
		return map[string]any{"pvc": map[string]any{"namespace": ns, "name": path.Base(spec.SourceVolume)}}, nil
	default:
		return map[string]any{"snapshot": map[string]any{"namespace": ns, "name": path.Base(spec.SourceSnapshot)}}, nil
	}
}

func (a *Agent) imageStorage(spec hypervisor.ImageSpec) map[string]any {
	if spec.SourceURL == "" {
		return map[string]any{}
	}
	return map[string]any{"resources": map[string]any{"requests": map[string]any{"storage": a.cfg.ImportSize}}}
}

// imageSize reads the size from the claim CDI bound for the DataVolume. A
// DataVolume carries no size of its own, and nothing here estimates one.
func (a *Agent) imageSize(ctx context.Context, name string) (int64, error) {
	dv := a.object(dvGVK, name)
	if err := a.c.Get(ctx, client.ObjectKeyFromObject(dv), dv); err != nil {
		return 0, fmt.Errorf("image %s: get DataVolume: %w", name, err)
	}
	phase, _, err := unstructured.NestedString(dv.Object, "status", "phase")
	if err != nil {
		return 0, hypervisor.Permanent(fmt.Errorf("image %s: DataVolume status.phase: %w", name, err))
	}
	if phase != "Succeeded" {
		return 0, fmt.Errorf("image %s: DataVolume is %q, not Succeeded", name, phase)
	}
	pvc := a.object(pvcGVK, name)
	if err := a.c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
		return 0, fmt.Errorf("image %s: get claim: %w", name, err)
	}
	have, found, err := unstructured.NestedString(pvc.Object, "status", "capacity", "storage")
	if err != nil {
		return 0, hypervisor.Permanent(fmt.Errorf("image %s: claim capacity: %w", name, err))
	}
	if !found {
		return 0, fmt.Errorf("image %s: claim has no capacity yet", name)
	}
	q, err := resource.ParseQuantity(have)
	if err != nil {
		return 0, hypervisor.Permanent(fmt.Errorf("image %s: claim capacity %q: %w", name, have, err))
	}
	return q.Value(), nil
}

func (a *Agent) removeImage(ctx context.Context, name string) error {
	// The authorization labels go first, so an image being deregistered is
	// cloned by nobody new even while a VirtualMachine still blocks its
	// removal and the deletion is retried (#93).
	if err := a.revokeAuthLabels(ctx, name); err != nil {
		return err
	}
	users, err := a.imageUsers(ctx, name)
	if err != nil {
		return err
	}
	if len(users) > 0 {
		return fmt.Errorf("image %s: still the root disk source of %v", name, users)
	}
	// The DataSource goes first so nothing new starts cloning from a disk
	// that is about to disappear.
	for _, gvk := range []schema.GroupVersionKind{dsGVK, dvGVK} {
		obj := a.object(gvk, name)
		if err := client.IgnoreNotFound(a.c.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground))); err != nil {
			return fmt.Errorf("image %s: delete %s: %w", name, gvk.Kind, err)
		}
	}
	return nil
}

// revokeAuthLabels removes the authorization labels from the image's managed
// DataSource with one patch. A missing DataSource, one not managed here, or
// one without the labels is left alone, so a repeat writes nothing.
func (a *Agent) revokeAuthLabels(ctx context.Context, name string) error {
	ds := a.object(dsGVK, name)
	if err := a.c.Get(ctx, client.ObjectKeyFromObject(ds), ds); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("image %s: get DataSource: %w", name, err)
	}
	cur := ds.GetLabels()
	if cur[LabelManaged] != "true" {
		return nil
	}
	patch := client.MergeFrom(ds.DeepCopy())
	changed := false
	for _, k := range authLabels {
		if _, has := cur[k]; has {
			delete(cur, k)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	ds.SetLabels(cur)
	if err := a.c.Patch(ctx, ds, patch); err != nil {
		return fmt.Errorf("image %s: revoke DataSource labels: %w", name, err)
	}
	return nil
}

// imageUsers names the VirtualMachines whose root disk clones from the image.
func (a *Agent) imageUsers(ctx context.Context, name string) ([]string, error) {
	vms := &unstructured.UnstructuredList{}
	vms.SetGroupVersionKind(vmGVK.GroupVersion().WithKind("VirtualMachineList"))
	if err := a.c.List(ctx, vms, client.InNamespace(a.cfg.Namespace)); err != nil {
		return nil, fmt.Errorf("list VirtualMachines: %w", err)
	}
	var users []string
	for _, vm := range vms.Items {
		templates, _, _ := unstructured.NestedSlice(vm.Object, "spec", "dataVolumeTemplates") //nolint:errcheck // a template of another shape is not one of ours, so it does not use the image
		for _, t := range templates {
			tmpl, _ := t.(map[string]any)                                  //nolint:errcheck // a template of another shape is not one of ours, so it does not use the image
			ref, _, _ := unstructured.NestedMap(tmpl, "spec", "sourceRef") //nolint:errcheck // a template of another shape is not one of ours, so it does not use the image
			if ref["kind"] == "DataSource" && ref["name"] == name && ref["namespace"] == a.cfg.ImageNamespace {
				users = append(users, vm.GetName())
				break
			}
		}
	}
	return users, nil
}
