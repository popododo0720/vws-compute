package kubevirt

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

func vmRole(t *testing.T) rbacv1.Role {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "..", "deploy", "rbac", "kubevirt-role.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	d := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	for {
		var r rbacv1.Role
		if err := d.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if r.Kind == "Role" && r.Name == "vws-compute-vms" {
			return r
		}
	}
	t.Fatal("deploy/rbac/kubevirt-role.yaml has no Role vws-compute-vms")
	return rbacv1.Role{}
}

func TestVMRoleDeletesTheClonedRootDisk(t *testing.T) {
	role := vmRole(t)
	var dvVerbs, pvcVerbs []string
	for _, rule := range role.Rules {
		dv := len(rule.APIGroups) == 1 && rule.APIGroups[0] == "cdi.kubevirt.io" &&
			len(rule.Resources) == 1 && rule.Resources[0] == "datavolumes"
		if dv {
			dvVerbs = rule.Verbs
		}
		if slices.Contains(rule.Resources, "persistentvolumeclaims") {
			pvcVerbs = append(pvcVerbs, rule.Verbs...)
		}
	}
	if !slices.Equal(dvVerbs, []string{"get", "delete"}) {
		t.Fatalf("datavolumes verbs = %v, want [get delete]", dvVerbs)
	}
	if !slices.Contains(pvcVerbs, "get") || !slices.Contains(pvcVerbs, "delete") {
		t.Fatalf("persistentvolumeclaims verbs = %v, want get and delete", pvcVerbs)
	}
}

func TestImageNotFoundPublishesReadyReasonAndDeletesRootDisk(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	const inst = "i-0123456789abcdef0"
	root := inst + "-root"
	plantDV(t, c, ns, root, map[string]string{LabelManaged: "true", LabelInstance: inst})
	claim(t, c, ns, root, corev1.ReadWriteOnce)
	ownByDataVolume(t, c, ns, root)
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	// envtest stamps kubernetes.io/pvc-protection and does not run the
	// controller that clears it, so Delete would otherwise leave the claim.
	dropPVCProtection(t, c, ns, "pvc-root")

	a := New(c, Config{Namespace: ns, ImageNamespace: ns})
	absent := hypervisor.MachineSpec{Name: inst, Power: hypervisor.PowerAbsent}
	if err := a.ConvergeMachine(ctx, absent); err != nil {
		t.Fatal(err)
	}
	wantRootGone(t, c, ns, root)
	if err := getPVC(t, c, ns, "pvc-root"); err != nil {
		t.Fatalf("pvc-root: %v", err)
	}

	if err := a.ConvergeMachine(ctx, absent); err != nil {
		t.Fatalf("second destroy: %v", err)
	}
	wantRootGone(t, c, ns, root)
	if err := getPVC(t, c, ns, "pvc-root"); err != nil {
		t.Fatalf("pvc-root after second destroy: %v", err)
	}

	const missing = "i-0123456789abcdef1"
	err := a.ConvergeMachine(ctx, hypervisor.MachineSpec{
		Name: missing, Image: "ami-missing", VCPUs: 2, MemoryMiB: 2048, Power: hypervisor.PowerOn,
	})
	want := "machine " + missing + ": image ami-missing not found"
	if err == nil || err.Error() != want || hypervisor.CauseOf(err) != hypervisor.CauseImageNotFound ||
		!hypervisor.IsPermanent(err) {
		t.Fatalf("missing image = %v, want permanent ImageNotFound %q", err, want)
	}
	if _, err := get(t, c, "VirtualMachine", ns, missing); !apierrors.IsNotFound(err) {
		t.Fatalf("VirtualMachine get = %v, want NotFound", err)
	}
}

func TestTerminateLeavesAStorageClaimNamedLikeTheClone(t *testing.T) {
	c, ns, _ := newClient(t)
	ctx := context.Background()
	const inst = "i-0123456789abcdef0"
	root := inst + "-root"
	plantDV(t, c, ns, root, nil)
	claim(t, c, ns, root, corev1.ReadWriteOnce)
	dropPVCProtection(t, c, ns, root)

	a := New(c, Config{Namespace: ns, ImageNamespace: ns})
	if err := a.ConvergeMachine(ctx, hypervisor.MachineSpec{Name: inst, Power: hypervisor.PowerAbsent}); err != nil {
		t.Fatal(err)
	}
	if err := getPVC(t, c, ns, root); err != nil {
		t.Fatalf("storage claim %s: %v", root, err)
	}
	dv := &unstructured.Unstructured{}
	dv.SetGroupVersionKind(dvGVK)
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: root}, dv); err != nil {
		t.Fatalf("foreign DataVolume %s: %v", root, err)
	}
}

func TestTerminateDoesNotDeleteAClaimThatReplacedTheClone(t *testing.T) {
	raw, ns, _ := newClient(t)
	ctx := context.Background()
	const inst = "i-0123456789abcdef2"
	root := inst + "-root"
	claim(t, raw, ns, root, corev1.ReadWriteOnce)
	markClone(t, raw, ns, root, inst)

	c := &replaceClone{Client: raw, t: t, ns: ns, root: root}

	a := New(c, Config{Namespace: ns, ImageNamespace: ns})
	if err := a.ConvergeMachine(ctx, hypervisor.MachineSpec{Name: inst, Power: hypervisor.PowerAbsent}); err != nil {
		t.Fatal(err)
	}
	if err := getPVC(t, c, ns, root); err != nil {
		t.Fatalf("replacement claim %s: %v", root, err)
	}
	var pvc corev1.PersistentVolumeClaim
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: root}, &pvc); err != nil {
		t.Fatal(err)
	}
	if pvc.Labels[LabelManaged] == "true" {
		t.Fatal("the owned clone is still the claim")
	}
}

type replaceClone struct {
	client.Client
	t    *testing.T
	ns   string
	root string
	done bool
}

func (s *replaceClone) Get(
	ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption,
) error {
	if err := s.Client.Get(ctx, key, obj, opts...); err != nil || s.done || key.Name != s.root {
		return err
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || u.GetKind() != pvcGVK.Kind {
		return nil
	}
	s.done = true
	if err := s.Delete(ctx, u.DeepCopy()); err != nil {
		s.t.Fatal(err)
	}
	block := corev1.PersistentVolumeBlock
	fresh := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: s.root, Namespace: s.ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  &block,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if err := s.Create(ctx, fresh); err != nil {
		s.t.Fatal(err)
	}
	if err := s.Client.Get(ctx, key, fresh); err != nil {
		s.t.Fatal(err)
	}
	fresh.Finalizers = nil
	if err := s.Update(ctx, fresh); err != nil {
		s.t.Fatal(err)
	}
	return nil
}

func plantDV(t *testing.T, c client.Client, ns, name string, labels map[string]string) {
	t.Helper()
	dv := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
	dv.SetGroupVersionKind(dvGVK)
	dv.SetNamespace(ns)
	dv.SetName(name)
	dv.SetLabels(labels)
	if err := c.Create(context.Background(), dv); err != nil {
		t.Fatal(err)
	}
}

func ownByDataVolume(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	dv := &unstructured.Unstructured{}
	dv.SetGroupVersionKind(dvGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, dv); err != nil {
		t.Fatal(err)
	}
	var pvc corev1.PersistentVolumeClaim
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &pvc); err != nil {
		t.Fatal(err)
	}
	pvc.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: dvGVK.GroupVersion().String(),
		Kind:       dvGVK.Kind,
		Name:       name,
		UID:        dv.GetUID(),
	}}
	pvc.Finalizers = nil
	if err := c.Update(context.Background(), &pvc); err != nil {
		t.Fatal(err)
	}
}

func markClone(t *testing.T, c client.Client, ns, name, instance string) {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &pvc); err != nil {
		t.Fatal(err)
	}
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}
	pvc.Labels[LabelManaged] = "true"
	pvc.Labels[LabelInstance] = instance
	pvc.Finalizers = nil
	if err := c.Update(context.Background(), &pvc); err != nil {
		t.Fatal(err)
	}
}

func wantRootGone(t *testing.T, c client.Client, ns, root string) {
	t.Helper()
	dv := &unstructured.Unstructured{}
	dv.SetGroupVersionKind(dvGVK)
	err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: root}, dv)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("DataVolume %s: %v, want NotFound", root, err)
	}
	if err := getPVC(t, c, ns, root); !apierrors.IsNotFound(err) {
		t.Fatalf("PersistentVolumeClaim %s: %v, want NotFound", root, err)
	}
}

func dropPVCProtection(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &pvc); err != nil {
		t.Fatal(err)
	}
	if len(pvc.Finalizers) == 0 {
		return
	}
	pvc.Finalizers = nil
	if err := c.Update(context.Background(), &pvc); err != nil {
		t.Fatal(err)
	}
}

func getPVC(t *testing.T, c client.Client, ns, name string) error {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	return c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &pvc)
}
