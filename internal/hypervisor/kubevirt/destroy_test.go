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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// vmRole reads Role vws-compute-vms before any envtest skip, so a missing
// verb fails even when kube-apiserver is absent.
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

func TestImageNotFoundPublishesReadyReasonAndDeletesRootDisk(t *testing.T) {
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
	if !slices.Equal(dvVerbs, []string{"delete"}) {
		t.Fatalf("datavolumes verbs = %v, want [delete]", dvVerbs)
	}
	if !slices.Contains(pvcVerbs, "get") || !slices.Contains(pvcVerbs, "delete") {
		t.Fatalf("persistentvolumeclaims verbs = %v, want get and delete", pvcVerbs)
	}

	c, ns, _ := newClient(t)
	ctx := context.Background()
	const inst = "i-0123456789abcdef0"
	root := inst + "-root"
	dv := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
	dv.SetGroupVersionKind(dvGVK)
	dv.SetNamespace(ns)
	dv.SetName(root)
	if err := c.Create(ctx, dv); err != nil {
		t.Fatal(err)
	}
	claim(t, c, ns, root, corev1.ReadWriteOnce)
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	// envtest stamps kubernetes.io/pvc-protection and does not run the
	// controller that clears it, so Delete would otherwise leave the claim.
	dropPVCProtection(t, c, ns, root)

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
