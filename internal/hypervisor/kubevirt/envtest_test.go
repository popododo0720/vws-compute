package kubevirt

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// env is one apiserver and etcd shared by this package's tests. It is nil when KUBEBUILDER_ASSETS is unset, and those tests skip.
var env *envtest.Environment

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}
	env = &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("testdata", "crds")}, ErrorIfCRDPathMissing: true}
	if _, err := env.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	os.Exit(code)
}

// writes counts the mutating calls a client made.
type writes struct{ n atomic.Int64 }

// newClient returns a client for the shared apiserver, and a namespace the
// test owns, so tests do not see each other's objects. Every create, patch,
// update, and delete through the client is counted.
//
// envtest runs no KubeVirt controller, so newClient stands in for it: after a
// VirtualMachine is written with runStrategy Always it makes a Running, Ready
// VirtualMachineInstance, and with Halted it deletes it. Those writes are not
// counted. Tests of the readiness check itself use newBareClient.
func newClient(t *testing.T) (client.Client, string, *writes) {
	t.Helper()
	return newClientWith(t, true)
}

// newBareClient is newClient without the stand-in KubeVirt controller: no
// VirtualMachineInstance appears unless the test makes one.
func newBareClient(t *testing.T) (client.Client, string, *writes) {
	t.Helper()
	return newClientWith(t, false)
}

func newClientWith(t *testing.T, kubevirt bool) (client.Client, string, *writes) {
	t.Helper()
	if env == nil {
		t.Skip("KUBEBUILDER_ASSETS is unset; envtest needs kube-apiserver and etcd")
	}
	raw, err := client.NewWithWatch(env.Config, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ns := strings.ToLower(strings.ReplaceAll(t.Name(), "_", "-"))
	ns = strings.ReplaceAll(ns, "/", "-")
	if err := raw.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	w := &writes{}
	count := func() { w.n.Add(1) }
	return interceptor.NewClient(raw, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.CreateOption) error {
			count()
			if err := c.Create(ctx, o, opts...); err != nil {
				return err
			}
			if kubevirt {
				simulateKubeVirt(ctx, t, c, o)
			}
			return nil
		},
		Patch: func(ctx context.Context, c client.WithWatch, o client.Object, p client.Patch, opts ...client.PatchOption) error {
			count()
			if err := c.Patch(ctx, o, p, opts...); err != nil {
				return err
			}
			if kubevirt {
				simulateKubeVirt(ctx, t, c, o)
			}
			return nil
		},
		Update: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.UpdateOption) error {
			count()
			return c.Update(ctx, o, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, o client.Object, opts ...client.DeleteOption) error {
			count()
			return c.Delete(ctx, o, opts...)
		},
	}), ns, w
}

// simulateKubeVirt does what KubeVirt would after a VirtualMachine write: an
// instance for runStrategy Always that is up at once, none for Halted.
func simulateKubeVirt(ctx context.Context, t *testing.T, c client.Client, o client.Object) {
	t.Helper()
	vm, ok := o.(*unstructured.Unstructured)
	if !ok || vm.GetKind() != "VirtualMachine" {
		return
	}
	run, _, err := unstructured.NestedString(vm.Object, "spec", "runStrategy")
	if err != nil {
		t.Errorf("runStrategy: %v", err)
		return
	}
	if run != "Always" {
		vmi := new(unstructured.Unstructured)
		vmi.SetGroupVersionKind(vmiGVK)
		vmi.SetNamespace(vm.GetNamespace())
		vmi.SetName(vm.GetName())
		if err := c.Delete(ctx, vmi); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete VMI: %v", err)
		}
		return
	}
	setVMI(ctx, t, c, vm.GetNamespace(), vm.GetName(), "Running", "True")
}

// setVMI makes the named VirtualMachineInstance exist with the given phase
// and Ready condition status. An empty ready leaves out the condition.
func setVMI(ctx context.Context, t *testing.T, c client.Client, ns, name, phase, ready string) {
	t.Helper()
	vmi := new(unstructured.Unstructured)
	vmi.SetGroupVersionKind(vmiGVK)
	err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, vmi)
	if apierrors.IsNotFound(err) {
		vmi = new(unstructured.Unstructured)
		vmi.SetGroupVersionKind(vmiGVK)
		vmi.SetNamespace(ns)
		vmi.SetName(name)
		err = c.Create(ctx, vmi)
	}
	if err != nil {
		t.Errorf("make VMI %s: %v", name, err)
		return
	}
	status := map[string]any{"phase": phase}
	if ready != "" {
		status["conditions"] = []any{map[string]any{"type": "Ready", "status": ready}}
	}
	vmi.Object["status"] = status
	if err := c.Status().Update(ctx, vmi); err != nil {
		t.Errorf("set VMI %s status: %v", name, err)
	}
}
