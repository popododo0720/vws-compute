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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
func newClient(t *testing.T) (client.Client, string, *writes) {
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
			return c.Create(ctx, o, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, o client.Object, p client.Patch, opts ...client.PatchOption) error {
			count()
			return c.Patch(ctx, o, p, opts...)
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
