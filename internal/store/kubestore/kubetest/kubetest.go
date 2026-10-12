// Package kubetest gives tests a kubestore over a real kube-apiserver and
// etcd, started by envtest. It is test support, like storetest, and the one
// other package besides storetest that may import "testing".
//
// A package that uses it calls Run from TestMain. When KUBEBUILDER_ASSETS is
// unset, no apiserver starts and Namespace skips the test.
package kubetest

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/popododo0720/vws-compute/internal/store/kubestore"
)

// env is the apiserver shared by one test binary. It is nil when envtest is
// unavailable.
var env *envtest.Environment

// Run starts the apiserver, runs the tests, stops it, and returns the exit
// code for os.Exit.
func Run(m *testing.M) int {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return m.Run()
	}
	env = &envtest.Environment{}
	if _, err := env.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		return 1
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	return code
}

var namespaces atomic.Int64

// Namespace returns a client for the shared apiserver and a namespace the
// caller owns, so tests do not see each other's objects. It skips the test
// when there is no apiserver.
func Namespace(t testing.TB) (client.WithWatch, string) {
	t.Helper()
	if env == nil {
		t.Skip("KUBEBUILDER_ASSETS is unset; envtest needs kube-apiserver and etcd")
	}
	c, err := client.NewWithWatch(env.Config, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ns := fmt.Sprintf("ks-%d-%d", os.Getpid(), namespaces.Add(1))
	if err := c.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	return c, ns
}

// Open returns a repaired store over ns. Calling it again for the same
// namespace is a process restart: a new store over the same cluster state.
func Open(t testing.TB, c client.Client, ns string) *kubestore.Store {
	t.Helper()
	s := kubestore.New(c, ns)
	if _, err := s.Repair(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// Store returns a new repository over a fresh namespace, repaired and ready.
func Store(t testing.TB) *kubestore.Store {
	t.Helper()
	c, ns := Namespace(t)
	return Open(t, c, ns)
}
