package kubestore_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/store/kubestore"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
)

func TestPingReachable(t *testing.T) {
	if err := kubetest.Store(t).Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPingUnreachable(t *testing.T) {
	// Port 1 on loopback refuses connections. The REST mapper is lazy, so
	// building the client does not contact the server.
	c, err := client.New(&rest.Config{Host: "http://127.0.0.1:1"}, client.Options{})
	if err != nil {
		t.Skipf("client needs discovery: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := kubestore.New(c, "ns").Ping(ctx); err == nil {
		t.Fatal("Ping against a closed port returned nil")
	}
}
