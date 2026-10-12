package kubevirt

import (
	"context"
	"testing"
)

func TestPingBothNamespaces(t *testing.T) {
	c, ns, _ := newClient(t)
	if err := New(c, Config{Namespace: ns}).Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}
