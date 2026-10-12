package kubestore

import (
	"context"
	"fmt"

	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Open connects to a cluster and repairs the namespace. An empty kubeconfig
// uses the in-cluster service account, then $KUBECONFIG, then the default
// kubeconfig path. It does not create the namespace.
func Open(ctx context.Context, kubeconfig, namespace string) (*Store, Repaired, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, Repaired{}, fmt.Errorf("kubeconfig: %w", err)
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, Repaired{}, fmt.Errorf("kubernetes client: %w", err)
	}
	s := New(c, namespace)
	r, err := s.Repair(ctx)
	if err != nil {
		return nil, r, err
	}
	return s, r, nil
}
