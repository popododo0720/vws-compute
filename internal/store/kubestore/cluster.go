package kubestore

import (
	"fmt"

	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// Open connects to a cluster. An empty kubeconfig uses the in-cluster service
// account, then $KUBECONFIG, then the default kubeconfig path. It does not
// create the namespace, and it does not repair it: Lead does, on the replica
// that holds the Lease.
func Open(kubeconfig, namespace string, legacyImageScope compute.ImageScope) (*Store, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	return New(c, namespace).WithLegacyImageScope(legacyImageScope), nil
}
