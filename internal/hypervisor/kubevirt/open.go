package kubevirt

import (
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Open connects to a cluster and returns an agent. An empty kubeconfig uses
// the in-cluster service account, then $KUBECONFIG and the default path. The
// client is uncached, so the agent needs no watch permission.
func Open(kubeconfig string, cfg Config) (*Agent, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return nil, err
	}
	c, err := client.New(rc, client.Options{})
	if err != nil {
		return nil, err
	}
	return New(c, cfg), nil
}
