package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
)

const (
	hypervisorFake     = "fake"
	hypervisorKubeVirt = "kubevirt"
)

// config is the parsed command line. A flag wins over its environment
// variable, and the environment wins over the built-in default.
type config struct {
	listen         string
	namespace      string
	kubeconfig     string
	seedImages     string
	storage        string
	hypervisor     string
	vmNamespace    string
	imageNamespace string
}

func parseConfig(args []string, getenv func(string) string, errOut io.Writer) (config, error) {
	env := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}
	var c config
	fs := flag.NewFlagSet("vws-compute", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.StringVar(&c.listen, "listen", "127.0.0.1:50051", "address to serve gRPC on")
	fs.StringVar(&c.namespace, "namespace", env("VWS_STORE_NAMESPACE", ""), "Kubernetes namespace that holds the ConfigMap store; empty keeps all state in memory (env VWS_STORE_NAMESPACE)")
	fs.StringVar(&c.kubeconfig, "kubeconfig", "", "kubeconfig path; empty uses the in-cluster service account, then $KUBECONFIG and the default path")
	fs.StringVar(&c.seedImages, "seed-images", "", "JSON file of disk images to register before serving; empty registers none")
	fs.StringVar(&c.storage, "storage", "", "vws-storage gRPC address CreateInstance verifies volumes against; empty makes CreateInstance answer DEPENDENCY_MISSING")
	fs.StringVar(&c.hypervisor, "hypervisor", env("VWS_HYPERVISOR", hypervisorFake), "fake or kubevirt (env VWS_HYPERVISOR)")
	fs.StringVar(&c.vmNamespace, "vm-namespace", env("VWS_VM_NAMESPACE", "vws-vms"), "namespace of VirtualMachines, VirtualMachineInstances, and volume PVCs (env VWS_VM_NAMESPACE)")
	fs.StringVar(&c.imageNamespace, "image-namespace", env("VWS_IMAGE_NAMESPACE", "vws-images"), "namespace of CDI DataVolumes and DataSources (env VWS_IMAGE_NAMESPACE)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	switch c.hypervisor {
	case hypervisorFake:
	case hypervisorKubeVirt:
		if c.namespace == "" {
			return config{}, errors.New("-hypervisor=kubevirt needs -namespace for the ConfigMap store")
		}
		if c.vmNamespace == "" || c.imageNamespace == "" {
			return config{}, errors.New("-vm-namespace and -image-namespace must not be empty")
		}
	default:
		return config{}, fmt.Errorf("unknown -hypervisor %q, want fake or kubevirt", c.hypervisor)
	}
	return c, nil
}
