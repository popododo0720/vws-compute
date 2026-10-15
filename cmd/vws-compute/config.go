package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// dnsLabel is a Kubernetes namespace name (RFC 1123 label, at most 63).
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// validate rejects malformed values at the boundary, before they reach a
// listener or a Kubernetes request.
func (c config) validate() error {
	if _, _, err := net.SplitHostPort(c.listen); err != nil {
		return fmt.Errorf("-listen %q: %w", c.listen, err)
	}
	if _, _, err := net.SplitHostPort(c.http); c.http != "" && err != nil {
		return fmt.Errorf("-http %q: %w", c.http, err)
	}
	for _, ns := range []struct{ flag, v string }{
		{"-namespace", c.namespace}, {"-vm-namespace", c.vmNamespace}, {"-image-namespace", c.imageNamespace},
	} {
		if ns.v != "" && !dnsLabel.MatchString(ns.v) {
			return fmt.Errorf("%s %q is not a valid namespace name", ns.flag, ns.v)
		}
	}
	return nil
}

const (
	hypervisorFake     = "fake"
	hypervisorKubeVirt = "kubevirt"
)

// config is the parsed command line. A flag wins over its environment
// variable, and the environment wins over the built-in default.
type config struct {
	listen         string
	http           string
	namespace      string
	kubeconfig     string
	seedImages     string
	storage        string
	hypervisor     string
	vmNamespace    string
	imageNamespace string
	// legacyImageScope owns images stored without a project and region: those
	// written before #92 and seed images.
	legacyImageScope compute.ImageScope
	// publicImages are readable by every project in their region.
	publicImages compute.PublicImages
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
	fs.StringVar(&c.http, "http", env("VWS_HEALTH_ADDR", ""), "address to serve /healthz, /readyz, and /metrics on; empty serves none (env VWS_HEALTH_ADDR)")
	fs.StringVar(&c.namespace, "namespace", env("VWS_STORE_NAMESPACE", ""), "Kubernetes namespace that holds the ConfigMap store; empty keeps all state in memory (env VWS_STORE_NAMESPACE)")
	fs.StringVar(&c.kubeconfig, "kubeconfig", "", "kubeconfig path; empty uses the in-cluster service account, then $KUBECONFIG and the default path")
	fs.StringVar(&c.seedImages, "seed-images", "", "JSON file of disk images to register before serving; empty registers none")
	fs.StringVar(&c.storage, "storage", "", "vws-storage gRPC address CreateInstance verifies volumes against; empty makes CreateInstance answer DEPENDENCY_MISSING")
	fs.StringVar(&c.hypervisor, "hypervisor", env("VWS_HYPERVISOR", hypervisorFake), "fake or kubevirt (env VWS_HYPERVISOR)")
	fs.StringVar(&c.vmNamespace, "vm-namespace", env("VWS_VM_NAMESPACE", "vws-vms"), "namespace of VirtualMachines, VirtualMachineInstances, and volume PVCs (env VWS_VM_NAMESPACE)")
	fs.StringVar(&c.imageNamespace, "image-namespace", env("VWS_IMAGE_NAMESPACE", "vws-images"), "namespace of CDI DataVolumes and DataSources (env VWS_IMAGE_NAMESPACE)")
	legacyScope := fs.String("legacy-image-scope", env("VWS_LEGACY_IMAGE_SCOPE", ""), "projects/{project}/regions/{region} that owns images stored without one (pre-#92 records, seed images); empty leaves them in no project, invisible to every caller (env VWS_LEGACY_IMAGE_SCOPE)")
	publicIDs := fs.String("public-images", env("VWS_PUBLIC_IMAGES", ""), "comma-separated image ids readable by every project in the image's region, such as seed images; changing or deleting them stays with the owner (env VWS_PUBLIC_IMAGES)")
	if err := fs.Parse(args); err != nil {
		return config{}, fmt.Errorf("flags: %w", err)
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *legacyScope != "" {
		parts := strings.Split(*legacyScope, "/")
		if len(parts) != 4 || parts[0] != "projects" || parts[2] != "regions" {
			return config{}, fmt.Errorf("-legacy-image-scope %q is not projects/*/regions/*", *legacyScope)
		}
		sc, err := compute.NewImageScope(parts[1], parts[3])
		if err != nil {
			return config{}, fmt.Errorf("-legacy-image-scope: %w", err)
		}
		c.legacyImageScope = sc
	}
	var ids []string
	for _, id := range strings.Split(*publicIDs, ",") {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	pub, err := compute.NewPublicImages(ids)
	if err != nil {
		return config{}, fmt.Errorf("-public-images: %w", err)
	}
	c.publicImages = pub
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
	if err := c.validate(); err != nil {
		return config{}, err
	}
	return c, nil
}
