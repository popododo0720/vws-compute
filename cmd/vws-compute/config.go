package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor/kubevirt"
)

// dnsLabel is a Kubernetes namespace name (RFC 1123 label, at most 63).
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// dnsSubdomain is a Kubernetes DNS subdomain name (RFC 1123), the shape a
// StorageClass name has. Kubernetes caps the whole name at 253 characters and
// does not cap a dot-separated part at 63.
var dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// imageAccessModes are the values a DataVolume's accessModes accepts.
var imageAccessModes = map[string]bool{
	"ReadWriteOnce":    true,
	"ReadOnlyMany":     true,
	"ReadWriteMany":    true,
	"ReadWriteOncePod": true,
}

// validStorageClass reports whether name is a Kubernetes DNS subdomain, the
// shape a StorageClass name has.
func validStorageClass(name string) bool {
	return len(name) <= 253 && dnsSubdomain.MatchString(name)
}

// validate rejects malformed values at the boundary, before they reach a
// listener or a Kubernetes request.
func (c config) validate() error {
	if _, _, err := net.SplitHostPort(c.listen); err != nil {
		return fmt.Errorf("-listen %q: %w", c.listen, err)
	}
	for _, ns := range []struct{ flag, v string }{
		{"-namespace", c.namespace},
		{"-vm-namespace", c.vmNamespace},
		{"-image-namespace", c.imageNamespace},
		{"-nad-namespace", c.nadNamespace},
	} {
		if ns.v != "" && !dnsLabel.MatchString(ns.v) {
			return fmt.Errorf("%s %q is not a valid namespace name", ns.flag, ns.v)
		}
	}
	if !validStorageClass(c.imageStorageClass) {
		return fmt.Errorf("-image-storage-class %q is not a valid StorageClass name", c.imageStorageClass)
	}
	if !imageAccessModes[c.imageAccessMode] {
		return fmt.Errorf("-image-access-mode %q is not a known access mode", c.imageAccessMode)
	}
	if !dnsSubdomain.MatchString(c.leaseName) || len(c.leaseName) > 253 {
		return fmt.Errorf("-lease-name %q is not a valid Lease name", c.leaseName)
	}
	// client-go's leaderelection needs renewDeadline > 1.2 * retryPeriod. A
	// deposed leader stops at most renewDeadline + retryPeriod after its last
	// renew, and a follower takes over no sooner than leaseDuration after
	// it, so leaseDuration must exceed that sum for terms never to overlap.
	// The Lease stores whole seconds, so a fraction would be truncated.
	if c.retryPeriod <= 0 || c.renewDeadline <= time.Duration(1.2*float64(c.retryPeriod)) ||
		c.leaseDuration%time.Second != 0 || c.leaseDuration <= c.renewDeadline+c.retryPeriod {
		return fmt.Errorf("lease timing %s/%s/%s: want whole seconds of -lease-duration > "+
			"-lease-renew-deadline + -lease-retry-period, and -lease-renew-deadline > 1.2 * -lease-retry-period > 0",
			c.leaseDuration, c.renewDeadline, c.retryPeriod)
	}
	return nil
}

// The Lease defaults. The name is the vws-deploy chart's service key, whose
// leader Role grants get and update on that Lease only. The timing is the
// client-go default.
const (
	defaultLeaseName     = "compute"
	defaultLeaseDuration = 15 * time.Second
	defaultRenewDeadline = 10 * time.Second
	defaultRetryPeriod   = 2 * time.Second
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
	// nadNamespace holds vws-network's per-subnet NetworkAttachmentDefinitions.
	nadNamespace string
	// imageStorageClass and imageAccessMode select the target claim of image
	// DataVolumes.
	imageStorageClass string
	imageAccessMode   string
	// legacyImageScope owns images stored without a project and region: those
	// written before #92 and seed images.
	legacyImageScope compute.ImageScope
	// publicImages are readable by every project in their region.
	publicImages compute.PublicImages
	// leaseName is the Lease that replicas sharing -namespace elect the
	// leader on. leaseDuration, renewDeadline, and retryPeriod time it.
	leaseName                                 string
	leaseDuration, renewDeadline, retryPeriod time.Duration
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
	fs.StringVar(&c.nadNamespace, "nad-namespace", env("VWS_NAD_NAMESPACE", ""),
		"namespace of the NetworkAttachmentDefinition vws-network makes per VPC subnet (its -kube-nad-namespace); "+
			"an instance's second and later overlay interfaces attach through it; empty refuses them (env VWS_NAD_NAMESPACE)")
	fs.StringVar(&c.imageNamespace, "image-namespace", env("VWS_IMAGE_NAMESPACE", "vws-images"), "namespace of CDI DataVolumes and DataSources (env VWS_IMAGE_NAMESPACE)")
	fs.StringVar(&c.imageStorageClass, "image-storage-class",
		env("VWS_IMAGE_STORAGE_CLASS", kubevirt.DefaultImageStorageClass),
		"StorageClass of image DataVolumes (env VWS_IMAGE_STORAGE_CLASS)")
	fs.StringVar(&c.imageAccessMode, "image-access-mode",
		env("VWS_IMAGE_ACCESS_MODE", kubevirt.DefaultImageAccessMode),
		"access mode of image DataVolumes (ReadWriteOnce, ReadOnlyMany, ReadWriteMany, "+
			"ReadWriteOncePod), env VWS_IMAGE_ACCESS_MODE")
	legacyScope := fs.String("legacy-image-scope", env("VWS_LEGACY_IMAGE_SCOPE", ""), "projects/{project}/regions/{region} that owns images stored without one (pre-#92 records, seed images); empty leaves them in no project, invisible to every caller (env VWS_LEGACY_IMAGE_SCOPE)")
	publicIDs := fs.String("public-images", env("VWS_PUBLIC_IMAGES", ""), "comma-separated image ids readable by every project in the image's region, such as seed images; changing or deleting them stays with the owner (env VWS_PUBLIC_IMAGES)")
	fs.StringVar(&c.leaseName, "lease-name", env("VWS_LEASE_NAME", defaultLeaseName),
		"coordination.k8s.io Lease in -namespace that replicas elect the leader on (env VWS_LEASE_NAME)")
	fs.DurationVar(&c.leaseDuration, "lease-duration", defaultLeaseDuration,
		"how long a follower waits after the leader's last renew before it takes the Lease")
	fs.DurationVar(&c.renewDeadline, "lease-renew-deadline", defaultRenewDeadline,
		"how long the leader retries a renew before it stops the reconciler and repair")
	fs.DurationVar(&c.retryPeriod, "lease-retry-period", defaultRetryPeriod,
		"interval between Lease acquire and renew attempts")
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
