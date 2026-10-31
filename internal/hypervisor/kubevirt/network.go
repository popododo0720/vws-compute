package kubevirt

import (
	"fmt"
	"net/netip"
	"regexp"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

const (
	// annotationIPAddress is the Kube-OVN pod annotation suffix that fixes
	// an interface's address. A direct interface uses it under its
	// provider: <provider>.kubernetes.io/ip_address.
	annotationIPAddress = ".kubernetes.io/ip_address"
	// annotationLogicalSwitch is the provider-scoped form of
	// AnnotationLogicalSwitch.
	annotationLogicalSwitch = ".kubernetes.io/logical_switch"
	// directPrefix names the Kube-OVN Subnet and NetworkAttachmentDefinition
	// of a direct network (vws-deploy#125).
	directPrefix = "vws-direct-"
)

// directName is a direct network name as vws-deploy#125 allows it.
var directName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,14}$`)

// DirectNAD is the NetworkAttachmentDefinition that vws-network creates in
// the VM namespace for direct network name.
func DirectNAD(name string) string { return directPrefix + name }

// DirectProvider is the Kube-OVN provider of direct network name's
// attachment in namespace ns.
func DirectProvider(name, ns string) string { return DirectNAD(name) + "." + ns + ".ovn" }

// networks renders a machine's interfaces as KubeVirt
// spec.template.spec.domain.devices.interfaces and spec.template.spec.networks
// in device index order, with the pod annotations Kube-OVN reads. Every
// interface uses bridge binding. The one overlay interface is the pod
// network on its logical switch; a direct interface is the Multus network
// vws-direct-<name> in namespace ns. An empty list renders nothing, so
// KubeVirt keeps its default pod interface. Any invalid or unsupported
// interface is a permanent error.
func networks(machine, ns string, ifaces []hypervisor.Interface) (devs, nets []any, ann map[string]any, err error) {
	if len(ifaces) == 0 {
		return nil, nil, nil, nil
	}
	bad := func(format string, args ...any) error {
		return hypervisor.Permanent(fmt.Errorf("machine %s: "+format, append([]any{machine}, args...)...))
	}
	ann = map[string]any{}
	names := map[string]bool{}
	directs := map[string]bool{}
	overlay := false
	for _, n := range ifaces {
		if errs := validation.IsDNS1123Label(n.Name); len(errs) > 0 || names[n.Name] {
			return nil, nil, nil, bad("interface name %q is not a unique DNS-1123 label", n.Name)
		}
		names[n.Name] = true
		if (n.Subnet == "") == (n.Direct == "") {
			return nil, nil, nil, bad("interface %s needs exactly one of an overlay subnet or a direct network", n.Name)
		}
		devs = append(devs, map[string]any{"name": n.Name, "bridge": map[string]any{}})
		if n.Subnet != "" {
			if overlay {
				return nil, nil, nil, bad("interface %s: more than one overlay interface is not supported", n.Name)
			}
			if errs := validation.IsDNS1123Subdomain(n.Subnet); len(errs) > 0 {
				return nil, nil, nil, bad("subnet %q is not a valid Kube-OVN Subnet name", n.Subnet)
			}
			if n.Mode != "" || n.IPv4 != "" {
				return nil, nil, nil, bad("interface %s: an overlay interface takes no mode or fixed address", n.Name)
			}
			overlay = true
			ann[AnnotationLogicalSwitch] = n.Subnet
			nets = append(nets, map[string]any{"name": n.Name, "pod": map[string]any{}})
			continue
		}
		if !directName.MatchString(n.Direct) {
			return nil, nil, nil, bad("direct network %q is not a valid name", n.Direct)
		}
		if directs[n.Direct] {
			return nil, nil, nil, bad("direct network %s is attached twice", n.Direct)
		}
		directs[n.Direct] = true
		if n.Mode != "" && n.Mode != hypervisor.ModeBridge {
			// SR-IOV and macvtap are extension points (vws-deploy#125).
			return nil, nil, nil, bad("interface %s: direct mode %q is not supported", n.Name, n.Mode)
		}
		provider := DirectProvider(n.Direct, ns)
		ann[provider+annotationLogicalSwitch] = DirectNAD(n.Direct)
		if n.IPv4 != "" {
			ip, err := netip.ParseAddr(n.IPv4)
			if err != nil || !ip.Is4() {
				return nil, nil, nil, bad("interface %s: %q is not an IPv4 address", n.Name, n.IPv4)
			}
			ann[provider+annotationIPAddress] = ip.String()
		}
		nets = append(nets, map[string]any{"name": n.Name, "multus": map[string]any{"networkName": DirectNAD(n.Direct)}})
	}
	return devs, nets, ann, nil
}
