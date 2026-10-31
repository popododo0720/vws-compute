package reconciler

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// directSubnetPrefix starts the ID of the EC2 subnet that stands for a direct
// network: subnet-direct-<name> (vws-deploy#125).
const directSubnetPrefix = "subnet-direct-"

// deviceIndex is the EC2 device index of a network interface. vws-console-api
// names the interface at device index N "nic<N>". Any other name keeps its
// position in the request.
func deviceIndex(device string, pos int) int {
	if rest, ok := strings.CutPrefix(device, "nic"); ok && rest != "" && strings.Trim(rest, "0123456789") == "" {
		if n, err := strconv.Atoi(rest); err == nil {
			return n
		}
	}
	return pos
}

// machineNetwork is the agent's view of an instance's interfaces. An
// instance without a direct interface keeps the single-overlay form: subnet
// is its first interface's subnet ID and ifaces is nil. An instance with a
// direct interface gets every interface in device index order and no subnet.
func machineNetwork(nics []compute.NetworkInterface) (subnet string, ifaces []hypervisor.Interface) {
	type indexed struct {
		index int
		nic   compute.NetworkInterface
	}
	var all []indexed
	direct := false
	for i, n := range nics {
		all = append(all, indexed{deviceIndex(n.DeviceName, i), n})
		if strings.HasPrefix(subnetID(n.Subnet), directSubnetPrefix) {
			direct = true
		}
	}
	if !direct {
		if len(nics) > 0 {
			subnet = subnetID(nics[0].Subnet)
		}
		return subnet, nil
	}
	slices.SortStableFunc(all, func(a, b indexed) int { return cmp.Compare(a.index, b.index) })
	for _, x := range all {
		iface := hypervisor.Interface{Name: "nic" + strconv.Itoa(x.index)}
		id := subnetID(x.nic.Subnet)
		if name, ok := strings.CutPrefix(id, directSubnetPrefix); ok {
			iface.Direct, iface.IPv4 = name, x.nic.PrivateIPv4
		} else {
			iface.Subnet = id
		}
		ifaces = append(ifaces, iface)
	}
	return "", ifaces
}
