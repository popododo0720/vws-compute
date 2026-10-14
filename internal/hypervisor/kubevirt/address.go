package kubevirt

import (
	"context"
	"fmt"
	"net/netip"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MachineIPv4 implements hypervisor.AddressObserver from the
// VirtualMachineInstance's status.interfaces. The machine has one
// interface, the pod network one KubeVirt names "default" (the template
// lists none, so KubeVirt adds it), which Kube-OVN puts on the subnet. No
// instance yet, or no IPv4 on it yet, is "".
func (a *Agent) MachineIPv4(ctx context.Context, name string) (string, error) {
	vmi := new(unstructured.Unstructured)
	vmi.SetGroupVersionKind(vmiGVK)
	err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: name}, vmi)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("machine %s: get virtual machine instance: %w", name, err)
	}
	return podIPv4(vmi), nil
}

// podIPv4 is the first IPv4 of the interface named "default", or of the
// only interface when none has that name.
func podIPv4(vmi *unstructured.Unstructured) string {
	ifaces, _, _ := unstructured.NestedSlice(vmi.Object, "status", "interfaces") //nolint:errcheck // another shape has no address we can use
	var pick map[string]any
	for _, i := range ifaces {
		m, ok := i.(map[string]any)
		if !ok {
			continue
		}
		if m["name"] == podInterface {
			pick = m
			break
		}
		if len(ifaces) == 1 {
			pick = m
		}
	}
	if pick == nil {
		return ""
	}
	cands := []any{pick["ipAddress"]}
	if list, ok := pick["ipAddresses"].([]any); ok {
		cands = append(cands, list...)
	}
	for _, c := range cands {
		s, _ := c.(string) //nolint:errcheck // a non-string is skipped
		if ip, err := netip.ParseAddr(s); err == nil && ip.Is4() {
			return ip.String()
		}
	}
	return ""
}

// podInterface is the name KubeVirt gives the pod network interface it adds
// to a VirtualMachine whose template lists no interfaces.
const podInterface = "default"
