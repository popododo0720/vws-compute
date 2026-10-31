package kubevirt

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// vmiInterfaces77h is status.interfaces of i-7197a391759a5ae33 on vws-test
// (evidence 77h, 2026-10-14 06:30 UTC).
const vmiInterfaces77h = `[{"infoSource":"domain","ipAddress":"172.31.76.3","ipAddresses":["172.31.76.3"],"linkState":"up","mac":"0a:4d:fc:dd:8a:98","name":"default","podInterfaceName":"eth0","queueCount":1}]`

// vmiInterfacesMultiNIC is status.interfaces of a VMI whose template names
// a direct nic0 and an overlay nic1 (vws-compute#151 review probe).
const vmiInterfacesMultiNIC = `[{"name":"nic0","ipAddress":"192.168.200.30","ipAddresses":["192.168.200.30"]},` +
	`{"name":"nic1","ipAddress":"10.0.1.5","ipAddresses":["10.0.1.5"]}]`

func setInterfaces(ctx context.Context, t *testing.T, c client.Client, ns, name, raw string) {
	t.Helper()
	var ifaces []any
	if err := json.Unmarshal([]byte(raw), &ifaces); err != nil {
		t.Fatal(err)
	}
	vmi := new(unstructured.Unstructured)
	vmi.SetGroupVersionKind(vmiGVK)
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, vmi); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedSlice(vmi.Object, ifaces, "status", "interfaces"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, vmi); err != nil {
		t.Fatal(err)
	}
}

// TestInstanceStatusCarriesVMIAddress: DescribeInstances showed
// PrivateIpAddress None (vws-test 76l/77h) because compute never reported
// the VMI's address. The guest is Ready before KubeVirt shows the address,
// so the instance goes READY without it; once status.interfaces has it, the
// next sweep reports it on the instance's first NIC, under the EC2 device
// name nic0 rather than KubeVirt's "default".
func TestInstanceStatusCarriesVMIAddress(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)
	subnet := "projects/demo/regions/vws-1/vpcs/vpc-1/subnets/subnet-76"
	repo, rec := createUnreconciled(t, New(c, Config{Namespace: ns, ImageNamespace: ns}), "vws.small", "pvc-root-1",
		&computev1.NetworkInterface{DeviceName: "nic0", Subnet: subnet})
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	id := ok(compute.ParseInstanceID("web-1"))
	inst, err := repo.Instance(ctx, id)
	if err != nil || inst.Status.Report.State != compute.ReportReady {
		t.Fatalf("after the first sweep = %+v, %v; want READY", inst.Status.Report, err)
	}

	setInterfaces(ctx, t, c, ns, "web-1", vmiInterfaces77h)
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	inst, err = repo.Instance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	want := compute.ObservedInterface{DeviceName: "nic0", Subnet: subnet, PrivateIPv4: "172.31.76.3"}
	if got := inst.Status.Report.Interfaces; len(got) != 1 || got[0] != want {
		t.Fatalf("observed interfaces = %+v, want [%+v]", got, want)
	}
	if inst.Status.Report.State != compute.ReportReady || inst.Status.Observed.Generation != inst.Spec.Generation {
		t.Fatalf("address report changed the verdict: %+v", inst.Status)
	}
}

// TestMultiNICInstanceReportsDeviceIndexZero: a multi-NIC VM has no
// "default" interface, so the address of nic0 (device index 0) is reported
// under the device index 0 NIC, even when the request lists nic1 first.
func TestMultiNICInstanceReportsDeviceIndexZero(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)
	const region = "projects/demo/regions/vws-1/vpcs/"
	direct := region + "vpc-direct/subnets/subnet-direct-prod"
	repo, rec := createUnreconciled(t, New(c, Config{Namespace: ns, ImageNamespace: ns}), "vws.small", "pvc-root-1",
		&computev1.NetworkInterface{DeviceName: "nic1", Subnet: region + "vpc-1/subnets/subnet-r40"},
		&computev1.NetworkInterface{DeviceName: "nic0", Subnet: direct})
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	setInterfaces(ctx, t, c, ns, "web-1", vmiInterfacesMultiNIC)
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	inst, err := repo.Instance(ctx, ok(compute.ParseInstanceID("web-1")))
	if err != nil {
		t.Fatal(err)
	}
	want := compute.ObservedInterface{DeviceName: "nic0", Subnet: direct, PrivateIPv4: "192.168.200.30"}
	if got := inst.Status.Report.Interfaces; len(got) != 1 || got[0] != want {
		t.Fatalf("observed interfaces = %+v, want [%+v]", got, want)
	}
}

func TestPodIPv4(t *testing.T) {
	for name, tc := range map[string]struct{ raw, want string }{
		"77h":          {vmiInterfaces77h, "172.31.76.3"},
		"none":         {`[]`, ""},
		"no address":   {`[{"name":"default"}]`, ""},
		"ipv6 first":   {`[{"name":"default","ipAddress":"fd00::5","ipAddresses":["fd00::5","10.0.0.7"]}]`, "10.0.0.7"},
		"only one":     {`[{"name":"eth0","ipAddress":"10.0.0.8"}]`, "10.0.0.8"},
		"default wins": {`[{"name":"net1","ipAddress":"10.9.9.9"},{"name":"default","ipAddress":"10.0.0.9"}]`, "10.0.0.9"},
		"not ours":     {`[{"name":"net1","ipAddress":"10.9.9.9"},{"name":"net2","ipAddress":"10.9.9.8"}]`, ""},
		"bad shape":    {`["default"]`, ""},
		"multi-NIC":    {vmiInterfacesMultiNIC, "192.168.200.30"},
		"nic0 later":   {`[{"name":"nic1","ipAddress":"10.0.1.5"},{"name":"nic0","ipAddress":"10.0.0.5"}]`, "10.0.0.5"},
		"no nic0":      {`[{"name":"nic1","ipAddress":"10.0.1.5"},{"name":"nic2","ipAddress":"10.0.2.5"}]`, ""},
	} {
		t.Run(name, func(t *testing.T) {
			var ifaces []any
			if err := json.Unmarshal([]byte(tc.raw), &ifaces); err != nil {
				t.Fatal(err)
			}
			vmi := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"interfaces": ifaces}}}
			if got := podIPv4(vmi); got != tc.want {
				t.Fatalf("podIPv4 = %q, want %q", got, tc.want)
			}
		})
	}
}
