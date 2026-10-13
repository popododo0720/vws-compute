package kubevirt

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// claimStorage answers every volume with one claim, as vws-storage reports
// VolumeStatus.claim_name.
type claimStorage struct{ claim string }

func (s claimStorage) Volume(context.Context, string) (grpcapi.VolumeInfo, error) {
	return grpcapi.VolumeInfo{ClaimName: s.claim}, nil
}

// TestCreateInstanceBootsFromVolumeOnKubeVirt drives CreateInstance through
// the gRPC API, the controller, and the reconciler with this agent against
// envtest: the instance reaches READY and its VirtualMachine boots from the
// storage claim. envtest runs no KubeVirt controller; newClient's stand-in
// marks the VirtualMachineInstance Running and Ready, so READY here means the
// readiness check passed against that stand-in, not that a guest booted.
func TestCreateInstanceBootsFromVolumeOnKubeVirt(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)

	repo := createThroughAPI(t, New(c, Config{Namespace: ns, ImageNamespace: ns}), "vws.small", "pvc-root-1")

	got, err := repo.Instance(ctx, ok(compute.ParseInstanceID("web-1")))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Report.State != compute.ReportReady || got.Status.Observed.Generation != 1 {
		t.Fatalf("status = %+v, want READY at generation 1", got.Status)
	}
	vm, err := get(t, c, "VirtualMachine", ns, "web-1")
	if err != nil {
		t.Fatal(err)
	}
	if claim := rootClaim(vm); claim != "pvc-root-1" {
		t.Fatalf("root disk claim = %q, want pvc-root-1", claim)
	}
}

func ok[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// createThroughAPI serves the gRPC API over a fresh memstore, creates
// instance web-1 of machineType booting from claim, and runs one reconcile
// sweep with agent.
func createThroughAPI(t *testing.T, agent hypervisor.Agent, machineType, claim string) *memstore.Store {
	t.Helper()
	repo, rec := createUnreconciled(t, agent, machineType, claim)
	if err := rec.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	return repo
}

// createUnreconciled is createThroughAPI without the sweep. It returns the
// reconciler so the test can sweep when it chooses. nics go into the request
// as given.
func createUnreconciled(t *testing.T, agent hypervisor.Agent, machineType, claim string, nics ...*computev1.NetworkInterface) (*memstore.Store, *reconciler.Reconciler) {
	t.Helper()
	ctx := context.Background()
	repo := memstore.New()
	rec := reconciler.New(repo, agent, reconciler.WithReports(repo))
	srv := grpc.NewServer()
	grpcapi.Register(srv, controller.New(repo), repo, grpcapi.WithVolumes(claimStorage{claim: claim}))
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	_, err = computev1.NewInstanceServiceClient(cc).CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/z1", InstanceId: "web-1", RequestId: "71717171-7171-4171-8171-717171717171",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: machineType, Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes:           []*computev1.VolumeAttachment{{Volume: "projects/demo/zones/z1/volumes/root-1", DeviceName: "vda", Boot: true}},
			NetworkInterfaces: nics,
		}},
	})
	if err != nil {
		t.Fatalf("CreateInstance %s: %v", machineType, err)
	}
	return repo, rec
}

var largeTypes = []struct {
	name      string
	vcpus     int
	memoryMiB int
}{{"vws.large", 4, 8192}, {"vws.xlarge", 8, 16384}}

// TestLargeMachineTypesReachMachineSpec: CreateInstance accepts vws.large and
// vws.xlarge, and the reconciler hands the agent their vCPU and memory.
func TestLargeMachineTypesReachMachineSpec(t *testing.T) {
	for _, tc := range largeTypes {
		t.Run(tc.name, func(t *testing.T) {
			agent := fakeagent.New()
			createThroughAPI(t, agent, tc.name, "pvc-root-1")
			m, found := agent.Machine("web-1")
			if !found || m.VCPUs != tc.vcpus || m.MemoryMiB != tc.memoryMiB {
				t.Fatalf("MachineSpec = %+v, %v; want %d vCPU, %d MiB", m, found, tc.vcpus, tc.memoryMiB)
			}
		})
	}
}

// TestLargeMachineTypesSizeTheVirtualMachine: the same requests against
// envtest produce VirtualMachines with those cores and guest memory.
func TestLargeMachineTypesSizeTheVirtualMachine(t *testing.T) {
	for _, tc := range largeTypes {
		t.Run(strings.ReplaceAll(tc.name, ".", "-"), func(t *testing.T) { // the subtest name becomes a namespace
			c, ns, _ := newClient(t)
			claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)
			createThroughAPI(t, New(c, Config{Namespace: ns, ImageNamespace: ns}), tc.name, "pvc-root-1")
			vm, err := get(t, c, "VirtualMachine", ns, "web-1")
			if err != nil {
				t.Fatal(err)
			}
			cores, _, _ := unstructured.NestedInt64(vm.Object, "spec", "template", "spec", "domain", "cpu", "cores")
			mem, _, _ := unstructured.NestedString(vm.Object, "spec", "template", "spec", "domain", "memory", "guest")
			if cores != int64(tc.vcpus) || mem != memory(tc.memoryMiB) {
				t.Fatalf("VM cores %d memory %q; want %d, %s", cores, mem, tc.vcpus, memory(tc.memoryMiB))
			}
		})
	}
}
