package kubevirt

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// TestHotplugRequiresBlockClaim: a volume is hot-plugged only when its claim
// is volumeMode Block. A Filesystem claim, and one made without a volumeMode
// (the apiserver stores Filesystem), are refused permanently before the
// VirtualMachine is patched, with the claim, its mode, and the remedy.
func TestHotplugRequiresBlockClaim(t *testing.T) {
	block, fs := corev1.PersistentVolumeBlock, corev1.PersistentVolumeFilesystem
	for _, tc := range []struct {
		name string
		mode *corev1.PersistentVolumeMode
		ok   bool
	}{
		{"block", &block, true},
		{"filesystem", &fs, false},
		{"unset", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, ns, w := newClient(t)
			standardImage(t, c, ns)
			a := New(c, Config{Namespace: ns})
			claimIn(t, c, ns, "pvc-data", corev1.ReadWriteOnce, tc.mode)
			s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
			if err := a.ConvergeMachine(ctx, s); err != nil {
				t.Fatal(err)
			}
			before := w.n.Load()
			s.Volumes = []hypervisor.Volume{{Name: "vdb", Claim: "pvc-data"}}
			err := a.ConvergeMachine(ctx, s)
			vols, _ := diskNames(t, c, ns, s.Name)
			attached := strings.Contains(strings.Join(vols, ","), "vdb")
			if tc.ok {
				if err != nil || !attached {
					t.Fatalf("Block claim: err %v, volumes %v; want attached", err, vols)
				}
				return
			}
			want := "claim pvc-data has volumeMode Filesystem, but hot-plugging needs volumeMode Block; recreate the volume as a Block volume and attach it again"
			if !hypervisor.IsPermanent(err) || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want permanent error containing %q", err, want)
			}
			if attached || w.n.Load() != before {
				t.Fatalf("refused attach still changed the VirtualMachine: volumes %v, %d writes", vols, w.n.Load()-before)
			}
		})
	}
}

// TestFilesystemVolumeAttachedBeforeStays: a Filesystem volume that is
// already in the VirtualMachine (attached before this check existed) is not
// checked again, and a Block volume can still be added next to it.
func TestFilesystemVolumeAttachedBeforeStays(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	standardImage(t, c, ns)
	a := New(c, Config{Namespace: ns})
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, c, ns, "pvc-old", corev1.ReadWriteOnce, &fs)
	claim(t, c, ns, "pvc-new", corev1.ReadWriteOnce)
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatal(err)
	}
	// Put the old volume in the template the way an earlier release did.
	vm, err := get(t, c, "VirtualMachine", ns, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	vols, _, _ := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "volumes")
	disks, _, _ := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "domain", "devices", "disks")
	vols = append(vols, map[string]any{"name": "vdb", "persistentVolumeClaim": map[string]any{"claimName": "pvc-old", "hotpluggable": true}})
	disks = append(disks, map[string]any{"name": "vdb", "serial": "vdb", "disk": map[string]any{"bus": "scsi"}})
	if err := unstructured.SetNestedSlice(vm.Object, vols, "spec", "template", "spec", "volumes"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedSlice(vm.Object, disks, "spec", "template", "spec", "domain", "devices", "disks"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, vm); err != nil {
		t.Fatal(err)
	}

	s.Volumes = []hypervisor.Volume{{Name: "vdb", Claim: "pvc-old"}}
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatalf("converge with the attached Filesystem volume: %v", err)
	}
	s.Volumes = append(s.Volumes, hypervisor.Volume{Name: "vdc", Claim: "pvc-new"})
	if err := a.ConvergeMachine(ctx, s); err != nil {
		t.Fatalf("add a Block volume next to it: %v", err)
	}
	if got, _ := diskNames(t, c, ns, s.Name); !strings.Contains(strings.Join(got, ","), "vdb") || !strings.Contains(strings.Join(got, ","), "vdc") {
		t.Fatalf("volumes = %v, want vdb and vdc", got)
	}
}

// TestFilesystemBootClaimBoots: the boot claim is not hot-plugged, so a
// Filesystem boot claim (an existing volume holding a disk.img) still boots.
func TestFilesystemBootClaimBoots(t *testing.T) {
	c, ns, _ := newClient(t)
	fs := corev1.PersistentVolumeFilesystem
	claimIn(t, c, ns, "pvc-root", corev1.ReadWriteOnce, &fs)
	s := spec("i-0123456789abcdef0", hypervisor.PowerOn)
	s.Image, s.BootClaim = "", "pvc-root"
	if err := New(c, Config{Namespace: ns}).ConvergeMachine(context.Background(), s); err != nil {
		t.Fatal(err)
	}
}

// mapStorage answers each volume with its own claim.
type mapStorage map[string]string

func (m mapStorage) Volume(_ context.Context, name string) (grpcapi.VolumeInfo, error) {
	claim, ok := m[name]
	if !ok {
		return grpcapi.VolumeInfo{}, grpcapi.ErrVolumeNotFound
	}
	return grpcapi.VolumeInfo{ClaimName: claim}, nil
}

// TestFilesystemAttachFailsOperationWithFailedPrecondition drives the attach
// through the gRPC API and the reconciler: the UpdateInstance operation fails
// with FAILED_PRECONDITION and the agent's message, and the VirtualMachine
// keeps only its boot disk.
func TestFilesystemAttachFailsOperationWithFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	fs := corev1.PersistentVolumeFilesystem
	claim(t, c, ns, "pvc-root", corev1.ReadWriteOnce)
	claimIn(t, c, ns, "pvc-data", corev1.ReadWriteOnce, &fs)

	repo := memstore.New()
	rec := reconciler.New(repo, New(c, Config{Namespace: ns, ImageNamespace: ns}), reconciler.WithReports(repo))
	srv := grpc.NewServer()
	const vols = "projects/demo/zones/z1/volumes/"
	grpcapi.Register(srv, controller.New(repo), repo, grpcapi.WithVolumes(mapStorage{vols + "root": "pvc-root", vols + "data": "pvc-data"}))
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
	ins, ops := computev1.NewInstanceServiceClient(cc), longrunningpb.NewOperationsClient(cc)

	boot := &computev1.VolumeAttachment{Volume: vols + "root", DeviceName: "vda", Boot: true}
	if _, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/z1", InstanceId: "web-1", RequestId: "61616161-6161-4161-8161-616161616161",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING, Volumes: []*computev1.VolumeAttachment{boot},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	op, err := ins.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId: "62626262-6262-4262-8262-626262626262",
		Instance: &computev1.Instance{Name: "projects/demo/zones/z1/instances/web-1", Spec: &computev1.InstanceSpec{
			Power:   computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes: []*computev1.VolumeAttachment{boot, {Volume: vols + "data", DeviceName: "vdb"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatalf("a permanent error is reported, not returned: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	got, err := ops.GetOperation(waitCtx, &longrunningpb.GetOperationRequest{Name: op.GetName()})
	if err != nil {
		t.Fatal(err)
	}
	want := "claim pvc-data has volumeMode Filesystem, but hot-plugging needs volumeMode Block; recreate the volume as a Block volume and attach it again"
	if !got.GetDone() || codes.Code(got.GetError().GetCode()) != codes.FailedPrecondition || !strings.Contains(got.GetError().GetMessage(), want) {
		t.Fatalf("operation = done %v error %v, want FAILED_PRECONDITION containing %q", got.GetDone(), got.GetError(), want)
	}
	if v, _ := diskNames(t, c, ns, "web-1"); strings.Contains(strings.Join(v, ","), "vdb") {
		t.Fatalf("volumes = %v, want no vdb", v)
	}
}

// TestRequireBlockNamesUnsetMode: a claim object with no volumeMode (the
// apiserver normally stores Filesystem) is Filesystem, and the message says so.
func TestRequireBlockNamesUnsetMode(t *testing.T) {
	err := requireBlock("m", hypervisor.Volume{Name: "vdb", Claim: "pvc-x"}, &corev1.PersistentVolumeClaim{})
	want := "machine m: volume vdb: claim pvc-x has volumeMode Filesystem (unset, the default), but hot-plugging needs volumeMode Block; recreate the volume as a Block volume and attach it again"
	if !hypervisor.IsPermanent(err) || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
}
