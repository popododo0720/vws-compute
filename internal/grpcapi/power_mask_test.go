package grpcapi_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const (
	consoleSubnet   = "projects/demo/regions/r/vpcs/v/subnets/subnet-1"
	consoleInstance = zoneParent + "/instances/i-0123456789abcdef0"
)

// consoleServer serves the API with a reconciler that converges
// controller-driven instances through reports, as vws-compute does with its
// built-in agent. The test sweeps by hand.
func consoleServer(t *testing.T) (computev1.InstanceServiceClient, *reconciler.Reconciler, *fakeagent.Agent) {
	t.Helper()
	repo := memstore.New()
	agent := fakeagent.New()
	rec := reconciler.New(repo, agent, reconciler.WithReports(repo))
	st := &fakeStorage{vols: map[string]grpcapi.VolumeInfo{
		volBase + "vol-boot": {ClaimName: "pvc-boot"}, volBase + "vol-data": {ClaimName: "pvc-data"},
	}}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	grpcapi.Register(srv, controller.New(repo), repo, grpcapi.WithVolumes(st))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return computev1.NewInstanceServiceClient(conn), rec, agent
}

// consolePower is the UpdateInstance vws-console-api sends for StopInstances
// and StartInstances (internal/backend/grpcclient services.go and ec2.go):
// mask spec.power and labels, with the instance's machine type and its NIC
// still in the body.
func consolePower(req string, power computev1.InstancePower) *computev1.UpdateInstanceRequest {
	return &computev1.UpdateInstanceRequest{
		RequestId:  req,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.power", "labels"}},
		Instance: &computev1.Instance{
			Name:   consoleInstance,
			Labels: map[string]string{"vws-action": "power"},
			Spec: &computev1.InstanceSpec{
				MachineType:       "vws.small",
				Power:             power,
				NetworkInterfaces: []*computev1.NetworkInterface{{DeviceName: "nic0", Subnet: consoleSubnet}},
			},
		},
	}
}

func sweepTo(t *testing.T, ins computev1.InstanceServiceClient, rec *reconciler.Reconciler, power computev1.InstancePower) *computev1.Instance {
	t.Helper()
	ctx := context.Background()
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := ins.GetInstance(ctx, &computev1.GetInstanceRequest{Name: consoleInstance})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY ||
		got.GetStatus().GetPower() != power || got.GetSpec().GetPower() != power {
		t.Fatalf("instance status = %v spec power %s, want READY %s", got.GetStatus(), got.GetSpec().GetPower(), power)
	}
	return got
}

// TestConsoleStopStartAttach replays vws-console-api's requests against an
// instance it created: StopInstances, StartInstances, then AttachVolume. Each
// sends its whole known spec with a mask naming one field. On 62d90e7 the stop
// failed with "network interfaces and user_data are not mutable" (#52).
func TestConsoleStopStartAttach(t *testing.T) {
	ctx := context.Background()
	ins, rec, agent := consoleServer(t)
	_, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: zoneParent, InstanceId: "i-0123456789abcdef0", RequestId: "51515151-5151-4151-8151-515151515151",
		Instance: &computev1.Instance{
			Labels: map[string]string{"vws-action": "run"},
			Spec: &computev1.InstanceSpec{
				MachineType:       "vws.small",
				Power:             computev1.InstancePower_INSTANCE_POWER_RUNNING,
				NetworkInterfaces: []*computev1.NetworkInterface{{DeviceName: "nic0", Subnet: consoleSubnet}},
				Volumes:           []*computev1.VolumeAttachment{{Volume: volBase + "vol-boot", DeviceName: "boot", Boot: true}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_RUNNING)

	if _, err := ins.UpdateInstance(ctx, consolePower("52525252-5252-4252-8252-525252525252", computev1.InstancePower_INSTANCE_POWER_STOPPED)); err != nil {
		t.Fatalf("StopInstances: %v", err)
	}
	got := sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_STOPPED)
	if m, _ := agent.Machine("i-0123456789abcdef0"); m.Power != hypervisor.PowerOff {
		t.Fatalf("agent power after stop = %d, want off", m.Power)
	}
	// The mask left the NIC and volumes alone.
	if nics := got.GetSpec().GetNetworkInterfaces(); len(nics) != 1 || nics[0].GetSubnet() != consoleSubnet || len(got.GetSpec().GetVolumes()) != 1 {
		t.Fatalf("spec after stop = %v", got.GetSpec())
	}

	if _, err := ins.UpdateInstance(ctx, consolePower("53535353-5353-4353-8353-535353535353", computev1.InstancePower_INSTANCE_POWER_RUNNING)); err != nil {
		t.Fatalf("StartInstances: %v", err)
	}
	sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_RUNNING)

	// AttachVolume: mask spec.volumes and labels, no power in the body.
	_, err = ins.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId:  "54545454-5454-4454-8454-545454545454",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.volumes", "labels"}},
		Instance: &computev1.Instance{
			Name:   consoleInstance,
			Labels: map[string]string{"vws-action": "attach"},
			Spec: &computev1.InstanceSpec{Volumes: []*computev1.VolumeAttachment{
				{Volume: volBase + "vol-boot", DeviceName: "boot", Boot: true},
				{Volume: volBase + "vol-data", DeviceName: "sdf"},
			}},
		},
	})
	if err != nil {
		t.Fatalf("AttachVolume: %v", err)
	}
	got = sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_RUNNING)
	if len(got.GetSpec().GetVolumes()) != 2 {
		t.Fatalf("volumes after attach = %v", got.GetSpec().GetVolumes())
	}
}

// TestUpdateInstanceRejectsOtherSpecPaths: a mask may name spec, spec.power,
// or spec.volumes. Any other spec field is not updatable.
func TestUpdateInstanceRejectsOtherSpecPaths(t *testing.T) {
	ctx := context.Background()
	ins, rec, _ := consoleServer(t)
	if _, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: zoneParent, InstanceId: "i-0123456789abcdef0", RequestId: "55555555-5555-4555-8555-555555555551",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes: []*computev1.VolumeAttachment{{Volume: volBase + "vol-boot", DeviceName: "boot", Boot: true}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_RUNNING)
	for i, p := range []string{"spec.machine_type", "spec.network_interfaces", "spec.user_data", "spec.key_pair"} {
		_, err := ins.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId:  "56565656-5656-4656-8656-56565656565" + string(rune('0'+i)),
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{p}},
			Instance:   &computev1.Instance{Name: consoleInstance, Spec: &computev1.InstanceSpec{MachineType: "vws.large"}},
		})
		if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != "field "+p+" cannot be updated" {
			t.Fatalf("mask %s: %v, want InvalidArgument field %s cannot be updated", p, err, p)
		}
	}
}
