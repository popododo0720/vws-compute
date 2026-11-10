package grpcapi_test

import (
	"context"
	"strings"
	"testing"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRebootInstance(t *testing.T) {
	ctx := context.Background()
	ins, rec, agent := consoleServer(t)
	_, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: zoneParent, InstanceId: "i-0123456789abcdef0",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes: []*computev1.VolumeAttachment{{Volume: volBase + "vol-boot", DeviceName: "boot", Boot: true}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_RUNNING)
	req := &computev1.RebootInstanceRequest{
		Name: consoleInstance, RequestId: "61616161-6161-4161-8161-616161616161", Etag: before.GetEtag(),
	}
	op, err := ins.RebootInstance(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if op.GetDone() {
		t.Fatal("reboot completed before node report")
	}
	md := &commonv1.OperationMetadata{}
	if err := op.GetMetadata().UnmarshalTo(md); err != nil {
		t.Fatal(err)
	}
	if md.GetAction() != "RebootInstances" || md.GetVerb() != commonv1.OperationVerb_OPERATION_VERB_UPDATE {
		t.Fatalf("metadata = %v", md)
	}
	coalesced := proto.Clone(req).(*computev1.RebootInstanceRequest)
	coalesced.RequestId = "67676767-6767-4767-8767-676767676767"
	coalesced.Etag = ""
	if _, err := ins.RebootInstance(ctx, coalesced); err != nil {
		t.Fatal(err)
	}
	pending, err := ins.RebootInstance(ctx, req)
	if err != nil || pending.GetDone() {
		t.Fatalf("coalesced reboot completed early: %v, %v", pending, err)
	}
	after := sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_RUNNING)
	if after.GetSpec().GetRebootCount() != 2 || after.GetStatus().GetRebootCount() != 2 ||
		agent.Reboots("i-0123456789abcdef0") != 1 {
		t.Fatalf("reboot result = %v", after)
	}
	replay, err := ins.RebootInstance(ctx, req)
	if err != nil || replay.GetName() != op.GetName() || !replay.GetDone() {
		t.Fatalf("replay = %v, %v", replay, err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.Reboots("i-0123456789abcdef0") != 1 {
		t.Fatal("replay rebooted again")
	}
	foreign := proto.Clone(req).(*computev1.RebootInstanceRequest)
	foreign.Name = strings.Replace(consoleInstance, "projects/demo/", "projects/foreign/", 1)
	if _, err := ins.RebootInstance(ctx, foreign); status.Code(err) != codes.Aborted {
		t.Fatalf("foreign replay = %v", err)
	}
	stale := proto.Clone(req).(*computev1.RebootInstanceRequest)
	stale.RequestId = "62626262-6262-4262-8262-626262626262"
	if _, err := ins.RebootInstance(ctx, stale); status.Code(err) != codes.Aborted {
		t.Fatalf("stale etag = %v", err)
	}
	stop := consolePower("63636363-6363-4363-8363-636363636363", computev1.InstancePower_INSTANCE_POWER_STOPPED)
	stop.Instance.Spec.RebootCount = 99
	if _, err := ins.UpdateInstance(ctx, stop); err != nil {
		t.Fatal(err)
	}
	sweepTo(t, ins, rec, computev1.InstancePower_INSTANCE_POWER_STOPPED)
	if _, err := ins.RebootInstance(ctx, req); err != nil {
		t.Fatalf("replay after stop = %v", err)
	}
	stopped := proto.Clone(req).(*computev1.RebootInstanceRequest)
	stopped.RequestId = "64646464-6464-4464-8464-646464646464"
	assertIncorrectState(t, func() error { _, err := ins.RebootInstance(ctx, stopped); return err }())
	if _, err := ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{Name: consoleInstance}); err != nil {
		t.Fatal(err)
	}
	stopped.RequestId = "65656565-6565-4565-8565-656565656565"
	assertIncorrectState(t, func() error { _, err := ins.RebootInstance(ctx, stopped); return err }())
	unknown := proto.Clone(req).(*computev1.RebootInstanceRequest)
	unknown.Name = zoneParent + "/instances/i-fffffffffffffffff"
	unknown.RequestId = "66666666-6666-4666-8666-666666666666"
	if _, err := ins.RebootInstance(ctx, unknown); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown instance = %v", err)
	}
}

func assertIncorrectState(t *testing.T, err error) {
	t.Helper()
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("state error = %v", err)
	}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetReason() == "INCORRECT_STATE" &&
			info.GetMetadata()["aws_code"] == "IncorrectInstanceState" {
			return
		}
	}
	t.Fatalf("missing incorrect state details: %v", err)
}
