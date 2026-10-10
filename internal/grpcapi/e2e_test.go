package grpcapi_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"golang.org/x/crypto/ssh"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

type harness struct {
	inst  computev1.InstanceServiceClient
	img   computev1.ImageServiceClient
	keys  computev1.KeyPairServiceClient
	ops   longrunningpb.OperationsClient
	agent *fakeagent.Agent
	ctl   *controller.Controller
}

func serve(t *testing.T) *harness {
	t.Helper()
	repo := memstore.New()
	agent := fakeagent.New()
	rec := reconciler.New(repo, agent, reconciler.WithInterval(10*time.Millisecond))
	ctl := controller.New(repo, controller.WithKick(rec.Kick))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	grpcapi.Register(srv, ctl, repo)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		_ = rec.Run(ctx)
		close(stopped)
	}()
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		cancel()
		<-stopped
	})
	return &harness{
		inst:  computev1.NewInstanceServiceClient(conn),
		img:   computev1.NewImageServiceClient(conn),
		keys:  computev1.NewKeyPairServiceClient(conn),
		ops:   longrunningpb.NewOperationsClient(conn),
		agent: agent,
		ctl:   ctl,
	}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("err = %v, want %s", err, want)
	}
}

func wantReason(t *testing.T, err error, reason string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("err = %v, want status %s", err, reason)
	}
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if ok && info.GetReason() == reason && info.GetDomain() == "vws.dev" {
			return
		}
	}
	t.Fatalf("err = %v, want reason %s", err, reason)
}

func ed25519Line(t *testing.T) (string, string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(sshPub.Marshal())
	line := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub)))
	return line, hex.EncodeToString(sum[:])
}

func (h *harness) submit(t *testing.T, requestID string, in compute.Intent) compute.Operation {
	t.Helper()
	req, err := compute.ParseRequestID(requestID)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := h.ctl.Submit(context.Background(), req, in)
	if err != nil {
		t.Fatal(err)
	}
	return rc.Operation
}

func (h *harness) wait(t *testing.T, op compute.Operation) *longrunningpb.Operation {
	t.Helper()
	got, err := h.ops.WaitOperation(context.Background(), &longrunningpb.WaitOperationRequest{
		Name:    "projects/demo/operations/" + op.ID.String(),
		Timeout: durationpb.New(10 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.GetDone() || got.GetError() != nil {
		t.Fatalf("operation %s done=%v err=%v", got.GetName(), got.GetDone(), got.GetError())
	}
	return got
}

func TestKeyPairService(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	line, fp := ed25519Line(t)
	parent := "projects/demo/regions/demo"
	name := parent + "/keyPairs/deploy"
	req := "11111111-1111-4111-8111-111111111111"

	created, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "deploy", RequestId: req,
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created.GetDone() {
		t.Fatal("CreateKeyPair operation is not done")
	}
	var got computev1.KeyPair
	if err := anypb.UnmarshalTo(created.GetResponse(), &got, proto.UnmarshalOptions{}); err != nil {
		t.Fatal(err)
	}
	if got.GetName() != name || got.GetStatus().GetFingerprintSha256() != fp || got.GetSpec().GetPublicKey() == "" {
		t.Fatalf("key pair = %v, want %s fingerprint %s", &got, name, fp)
	}
	if raw, _ := proto.Marshal(created); bytes.Contains(raw, []byte("PRIVATE KEY")) {
		t.Fatal("create response contains private key material")
	}

	replay, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "deploy", RequestId: req,
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.GetName() != created.GetName() {
		t.Fatalf("replay name = %s, want %s", replay.GetName(), created.GetName())
	}

	other, _ := ed25519Line(t)
	_, err = h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "deploy", RequestId: req,
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: other}},
	})
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")

	_, err = h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "deploy", RequestId: "22222222-2222-4222-8222-222222222222",
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
	})
	wantCode(t, err, codes.AlreadyExists)

	_, err = h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "broken", RequestId: "33333333-3333-4333-8333-333333333333",
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: "not a key"}},
	})
	wantCode(t, err, codes.InvalidArgument)

	fetched, err := h.keys.GetKeyPair(ctx, &computev1.GetKeyPairRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if fetched.GetStatus().GetFingerprintSha256() != fp {
		t.Fatalf("get fingerprint = %s", fetched.GetStatus().GetFingerprintSha256())
	}

	_, err = h.keys.ReportKeyPairStatus(ctx, &computev1.ReportKeyPairStatusRequest{
		Name: name,
		Status: &computev1.KeyPairStatus{Observed: &commonv1.ObservedState{
			Generation: fetched.GetGeneration() + 1,
		}},
	})
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "GENERATION_CONFLICT")

	second, _ := ed25519Line(t)
	if _, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "other", RequestId: "44444444-4444-4444-8444-444444444444",
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: second}},
	}); err != nil {
		t.Fatal(err)
	}
	page, err := h.keys.ListKeyPairs(ctx, &computev1.ListKeyPairsRequest{Parent: parent, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetKeyPairs()) != 1 || page.GetNextPageToken() == "" {
		t.Fatalf("first page = %d token %q", len(page.GetKeyPairs()), page.GetNextPageToken())
	}
	rest, err := h.keys.ListKeyPairs(ctx, &computev1.ListKeyPairsRequest{Parent: parent, PageSize: 1, PageToken: page.GetNextPageToken()})
	if err != nil {
		t.Fatal(err)
	}
	if len(rest.GetKeyPairs()) != 1 || rest.GetKeyPairs()[0].GetName() == page.GetKeyPairs()[0].GetName() {
		t.Fatalf("second page = %v", rest.GetKeyPairs())
	}

	_, err = h.keys.ListKeyPairs(ctx, &computev1.ListKeyPairsRequest{Parent: parent, PageSize: -1})
	wantCode(t, err, codes.InvalidArgument)

	del, err := h.keys.DeleteKeyPair(ctx, &computev1.DeleteKeyPairRequest{
		Name: name, RequestId: "55555555-5555-4555-8555-555555555555",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !del.GetDone() || del.GetError() != nil {
		t.Fatalf("delete = done %v err %v", del.GetDone(), del.GetError())
	}
	_, err = h.keys.GetKeyPair(ctx, &computev1.GetKeyPairRequest{Name: name})
	wantCode(t, err, codes.NotFound)

	only, err := h.keys.ListKeyPairs(ctx, &computev1.ListKeyPairsRequest{Parent: parent})
	if err != nil {
		t.Fatal(err)
	}
	if len(only.GetKeyPairs()) != 1 || !strings.HasSuffix(only.GetKeyPairs()[0].GetName(), "/other") {
		t.Fatalf("after delete = %v", only.GetKeyPairs())
	}
}

func TestInstanceAndImageProjection(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	line, _ := ed25519Line(t)
	parent := "projects/demo/regions/demo"
	if _, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "deploy", RequestId: "11111111-1111-4111-8111-111111111111",
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
	}); err != nil {
		t.Fatal(err)
	}
	keyName := parent + "/keyPairs/deploy"

	_, err := h.inst.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/demo", InstanceId: "web", RequestId: "22222222-2222-4222-8222-222222222222",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small",
			Power:       computev1.InstancePower_INSTANCE_POWER_RUNNING,
			Volumes: []*computev1.VolumeAttachment{{
				Volume: "projects/demo/zones/demo/volumes/root", DeviceName: "boot", Boot: true,
			}},
		}},
	})
	wantCode(t, err, codes.FailedPrecondition)
	wantReason(t, err, "DEPENDENCY_MISSING")

	_, err = h.inst.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/demo", InstanceId: "web", RequestId: "33333333-3333-4333-8333-333333333333",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
		}},
	})
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")

	_, err = h.img.CreateImage(ctx, &computev1.CreateImageRequest{
		Parent: parent, ImageId: "debian", RequestId: "44444444-4444-4444-8444-444444444444",
		Image: &computev1.Image{Spec: &computev1.ImageSpec{
			Source: &computev1.ImageSpec_Snapshot{Snapshot: parent + "/snapshots/snap"},
		}},
	})
	wantCode(t, err, codes.FailedPrecondition)
	wantReason(t, err, "DEPENDENCY_MISSING")

	reg, err := compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	imageOp := h.submit(t, "img-seed", reg)
	h.wait(t, imageOp)
	imageID := imageOp.Target.ID

	gotImg, err := h.img.GetImage(ctx, &computev1.GetImageRequest{
		Name: "projects/demo/regions/demo/images/" + imageID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotImg.GetSpec().GetFamily() != "debian-13" || gotImg.GetStatus().GetSizeGib() != 2 ||
		gotImg.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY {
		t.Fatalf("image = %v", gotImg)
	}

	run, err := compute.NewRunInstance(imageID, "vws.small", keyName)
	if err != nil {
		t.Fatal(err)
	}
	runOp := h.submit(t, "run-seed", run)
	h.wait(t, runOp)
	instanceID := runOp.Target.ID
	instanceName := "projects/demo/zones/demo/instances/" + instanceID

	gotInst, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
	if err != nil {
		t.Fatal(err)
	}
	if gotInst.GetSpec().GetMachineType() != "vws.small" || gotInst.GetSpec().GetPower() != computev1.InstancePower_INSTANCE_POWER_RUNNING ||
		gotInst.GetSpec().GetKeyPair() != keyName {
		t.Fatalf("instance = %v", gotInst)
	}
	m, _ := h.agent.Machine(instanceID)
	if len(m.AuthorizedKeys) != 1 || !strings.Contains(m.AuthorizedKeys[0], "ssh-ed25519") {
		t.Fatalf("authorized keys = %q", m.AuthorizedKeys)
	}

	stopped, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId: "66666666-6666-4666-8666-666666666666",
		Instance: &computev1.Instance{
			Name: instanceName,
			Spec: &computev1.InstanceSpec{
				MachineType: "vws.small",
				Power:       computev1.InstancePower_INSTANCE_POWER_STOPPED,
				KeyPair:     keyName,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: stopped.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	gotInst, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
	if err != nil {
		t.Fatal(err)
	}
	if gotInst.GetSpec().GetPower() != computev1.InstancePower_INSTANCE_POWER_STOPPED {
		t.Fatalf("after stop = %s", gotInst.GetSpec().GetPower())
	}

	deleted, err := h.inst.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{
		Name: instanceName, RequestId: "77777777-7777-4777-8777-777777777777",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: deleted.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	_, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
	wantCode(t, err, codes.NotFound)
}

func TestWaitOperationReturnsPendingWhenTimeoutElapses(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	release := h.agent.Block()
	defer release()

	reg, err := compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	op := h.submit(t, "img-seed", reg)
	name := "projects/demo/operations/" + op.ID.String()
	began := time.Now()
	got, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: name, Timeout: durationpb.New(100 * time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(began); got.GetDone() || elapsed < 100*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("WaitOperation done=%v after %v, want not done after about 100ms", got.GetDone(), elapsed)
	}
	if got.GetError() != nil || got.GetResponse() != nil {
		t.Fatalf("pending operation has result %v", got.GetResult())
	}

	_, err = h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: name, Timeout: durationpb.New(-time.Second)})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "projects/demo/operations/op-00000000000000000000"})
	wantCode(t, err, codes.NotFound)
	_, err = h.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: "nope"})
	wantCode(t, err, codes.InvalidArgument)

	release()
	h.wait(t, op)
}

func TestFailedChangeSurfacesOnOperationAndInstance(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	reg, err := compute.NewRegisterImage("debian-13", "https://images.example/d.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	imageOp := h.submit(t, "img-seed", reg)
	h.wait(t, imageOp)

	release := h.agent.Block()
	run, err := compute.NewRunInstance(imageOp.Target.ID, "vws.small", "")
	if err != nil {
		release()
		t.Fatal(err)
	}
	runOp := h.submit(t, "run-seed", run)
	h.agent.Fail(runOp.Target.ID, hypervisor.Permanent(errors.New("disk corrupt")))
	release()

	got, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{
		Name:    "projects/demo/operations/" + runOp.ID.String(),
		Timeout: durationpb.New(10 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.GetDone() || codes.Code(got.GetError().GetCode()) != codes.FailedPrecondition || got.GetError().GetMessage() != "disk corrupt" {
		t.Fatalf("run = done %v %v, want FAILED_PRECONDITION disk corrupt", got.GetDone(), got.GetError())
	}
	inst, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{
		Name: "projects/demo/zones/demo/instances/" + runOp.Target.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	obs := inst.GetStatus().GetObserved()
	if obs.GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR || obs.GetMessage() != "disk corrupt" {
		t.Fatalf("instance status = %s %q", obs.GetState(), obs.GetMessage())
	}
}
