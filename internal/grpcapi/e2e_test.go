package grpcapi_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
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
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store"
	"github.com/popododo0720/vws-compute/internal/store/kubestore/kubetest"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

type harness struct {
	inst  computev1.InstanceServiceClient
	img   computev1.ImageServiceClient
	keys  computev1.KeyPairServiceClient
	ops   longrunningpb.OperationsClient
	agent *fakeagent.Agent
	ctl   *controller.Controller
	stop  func()
}

func serve(t *testing.T) *harness {
	t.Helper()
	return serveRepo(t, memstore.New().WithLegacyImageScope(demoScope))
}

func serveRepo(t *testing.T, repo store.Repository, opts ...grpcapi.Option) *harness {
	t.Helper()
	agent := fakeagent.New()
	rec := reconciler.New(repo, agent, reconciler.WithInterval(10*time.Millisecond))
	ctl := controller.New(repo, controller.WithKick(rec.Kick))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.WaitForHandlers(true))
	grpcapi.Register(srv, ctl, repo, opts...)
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
	h := &harness{
		inst:  computev1.NewInstanceServiceClient(conn),
		img:   computev1.NewImageServiceClient(conn),
		keys:  computev1.NewKeyPairServiceClient(conn),
		ops:   longrunningpb.NewOperationsClient(conn),
		agent: agent,
		ctl:   ctl,
	}
	var once sync.Once
	h.stop = func() {
		once.Do(func() {
			_ = conn.Close()
			srv.Stop()
			cancel()
			<-stopped
			_ = repo.Close()
		})
	}
	t.Cleanup(h.stop)
	return h
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
	stoppedMachine, ok := h.agent.Machine(instanceID)
	if !ok || stoppedMachine.Power != hypervisor.PowerOff {
		t.Fatalf("agent after stop = %+v exists %v", stoppedMachine, ok)
	}

	started, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId: "88888888-8888-4888-8888-888888888888",
		Instance: &computev1.Instance{
			Name: instanceName,
			Spec: &computev1.InstanceSpec{
				MachineType: "vws.small",
				Power:       computev1.InstancePower_INSTANCE_POWER_RUNNING,
				KeyPair:     keyName,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: started.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	gotInst, err = h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
	if err != nil {
		t.Fatal(err)
	}
	if gotInst.GetSpec().GetPower() != computev1.InstancePower_INSTANCE_POWER_RUNNING {
		t.Fatalf("after start = %s", gotInst.GetSpec().GetPower())
	}
	if got := h.agent.Creations(instanceID); got != 1 {
		t.Fatalf("creations after start = %d, want 1", got)
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
	if _, exists := h.agent.Machine(instanceID); exists {
		t.Fatal("machine exists after terminate")
	}
}

func TestCreateImageRoundTrip(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	parent := "projects/demo/regions/demo"
	snap := parent + "/snapshots/snap"
	name := parent + "/images/debian-12"
	created, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
		Parent: parent, ImageId: "debian-12", RequestId: "44444444-4444-4444-8444-444444444444",
		Image: &computev1.Image{Spec: &computev1.ImageSpec{
			Family: "debian-12",
			Source: &computev1.ImageSpec_Snapshot{Snapshot: snap},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: created.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	got, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetName() != name || got.GetSpec().GetSnapshot() != snap || got.GetSpec().GetVolume() != "" ||
		got.GetSpec().GetFamily() != "debian-12" || got.GetStatus().GetSizeGib() != 2 ||
		got.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY ||
		got.GetStatus().GetProgressPercent() != 100 {
		t.Fatalf("image = %v", got)
	}
	listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: parent})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetImages()) != 1 || listed.GetImages()[0].GetName() != name {
		t.Fatalf("list = %v", listed.GetImages())
	}

	_, err = h.img.CreateImage(ctx, &computev1.CreateImageRequest{
		Parent: parent, ImageId: "debian-12", RequestId: "55555555-5555-4555-8555-555555555555",
		Image: &computev1.Image{Spec: &computev1.ImageSpec{
			Source: &computev1.ImageSpec_Snapshot{Snapshot: snap},
		}},
	})
	wantCode(t, err, codes.AlreadyExists)

	only, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
		Parent: parent, ImageId: "other-disk", RequestId: "66666666-6666-4666-8666-666666666666",
		ValidateOnly: true,
		Image: &computev1.Image{Spec: &computev1.ImageSpec{
			Source: &computev1.ImageSpec_Volume{Volume: "projects/demo/zones/demo/volumes/root"},
		}},
	})
	if err != nil || !only.GetDone() {
		t.Fatalf("validate_only = %v %v", only, err)
	}
	_, err = h.img.GetImage(ctx, &computev1.GetImageRequest{Name: parent + "/images/other-disk"})
	wantCode(t, err, codes.NotFound)

	deleted, err := h.img.DeleteImage(ctx, &computev1.DeleteImageRequest{
		Name: name, RequestId: "77777777-7777-4777-8777-777777777777",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: deleted.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	_, err = h.img.GetImage(ctx, &computev1.GetImageRequest{Name: name})
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

func operationResource(t *testing.T, op *longrunningpb.Operation) string {
	t.Helper()
	var md commonv1.OperationMetadata
	if err := anypb.UnmarshalTo(op.GetMetadata(), &md, proto.UnmarshalOptions{}); err != nil {
		t.Fatal(err)
	}
	return md.GetResource()
}

func unpackKeyPair(t *testing.T, op *longrunningpb.Operation) *computev1.KeyPair {
	t.Helper()
	var got computev1.KeyPair
	if err := anypb.UnmarshalTo(op.GetResponse(), &got, proto.UnmarshalOptions{}); err != nil {
		t.Fatal(err)
	}
	return &got
}

func seedRunning(t *testing.T, h *harness) (instanceName, imageName string) {
	t.Helper()
	reg, err := compute.NewRegisterImage("debian-13", "https://images.example/debian-13.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	imageOp := h.submit(t, "img-seed", reg)
	h.wait(t, imageOp)
	run, err := compute.NewRunInstance(imageOp.Target.ID, "vws.small", "")
	if err != nil {
		t.Fatal(err)
	}
	runOp := h.submit(t, "run-seed", run)
	h.wait(t, runOp)
	return "projects/demo/zones/demo/instances/" + runOp.Target.ID,
		"projects/demo/regions/demo/images/" + imageOp.Target.ID
}

func TestConsoleKeyPairWithFingerprintLabel(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	line, _ := ed25519Line(t)
	parent := "projects/a12345/regions/us-east1"
	name := parent + "/keyPairs/console"
	req := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	body := &computev1.KeyPair{
		Spec:   &computev1.KeyPairSpec{PublicKey: line},
		Labels: map[string]string{"vws-fingerprint": "fp-console"},
	}
	created, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "console", RequestId: req, KeyPair: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := unpackKeyPair(t, created)
	if got.GetName() != name || got.GetLabels()["vws-fingerprint"] != "fp-console" || got.GetGeneration() != 1 || got.GetEtag() != "1" {
		t.Fatalf("created = name %s gen %d etag %s labels %v", got.GetName(), got.GetGeneration(), got.GetEtag(), got.GetLabels())
	}
	if resource := operationResource(t, created); resource != name {
		t.Fatalf("create resource = %s, want %s", resource, name)
	}

	replay, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "console", RequestId: req, KeyPair: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.GetName() != created.GetName() {
		t.Fatalf("replay = %s, want %s", replay.GetName(), created.GetName())
	}

	_, err = h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "console", RequestId: req,
		KeyPair: &computev1.KeyPair{
			Spec:   &computev1.KeyPairSpec{PublicKey: line},
			Labels: map[string]string{"vws-fingerprint": "other"},
		},
	})
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "IDEMPOTENCY_CONFLICT")

	updated, err := h.keys.UpdateKeyPair(ctx, &computev1.UpdateKeyPairRequest{
		RequestId:  "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels.vws-fingerprint"}},
		KeyPair: &computev1.KeyPair{
			Name: name, Etag: got.GetEtag(),
			Labels: map[string]string{"vws-fingerprint": "fp-next"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	next := unpackKeyPair(t, updated)
	if next.GetGeneration() != 1 || next.GetEtag() != "1.1" || next.GetLabels()["vws-fingerprint"] != "fp-next" {
		t.Fatalf("updated = gen %d etag %s labels %v", next.GetGeneration(), next.GetEtag(), next.GetLabels())
	}

	again, err := h.keys.UpdateKeyPair(ctx, &computev1.UpdateKeyPairRequest{
		RequestId:  "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels.vws-fingerprint"}},
		KeyPair: &computev1.KeyPair{
			Name: name, Etag: "1",
			Labels: map[string]string{"vws-fingerprint": "fp-next"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.GetName() != updated.GetName() {
		t.Fatalf("stale-etag retry = %s, want %s", again.GetName(), updated.GetName())
	}
	if fetched, err := h.keys.GetKeyPair(ctx, &computev1.GetKeyPairRequest{Name: name}); err != nil || fetched.GetLabels()["vws-fingerprint"] != "fp-next" {
		t.Fatalf("get after retry = %v, %v", fetched.GetLabels(), err)
	}

	_, err = h.keys.UpdateKeyPair(ctx, &computev1.UpdateKeyPairRequest{
		RequestId:  "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels.vws-fingerprint"}},
		KeyPair: &computev1.KeyPair{
			Name: name, Etag: "1",
			Labels: map[string]string{"vws-fingerprint": "fp-stale"},
		},
	})
	wantCode(t, err, codes.Aborted)
	wantReason(t, err, "ETAG_MISMATCH")

	_, err = h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: "projects/a<accountId>/regions/us-east1", KeyPairId: "console",
		RequestId: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		KeyPair:   &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: line}},
	})
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_RESOURCE_ID")

	missing := parent + "/keyPairs/later"
	other, _ := ed25519Line(t)
	importReq := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	imported, err := h.keys.UpdateKeyPair(ctx, &computev1.UpdateKeyPairRequest{
		RequestId: importReq, AllowMissing: true,
		KeyPair: &computev1.KeyPair{
			Name:   missing,
			Spec:   &computev1.KeyPairSpec{PublicKey: other},
			Labels: map[string]string{"vws-fingerprint": "fp-import"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	retried, err := h.keys.UpdateKeyPair(ctx, &computev1.UpdateKeyPairRequest{
		RequestId: importReq, AllowMissing: true,
		KeyPair: &computev1.KeyPair{
			Name:   missing,
			Spec:   &computev1.KeyPairSpec{PublicKey: other},
			Labels: map[string]string{"vws-fingerprint": "fp-import"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if retried.GetName() != imported.GetName() || unpackKeyPair(t, retried).GetLabels()["vws-fingerprint"] != "fp-import" {
		t.Fatalf("allow_missing retry = %s labels %v", retried.GetName(), unpackKeyPair(t, retried).GetLabels())
	}
}

func TestUpdateMaskSemantics(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	instanceName, imageName := seedRunning(t, h)

	_, err := h.inst.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/demo", InstanceId: "bad", RequestId: "11111111-1111-4111-8111-111111111111",
		Instance: &computev1.Instance{
			Labels: map[string]string{"Bad": "x"},
			Spec: &computev1.InstanceSpec{
				MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
				Volumes: []*computev1.VolumeAttachment{{
					Volume: "projects/demo/zones/demo/volumes/root", DeviceName: "boot", Boot: true,
				}},
			},
		},
	})
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")

	_, err = h.img.CreateImage(ctx, &computev1.CreateImageRequest{
		Parent: "projects/demo/regions/demo", ImageId: "bad", RequestId: "22222222-2222-4222-8222-222222222222",
		Image: &computev1.Image{Spec: &computev1.ImageSpec{
			Source:      &computev1.ImageSpec_Snapshot{Snapshot: "projects/demo/regions/demo/snapshots/snap"},
			Description: "has\x00nul",
		}},
	})
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")

	set := func(req string, mask []string, labels map[string]string, power computev1.InstancePower) *computev1.Instance {
		t.Helper()
		op, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
			RequestId: req, UpdateMask: &fieldmaskpb.FieldMask{Paths: mask},
			Instance: &computev1.Instance{
				Name: instanceName, Labels: labels,
				Spec: &computev1.InstanceSpec{Power: power},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: op.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		got, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	got := set("33333333-3333-4333-8333-333333333333", []string{"labels.vws-fingerprint"}, map[string]string{"vws-fingerprint": "fp"}, 0)
	if got.GetGeneration() != 1 || got.GetEtag() != "1.1" || got.GetLabels()["vws-fingerprint"] != "fp" {
		t.Fatalf("fingerprint label = gen %d etag %s labels %v", got.GetGeneration(), got.GetEtag(), got.GetLabels())
	}
	got = set("44444444-4444-4444-8444-444444444444", []string{"labels.role"}, map[string]string{"role": "web"}, 0)
	if got.GetGeneration() != 1 || got.GetLabels()["vws-fingerprint"] != "fp" || got.GetLabels()["role"] != "web" || got.GetEtag() != "1.2" {
		t.Fatalf("per-key label = gen %d etag %s labels %v", got.GetGeneration(), got.GetEtag(), got.GetLabels())
	}
	got = set("55555555-5555-4555-8555-555555555555", []string{"labels.role"}, nil, 0)
	if _, ok := got.GetLabels()["role"]; ok || got.GetLabels()["vws-fingerprint"] != "fp" || got.GetGeneration() != 1 {
		t.Fatalf("delete role = gen %d labels %v", got.GetGeneration(), got.GetLabels())
	}
	got = set("66666666-6666-4666-8666-666666666666", []string{"labels"}, map[string]string{"role": "web"}, 0)
	if len(got.GetLabels()) != 1 || got.GetLabels()["role"] != "web" {
		t.Fatalf("replace labels = %v", got.GetLabels())
	}
	got = set("77777777-7777-4777-8777-777777777777", []string{"labels"}, nil, 0)
	if len(got.GetLabels()) != 0 || got.GetGeneration() != 1 {
		t.Fatalf("clear labels = gen %d %v", got.GetGeneration(), got.GetLabels())
	}

	got = set("88888888-8888-4888-8888-888888888888", []string{"labels"}, map[string]string{"vws-fingerprint": "kept"}, 0)
	rev := got.GetEtag()
	got = set("99999999-9999-4999-8999-999999999999", nil, nil, computev1.InstancePower_INSTANCE_POWER_STOPPED)
	if got.GetSpec().GetPower() != computev1.InstancePower_INSTANCE_POWER_STOPPED || got.GetLabels()["vws-fingerprint"] != "kept" || got.GetGeneration() != 2 {
		t.Fatalf("stop with empty labels = gen %d power %s labels %v", got.GetGeneration(), got.GetSpec().GetPower(), got.GetLabels())
	}
	if got.GetEtag() == rev {
		t.Fatalf("etag stayed %s across a generation change", rev)
	}

	got = set("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaab", []string{"spec"}, map[string]string{"role": "ignored"}, computev1.InstancePower_INSTANCE_POWER_RUNNING)
	if _, ok := got.GetLabels()["role"]; ok || got.GetLabels()["vws-fingerprint"] != "kept" || got.GetSpec().GetPower() != computev1.InstancePower_INSTANCE_POWER_RUNNING {
		t.Fatalf("spec-only update = power %s labels %v", got.GetSpec().GetPower(), got.GetLabels())
	}

	before, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbc", UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}},
		Instance: &computev1.Instance{
			Name: instanceName, Labels: map[string]string{"Bad": "x"},
			Spec: &computev1.InstanceSpec{Power: computev1.InstancePower_INSTANCE_POWER_STOPPED},
		},
	})
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")
	after, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
	if err != nil || after.GetGeneration() != before.GetGeneration() || after.GetEtag() != before.GetEtag() {
		t.Fatalf("invalid labels changed the instance: %v %v", after, err)
	}

	img, err := h.img.UpdateImage(ctx, &computev1.UpdateImageRequest{
		RequestId: "cccccccc-cccc-4ccc-8ccc-cccccccccccd", UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.description"}},
		Image: &computev1.Image{Name: imageName, Spec: &computev1.ImageSpec{Description: "line1\nline2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !img.GetDone() {
		t.Fatal("description update is still running")
	}
	gotImg, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: imageName})
	if err != nil {
		t.Fatal(err)
	}
	if gotImg.GetSpec().GetDescription() != "line1\nline2" || gotImg.GetSpec().GetFamily() != "debian-13" ||
		gotImg.GetGeneration() != 1 || gotImg.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY {
		t.Fatalf("image after description = gen %d family %s desc %q state %s", gotImg.GetGeneration(), gotImg.GetSpec().GetFamily(), gotImg.GetSpec().GetDescription(), gotImg.GetStatus().GetObserved().GetState())
	}
	if _, err := h.img.UpdateImage(ctx, &computev1.UpdateImageRequest{
		RequestId: "dddddddd-dddd-4ddd-8ddd-ddddddddddde", UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.family"}},
		Image: &computev1.Image{Name: imageName, Spec: &computev1.ImageSpec{Family: "debian-13", Description: "ignored"}},
	}); err != nil {
		t.Fatal(err)
	}
	gotImg, err = h.img.GetImage(ctx, &computev1.GetImageRequest{Name: imageName})
	if err != nil || gotImg.GetSpec().GetDescription() != "line1\nline2" {
		t.Fatalf("family echo changed description to %q (%v)", gotImg.GetSpec().GetDescription(), err)
	}
	_, err = h.img.UpdateImage(ctx, &computev1.UpdateImageRequest{
		RequestId: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeef", UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.family"}},
		Image: &computev1.Image{Name: imageName, Spec: &computev1.ImageSpec{Family: "other"}},
	})
	wantCode(t, err, codes.InvalidArgument)
	wantReason(t, err, "INVALID_SPEC")

	if _, err := h.img.UpdateImage(ctx, &computev1.UpdateImageRequest{
		RequestId: "ffffffff-ffff-4fff-8fff-ffffffffff00",
		Image: &computev1.Image{
			Name: imageName, Labels: map[string]string{"vws-fingerprint": "img"},
			Spec: &computev1.ImageSpec{Description: "replaced"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	gotImg, err = h.img.GetImage(ctx, &computev1.GetImageRequest{Name: imageName})
	if err != nil || gotImg.GetSpec().GetDescription() != "replaced" || gotImg.GetLabels()["vws-fingerprint"] != "img" || gotImg.GetGeneration() != 1 {
		t.Fatalf("omitted mask = gen %d desc %q labels %v", gotImg.GetGeneration(), gotImg.GetSpec().GetDescription(), gotImg.GetLabels())
	}
}

func TestUpdateResponsesKeepTheCallersLocation(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	instanceName, imageName := seedRunning(t, h)
	stopped, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId: "12121212-1212-4212-8212-121212121212",
		Instance: &computev1.Instance{
			Name: instanceName,
			Spec: &computev1.InstanceSpec{Power: computev1.InstancePower_INSTANCE_POWER_STOPPED},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := operationResource(t, stopped); got != instanceName {
		t.Fatalf("update resource = %s, want %s", got, instanceName)
	}
	polled, err := h.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: stopped.GetName()})
	if err != nil {
		t.Fatal(err)
	}
	if got := operationResource(t, polled); !strings.Contains(got, "/zones/default/") {
		t.Fatalf("GetOperation resource = %s, want the placeholder zone", got)
	}

	described, err := h.img.UpdateImage(ctx, &computev1.UpdateImageRequest{
		RequestId:  "13131313-1313-4313-8313-131313131313",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec.description"}},
		Image:      &computev1.Image{Name: imageName, Spec: &computev1.ImageSpec{Description: "kept"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := operationResource(t, described); got != imageName {
		t.Fatalf("image update resource = %s, want %s", got, imageName)
	}
}

func TestMetadataSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	kc, ns := kubetest.Namespace(t)
	h := serveRepo(t, kubetest.Open(t, kc, ns).WithLegacyImageScope(demoScope))
	line, _ := ed25519Line(t)
	parent := "projects/a12345/regions/us-east1"
	name := parent + "/keyPairs/console"
	req := "14141414-1414-4414-8414-141414141414"
	if _, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "console", RequestId: req,
		KeyPair: &computev1.KeyPair{
			Spec:   &computev1.KeyPairSpec{PublicKey: line},
			Labels: map[string]string{"vws-fingerprint": "fp-restart"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	instanceName, imageName := seedRunning(t, h)
	if _, err := h.inst.UpdateInstance(ctx, &computev1.UpdateInstanceRequest{
		RequestId:  "15151515-1515-4515-8515-151515151515",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		Instance:   &computev1.Instance{Name: instanceName, Labels: map[string]string{"vws-fingerprint": "inst"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.img.UpdateImage(ctx, &computev1.UpdateImageRequest{
		RequestId: "16161616-1616-4616-8616-161616161616",
		Image: &computev1.Image{
			Name: imageName, Labels: map[string]string{"vws-fingerprint": "img"},
			Spec: &computev1.ImageSpec{Description: "kept\nacross restart"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	h.stop()

	h = serveRepo(t, kubetest.Open(t, kc, ns))
	got, err := h.keys.GetKeyPair(ctx, &computev1.GetKeyPairRequest{Name: name})
	if err != nil || got.GetLabels()["vws-fingerprint"] != "fp-restart" {
		t.Fatalf("key pair after restart = %v %v", got.GetLabels(), err)
	}
	replay, err := h.keys.CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: parent, KeyPairId: "console", RequestId: req,
		KeyPair: &computev1.KeyPair{
			Spec:   &computev1.KeyPairSpec{PublicKey: line},
			Labels: map[string]string{"vws-fingerprint": "fp-restart"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !replay.GetDone() || unpackKeyPair(t, replay).GetName() != name {
		t.Fatalf("replay after restart = done %v name %s", replay.GetDone(), unpackKeyPair(t, replay).GetName())
	}
	inst, err := h.inst.GetInstance(ctx, &computev1.GetInstanceRequest{Name: instanceName})
	if err != nil || inst.GetLabels()["vws-fingerprint"] != "inst" {
		t.Fatalf("instance after restart = %v %v", inst.GetLabels(), err)
	}
	img, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: imageName})
	if err != nil || img.GetLabels()["vws-fingerprint"] != "img" || img.GetSpec().GetDescription() != "kept\nacross restart" ||
		img.GetStatus().GetObserved().GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_READY {
		t.Fatalf("image after restart = desc %q labels %v state %s err %v", img.GetSpec().GetDescription(), img.GetLabels(), img.GetStatus().GetObserved().GetState(), err)
	}
}

// demoScope is the scope a test harness gives images registered without one,
// as -legacy-image-scope does in production.
var demoScope, _ = compute.NewImageScope("demo", "demo")

// TestImageOfAnotherProjectIsNotFound pins vws-compute#92: an image is read,
// listed, and deleted only under the project and region that created it, and
// another owner sees the same NotFound as for no image at all.
func TestImageOfAnotherProjectIsNotFound(t *testing.T) {
	ctx := context.Background()
	h := serve(t)
	owner := "projects/a111111111111/regions/vws-1"
	created, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
		Parent: owner, ImageId: "ami-0000000000000aaa1", RequestId: "92929292-9292-4292-8292-929292929292",
		Image: &computev1.Image{Spec: &computev1.ImageSpec{
			Source: &computev1.ImageSpec_Snapshot{Snapshot: owner + "/snapshots/s"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: created.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: owner + "/images/ami-0000000000000aaa1"}); err != nil {
		t.Fatal(err)
	}
	for _, parent := range []string{"projects/a222222222222/regions/vws-1", "projects/a111111111111/regions/vws-2"} {
		_, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: parent + "/images/ami-0000000000000aaa1"})
		wantCode(t, err, codes.NotFound)
		_, missing := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: parent + "/images/ami-0000000000000bbb2"})
		if status.Convert(err).Message() != strings.ReplaceAll(status.Convert(missing).Message(), "bbb2", "aaa1") {
			t.Fatalf("other owner %q, absent %q", err, missing)
		}
		_, err = h.img.DeleteImage(ctx, &computev1.DeleteImageRequest{Name: parent + "/images/ami-0000000000000aaa1", RequestId: "93939393-9393-4393-8393-93939393939" + parent[len(parent)-1:]})
		wantCode(t, err, codes.NotFound)
		listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: parent})
		if err != nil {
			t.Fatal(err)
		}
		if len(listed.GetImages()) != 0 {
			t.Fatalf("%s lists %v", parent, listed.GetImages())
		}
	}
}

// TestPublicImageIsReadableEverywhereInItsRegion: a public image is listed
// and read from every project in its region, while update and delete from
// another project get the same NotFound as a private image, and another
// region does not see it at all.
func TestPublicImageIsReadableEverywhereInItsRegion(t *testing.T) {
	ctx := context.Background()
	pub, err := compute.NewPublicImages([]string{"ami-0000000000000aaa1"})
	if err != nil {
		t.Fatal(err)
	}
	h := serveRepo(t, memstore.New(), grpcapi.WithPublicImages(pub))
	owner := "projects/a111111111111/regions/vws-1"
	for i, id := range []string{"ami-0000000000000aaa1", "ami-0000000000000ccc3"} {
		created, err := h.img.CreateImage(ctx, &computev1.CreateImageRequest{
			Parent: owner, ImageId: id, RequestId: fmt.Sprintf("9494949%d-9494-4494-8494-949494949494", i),
			Image: &computev1.Image{Spec: &computev1.ImageSpec{
				Source: &computev1.ImageSpec_Snapshot{Snapshot: owner + "/snapshots/s"},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: created.GetName(), Timeout: durationpb.New(10 * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	other := "projects/a222222222222/regions/vws-1"
	got, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: other + "/images/ami-0000000000000aaa1"})
	if err != nil {
		t.Fatalf("public image from another project: %v", err)
	}
	if got.GetName() != other+"/images/ami-0000000000000aaa1" {
		t.Fatalf("name %q", got.GetName())
	}
	listed, err := h.img.ListImages(ctx, &computev1.ListImagesRequest{Parent: other})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetImages()) != 1 || listed.GetImages()[0].GetName() != other+"/images/ami-0000000000000aaa1" {
		t.Fatalf("other project lists %v, want only the public image", listed.GetImages())
	}
	_, private := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: other + "/images/ami-0000000000000ccc3"})
	wantCode(t, private, codes.NotFound)

	_, err = h.img.DeleteImage(ctx, &computev1.DeleteImageRequest{Name: other + "/images/ami-0000000000000aaa1", RequestId: "95959595-9595-4595-8595-959595959595"})
	wantCode(t, err, codes.NotFound)
	if status.Convert(err).Message() != "image "+other+"/images/ami-0000000000000aaa1 not found" {
		t.Fatalf("delete message %q", status.Convert(err).Message())
	}
	_, err = h.img.UpdateImage(ctx, &computev1.UpdateImageRequest{
		RequestId:  "96969696-9696-4696-8696-969696969696",
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		Image:      &computev1.Image{Name: other + "/images/ami-0000000000000aaa1", Labels: map[string]string{"k": "v"}},
	})
	wantCode(t, err, codes.NotFound)

	_, err = h.img.GetImage(ctx, &computev1.GetImageRequest{Name: "projects/a222222222222/regions/vws-2/images/ami-0000000000000aaa1"})
	wantCode(t, err, codes.NotFound)
	if _, err := h.img.GetImage(ctx, &computev1.GetImageRequest{Name: owner + "/images/ami-0000000000000aaa1"}); err != nil {
		t.Fatalf("owner: %v", err)
	}
}
