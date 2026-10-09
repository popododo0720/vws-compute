package grpcapi_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	pb "github.com/popododo0720/vws-compute/internal/grpcapi/internal/computepb"
	"github.com/popododo0720/vws-compute/internal/hypervisor"
	"github.com/popododo0720/vws-compute/internal/hypervisor/fakeagent"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

// serve wires the real service over an in-memory connection.
func serve(t *testing.T) (pb.ComputeServiceClient, *fakeagent.Agent) {
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
	return pb.NewComputeServiceClient(conn), agent
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("err = %v, want %s", err, want)
	}
}

func done(t *testing.T, c pb.ComputeServiceClient, op *pb.Operation) *pb.Operation {
	t.Helper()
	got, err := c.WaitOperation(context.Background(), &pb.WaitOperationRequest{OperationId: op.GetOperationId(), Timeout: durationpb.New(10 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetState() != pb.OperationState_OPERATION_STATE_SUCCEEDED || got.GetEndTime() == nil {
		t.Fatalf("operation %s (%s) = %s, want SUCCEEDED with an end time", got.GetOperationId(), got.GetKind(), got.GetState())
	}
	return got
}

func describe(t *testing.T, c pb.ComputeServiceClient, id string) *pb.Instance {
	t.Helper()
	resp, err := c.DescribeInstances(context.Background(), &pb.DescribeInstancesRequest{InstanceIds: []string{id}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetInstances()) != 1 {
		t.Fatalf("describe %s returned %d instances", id, len(resp.GetInstances()))
	}
	return resp.GetInstances()[0]
}

func TestComputeServiceEndToEnd(t *testing.T) {
	ctx := context.Background()
	c, agent := serve(t)

	created, err := c.CreateKeyPair(ctx, &pb.CreateKeyPairRequest{RequestId: "key-1", KeyName: "deploy"})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetOperation().GetState() != pb.OperationState_OPERATION_STATE_SUCCEEDED || created.GetKeyPair().GetKeyType() != pb.KeyType_KEY_TYPE_ED25519 {
		t.Fatalf("CreateKeyPair = %s %s, want SUCCEEDED ed25519", created.GetOperation().GetState(), created.GetKeyPair().GetKeyType())
	}
	signer, err := ssh.ParsePrivateKey([]byte(created.GetPrivateKeyOpenssh()))
	if err != nil {
		t.Fatalf("private key: %v", err)
	}
	if got := ssh.FingerprintSHA256(signer.PublicKey()); got != created.GetKeyPair().GetFingerprint() {
		t.Fatalf("fingerprint = %s, want %s from the private key", created.GetKeyPair().GetFingerprint(), got)
	}

	replay, err := c.CreateKeyPair(ctx, &pb.CreateKeyPairRequest{RequestId: "key-1", KeyName: "deploy"})
	if err != nil {
		t.Fatal(err)
	}
	if replay.GetOperation().GetOperationId() != created.GetOperation().GetOperationId() || replay.GetPrivateKeyOpenssh() != "" ||
		replay.GetKeyPair().GetKeyPairId() != created.GetKeyPair().GetKeyPairId() {
		t.Fatalf("replay = op %s key %q, want op %s and an empty key", replay.GetOperation().GetOperationId(), replay.GetPrivateKeyOpenssh(), created.GetOperation().GetOperationId())
	}
	got, err := c.GetOperation(ctx, &pb.GetOperationRequest{OperationId: created.GetOperation().GetOperationId()})
	if err != nil {
		t.Fatal(err)
	}
	if raw, _ := proto.Marshal(got); bytes.Contains(raw, []byte("PRIVATE KEY")) || got.GetKeyPairId() != created.GetKeyPair().GetKeyPairId() {
		t.Fatalf("GetOperation = %v, want the key pair operation without key material", got)
	}

	reg, err := c.RegisterImage(ctx, &pb.RegisterImageRequest{RequestId: "img-1", Name: "debian-13", SourceUrl: "https://images.example/debian-13.qcow2"})
	if err != nil {
		t.Fatal(err)
	}
	imageID := done(t, c, reg.GetOperation()).GetImageId()
	imgs, err := c.DescribeImages(ctx, &pb.DescribeImagesRequest{ImageIds: []string{imageID}})
	if err != nil {
		t.Fatal(err)
	}
	if img := imgs.GetImages()[0]; img.GetState() != pb.ImageState_IMAGE_STATE_AVAILABLE || img.GetSourceUrl() != "https://images.example/debian-13.qcow2" || img.GetSizeBytes() != fakeagent.ImageSize {
		t.Fatalf("image = %v, want AVAILABLE from its URL", img)
	}

	run, err := c.RunInstance(ctx, &pb.RunInstanceRequest{RequestId: "run-1", ImageId: imageID, InstanceType: "vws.small", KeyName: "deploy"})
	if err != nil {
		t.Fatal(err)
	}
	if run.GetOperation().GetKind() != pb.OperationKind_OPERATION_KIND_RUN_INSTANCE {
		t.Fatalf("run kind = %s", run.GetOperation().GetKind())
	}
	id := done(t, c, run.GetOperation()).GetInstanceId()
	inst := describe(t, c, id)
	if inst.GetState() != pb.InstanceState_INSTANCE_STATE_RUNNING || inst.GetInstanceType() != "vws.small" ||
		inst.GetKeyName() != "deploy" || inst.GetKeyFingerprint() != created.GetKeyPair().GetFingerprint() {
		t.Fatalf("instance = %v, want RUNNING vws.small with key deploy", inst)
	}
	m, _ := agent.Machine(id)
	if want := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))); len(m.AuthorizedKeys) != 1 || m.AuthorizedKeys[0] != want {
		t.Fatalf("machine authorized keys = %q, want [%q]", m.AuthorizedKeys, want)
	}

	stop, err := c.StopInstance(ctx, &pb.StopInstanceRequest{RequestId: "stop-1", InstanceId: id})
	if err != nil {
		t.Fatal(err)
	}
	done(t, c, stop.GetOperation())
	if st := describe(t, c, id).GetState(); st != pb.InstanceState_INSTANCE_STATE_STOPPED {
		t.Fatalf("after stop = %s, want STOPPED", st)
	}
	if m, _ := agent.Machine(id); m.Power != hypervisor.PowerOff {
		t.Fatalf("machine power after stop = %d, want off", m.Power)
	}
	start, err := c.StartInstance(ctx, &pb.StartInstanceRequest{RequestId: "start-1", InstanceId: id})
	if err != nil {
		t.Fatal(err)
	}
	done(t, c, start.GetOperation())
	if st := describe(t, c, id).GetState(); st != pb.InstanceState_INSTANCE_STATE_RUNNING {
		t.Fatalf("after start = %s, want RUNNING", st)
	}

	snap, err := c.CreateImage(ctx, &pb.CreateImageRequest{RequestId: "snap-1", InstanceId: id, Name: "snapshot-1"})
	if err != nil {
		t.Fatal(err)
	}
	snapID := done(t, c, snap.GetOperation()).GetImageId()
	imgs, err = c.DescribeImages(ctx, &pb.DescribeImagesRequest{ImageIds: []string{snapID}})
	if err != nil {
		t.Fatal(err)
	}
	if img := imgs.GetImages()[0]; img.GetSourceInstanceId() != id || img.GetState() != pb.ImageState_IMAGE_STATE_AVAILABLE {
		t.Fatalf("snapshot = %v, want AVAILABLE from %s", img, id)
	}

	_, err = c.StopInstance(ctx, &pb.StopInstanceRequest{RequestId: "run-1", InstanceId: id})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.StartInstance(ctx, &pb.StartInstanceRequest{InstanceId: id})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.StartInstance(ctx, &pb.StartInstanceRequest{RequestId: "start-x", InstanceId: "i-00000000000000000"})
	wantCode(t, err, codes.NotFound)
	_, err = c.RunInstance(ctx, &pb.RunInstanceRequest{RequestId: "run-x", ImageId: imageID, InstanceType: "vws.huge"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.CreateKeyPair(ctx, &pb.CreateKeyPairRequest{RequestId: "key-2", KeyName: "deploy"})
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.ImportKeyPair(ctx, &pb.ImportKeyPairRequest{RequestId: "key-3", KeyName: "other", PublicKeyOpenssh: "not a key"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.DescribeInstances(ctx, &pb.DescribeInstancesRequest{PageSize: -1})
	wantCode(t, err, codes.InvalidArgument)

	term, err := c.TerminateInstance(ctx, &pb.TerminateInstanceRequest{RequestId: "term-1", InstanceId: id})
	if err != nil {
		t.Fatal(err)
	}
	done(t, c, term.GetOperation())
	if st := describe(t, c, id).GetState(); st != pb.InstanceState_INSTANCE_STATE_TERMINATED {
		t.Fatalf("after terminate = %s, want TERMINATED", st)
	}
	_, err = c.StartInstance(ctx, &pb.StartInstanceRequest{RequestId: "start-2", InstanceId: id})
	wantCode(t, err, codes.FailedPrecondition)

	del, err := c.DeleteKeyPair(ctx, &pb.DeleteKeyPairRequest{RequestId: "del-1", KeyName: "deploy"})
	if err != nil {
		t.Fatal(err)
	}
	if del.GetOperation().GetState() != pb.OperationState_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("delete = %s, want SUCCEEDED", del.GetOperation().GetState())
	}
	keys, err := c.DescribeKeyPairs(ctx, &pb.DescribeKeyPairsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys.GetKeyPairs()) != 0 {
		t.Fatalf("key pairs after delete = %d, want 0", len(keys.GetKeyPairs()))
	}
}

func TestWaitOperationReturnsPendingWhenTimeoutElapses(t *testing.T) {
	ctx := context.Background()
	c, agent := serve(t)
	release := agent.Block()
	defer release()

	reg, err := c.RegisterImage(ctx, &pb.RegisterImageRequest{RequestId: "img-1", Name: "debian-13", SourceUrl: "https://images.example/d.qcow2"})
	if err != nil {
		t.Fatal(err)
	}
	id := reg.GetOperation().GetOperationId()
	began := time.Now()
	got, err := c.WaitOperation(ctx, &pb.WaitOperationRequest{OperationId: id, Timeout: durationpb.New(100 * time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(began); got.GetState() != pb.OperationState_OPERATION_STATE_PENDING || elapsed < 100*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("WaitOperation = %s after %v, want PENDING after about 100ms", got.GetState(), elapsed)
	}
	if got.GetEndTime() != nil || got.GetError() != nil {
		t.Fatalf("pending operation has end time %v or error %v", got.GetEndTime(), got.GetError())
	}

	_, err = c.WaitOperation(ctx, &pb.WaitOperationRequest{OperationId: id, Timeout: durationpb.New(-time.Second)})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.GetOperation(ctx, &pb.GetOperationRequest{OperationId: "op-00000000000000000000"})
	wantCode(t, err, codes.NotFound)
	_, err = c.GetOperation(ctx, &pb.GetOperationRequest{OperationId: "nope"})
	wantCode(t, err, codes.InvalidArgument)

	release()
	done(t, c, reg.GetOperation())
}

func TestFailedChangeSurfacesOnOperationAndInstance(t *testing.T) {
	ctx := context.Background()
	c, agent := serve(t)
	reg, err := c.RegisterImage(ctx, &pb.RegisterImageRequest{RequestId: "img-1", Name: "debian-13", SourceUrl: "https://images.example/d.qcow2"})
	if err != nil {
		t.Fatal(err)
	}
	imageID := done(t, c, reg.GetOperation()).GetImageId()

	release := agent.Block()
	run, err := c.RunInstance(ctx, &pb.RunInstanceRequest{RequestId: "run-1", ImageId: imageID, InstanceType: "vws.small"})
	if err != nil {
		release()
		t.Fatal(err)
	}
	id := run.GetOperation().GetInstanceId()
	agent.Fail(id, hypervisor.Permanent(errors.New("disk corrupt")))
	release()

	got, err := c.WaitOperation(ctx, &pb.WaitOperationRequest{OperationId: run.GetOperation().GetOperationId(), Timeout: durationpb.New(10 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetState() != pb.OperationState_OPERATION_STATE_FAILED || codes.Code(got.GetError().GetCode()) != codes.FailedPrecondition ||
		got.GetError().GetMessage() != "disk corrupt" {
		t.Fatalf("run = %s %v, want FAILED with FAILED_PRECONDITION: disk corrupt", got.GetState(), got.GetError())
	}
	if inst := describe(t, c, id); inst.GetState() != pb.InstanceState_INSTANCE_STATE_ERROR || inst.GetStateReason() != "disk corrupt" {
		t.Fatalf("instance = %s %q, want ERROR: disk corrupt", inst.GetState(), inst.GetStateReason())
	}
}
