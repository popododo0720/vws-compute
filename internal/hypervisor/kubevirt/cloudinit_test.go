package kubevirt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/controller"
	"github.com/popododo0720/vws-compute/internal/grpcapi"
	"github.com/popododo0720/vws-compute/internal/reconciler"
	"github.com/popododo0720/vws-compute/internal/store/memstore"
)

const (
	testKey    = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl deploy@vws"
	testScript = "#!/bin/sh\necho vws-user-data > /var/tmp/vws-user-data\n"
)

// parts splits rendered multipart user data into decoded parts.
func parts(t *testing.T, rendered string) []struct{ typ, merge, body string } {
	t.Helper()
	head, body, ok := strings.Cut(rendered, "\r\n\r\n")
	if !ok {
		t.Fatalf("no header block in %q", rendered)
	}
	ct := strings.TrimPrefix(strings.Split(head, "\r\n")[0], "Content-Type: ")
	media, params, err := mime.ParseMediaType(ct)
	if err != nil || media != "multipart/mixed" {
		t.Fatalf("content type %q: %v", ct, err)
	}
	r := multipart.NewReader(strings.NewReader(body), params["boundary"])
	var out []struct{ typ, merge, body string }
	for {
		p, err := r.NextPart()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(p)
		if err != nil {
			t.Fatal(err)
		}
		if p.Header.Get("Content-Transfer-Encoding") == "base64" {
			if raw, err = base64.StdEncoding.DecodeString(strings.ReplaceAll(string(raw), "\r\n", "")); err != nil {
				t.Fatal(err)
			}
		}
		typ, _, _ := mime.ParseMediaType(p.Header.Get("Content-Type")) //nolint:errcheck // checked by comparison
		out = append(out, struct{ typ, merge, body string }{typ, p.Header.Get("Merge-Type"), string(raw)})
	}
}

func TestRenderUserData(t *testing.T) {
	if got, err := renderUserData(nil, ""); err != nil || got != "" {
		t.Fatalf("nothing = %q, %v", got, err)
	}
	if got, err := renderUserData(nil, testScript); err != nil || got != testScript {
		t.Fatalf("user data alone = %q, %v; want it unchanged", got, err)
	}
	got, err := renderUserData([]string{testKey}, "")
	if err != nil || got != "#cloud-config\nssh_authorized_keys: [\""+testKey+"\"]\n" {
		t.Fatalf("key alone = %q, %v", got, err)
	}

	for _, user := range []string{testScript, "#cloud-config\nssh_authorized_keys: [\"ssh-rsa AAAA user\"]\n", "no prefix\r\n--vws-tricky\r\n"} {
		got, err := renderUserData([]string{testKey}, user)
		if err != nil {
			t.Fatal(err)
		}
		again, _ := renderUserData([]string{testKey}, user) //nolint:errcheck // same input as above
		if again != got {
			t.Fatal("rendering is not stable")
		}
		ps := parts(t, got)
		if len(ps) != 2 || ps[0].body != user || !strings.Contains(ps[1].body, testKey) || ps[1].typ != "text/cloud-config" || ps[1].merge != keyMergeType {
			t.Fatalf("parts of %q = %+v", user, ps)
		}
		if want := userContentType(user); ps[0].typ != want {
			t.Fatalf("user part type = %q, want %q", ps[0].typ, want)
		}
	}
	if userContentType(testScript) != "text/x-shellscript" || userContentType("#cloud-config\n") != "text/cloud-config" {
		t.Fatal("content types")
	}
	if _, err := renderUserData([]string{testKey}, "Content-Type: multipart/mixed; boundary=x\n\n--x--\n"); err == nil {
		t.Fatal("MIME user data with a key was accepted")
	}
}

// strictVolumes decodes each of the VirtualMachine's template volumes into
// the KubeVirt v1.9.0 Volume type, failing on unknown fields. (Volumes are
// decoded one by one because VirtualMachineInstanceSpec has its own
// UnmarshalJSON, which bypasses DisallowUnknownFields.)
func strictVolumes(t *testing.T, vm *unstructured.Unstructured) []kubevirtv1.Volume {
	t.Helper()
	raw, ok, err := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "volumes")
	if err != nil || !ok {
		t.Fatalf("spec.template.spec.volumes: found %v, %v", ok, err)
	}
	out := make([]kubevirtv1.Volume, 0, len(raw))
	for _, r := range raw {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		var v kubevirtv1.Volume
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("volume is not a valid KubeVirt v1.9.0 Volume: %v\n%s", err, b)
		}
		out = append(out, v)
	}
	return out
}

// TestKeyAndUserDataReachCloudInit drives CreateInstance with a key pair and
// user data through the gRPC API and the reconciler to this agent on
// envtest. The VirtualMachine reads NoCloud user data from Secret
// web-1-cloudinit, which holds the user's script unchanged plus a
// cloud-config part with the key. Terminate deletes the Secret. On main the
// user data never reached the guest: the VM inlined a cloud-config with only
// the key.
func TestKeyAndUserDataReachCloudInit(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	claim(t, c, ns, "pvc-root-1", corev1.ReadWriteOnce)
	repo := memstore.New()
	rec := reconciler.New(repo, New(c, Config{Namespace: ns, ImageNamespace: ns}), reconciler.WithReports(repo))
	srv := grpc.NewServer()
	grpcapi.Register(srv, controller.New(repo), repo, grpcapi.WithVolumes(claimStorage{claim: "pvc-root-1"}))
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

	if _, err := computev1.NewKeyPairServiceClient(cc).CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
		Parent: "projects/demo/regions/demo", KeyPairId: "deploy", RequestId: "c1c1c1c1-c1c1-41c1-81c1-c1c1c1c1c1c1",
		KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: testKey}},
	}); err != nil {
		t.Fatal(err)
	}
	ins := computev1.NewInstanceServiceClient(cc)
	if _, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
		Parent: "projects/demo/zones/z1", InstanceId: "web-1", RequestId: "c2c2c2c2-c2c2-42c2-82c2-c2c2c2c2c2c2",
		Instance: &computev1.Instance{Spec: &computev1.InstanceSpec{
			MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
			KeyPair:  "projects/demo/regions/demo/keyPairs/deploy",
			UserData: testScript,
			Volumes:  []*computev1.VolumeAttachment{{Volume: "projects/demo/zones/z1/volumes/root-1", DeviceName: "vda", Boot: true}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}

	vm, err := get(t, c, "VirtualMachine", ns, "web-1")
	if err != nil {
		t.Fatal(err)
	}
	// Decode the rendered volumes strictly into the KubeVirt v1.9.0 API
	// types: a field KubeVirt does not know (such as the old
	// cloudInitNoCloud.userDataSecretRef, which its webhook ignores and then
	// rejects the VM for having no user data source) fails here.
	vols := strictVolumes(t, vm)
	var ref string
	for _, v := range vols {
		if v.Name != "cloudinitdisk" {
			continue
		}
		if v.CloudInitNoCloud == nil || v.CloudInitNoCloud.UserDataSecretRef == nil {
			t.Fatalf("cloudinitdisk has no cloudInitNoCloud.secretRef: %+v", v)
		}
		if v.CloudInitNoCloud.UserData != "" || v.CloudInitNoCloud.UserDataBase64 != "" {
			t.Fatal("user data is inlined in the VirtualMachine")
		}
		ref = v.CloudInitNoCloud.UserDataSecretRef.Name
	}
	if ref != "web-1-cloudinit" {
		t.Fatalf("cloudinitdisk secretRef = %q, want web-1-cloudinit (volumes %+v)", ref, vols)
	}
	var sec corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref}, &sec); err != nil {
		t.Fatal(err)
	}
	ps := parts(t, string(sec.Data[SecretKeyUserData]))
	keyBlob := strings.Join(strings.Fields(testKey)[:2], " ") // the stored key drops the comment
	if len(ps) != 2 || ps[0].body != testScript || ps[0].typ != "text/x-shellscript" || !strings.Contains(ps[1].body, keyBlob) {
		t.Fatalf("cloud-init parts = %+v", ps)
	}
	if sec.Labels[LabelManaged] != "true" || sec.Labels[LabelInstance] != "web-1" {
		t.Fatalf("secret labels = %v", sec.Labels)
	}

	if _, err := ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{
		Name: "projects/demo/zones/z1/instances/web-1", RequestId: "c3c3c3c3-c3c3-43c3-83c3-c3c3c3c3c3c3",
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref}, &sec); !apierrors.IsNotFound(err) {
		t.Fatalf("secret after terminate: %v, want not found", err)
	}
}

// TestCloudInitSecretIsOursOnly: a Secret of the same name we did not make
// is never overwritten or deleted.
func TestCloudInitSecretIsOursOnly(t *testing.T) {
	ctx := context.Background()
	c, ns, _ := newClient(t)
	a := New(c, Config{Namespace: ns, ImageNamespace: ns})
	foreign := &corev1.Secret{}
	foreign.Name, foreign.Namespace = cloudInitSecret("web-9"), ns
	foreign.Data = map[string][]byte{SecretKeyUserData: []byte("theirs")}
	if err := c.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	if err := a.ensureCloudInitSecret(ctx, "web-9", "ours"); err == nil {
		t.Fatal("overwrote a foreign secret")
	}
	if err := a.deleteCloudInitSecret(ctx, "web-9"); err != nil {
		t.Fatal(err)
	}
	var got corev1.Secret
	if err := c.Get(ctx, client.ObjectKeyFromObject(foreign), &got); err != nil || string(got.Data[SecretKeyUserData]) != "theirs" {
		t.Fatalf("foreign secret after delete = %v, %v", got.Data, err)
	}
	// A retried create converges our own Secret to the new data.
	if err := a.ensureCloudInitSecret(ctx, "web-8", "one"); err != nil {
		t.Fatal(err)
	}
	if err := a.ensureCloudInitSecret(ctx, "web-8", "two"); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: cloudInitSecret("web-8")}, &got); err != nil || string(got.Data[SecretKeyUserData]) != "two" {
		t.Fatalf("own secret = %v, %v", got.Data, err)
	}
}
