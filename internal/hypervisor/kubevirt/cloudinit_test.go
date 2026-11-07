package kubevirt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
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
	// testRaw is InstanceSpec.user_data_raw that is not UTF-8 (vws-compute#148).
	testRaw = "\xff\xfe#!/bin/sh\x00\x80echo vws-user-data\n"
)

func TestRenderUserData(t *testing.T) {
	keyConfig := "#cloud-config\nssh_authorized_keys: [\"" + testKey + "\"]\n"
	mimeUser := "Content-Type: multipart/mixed; boundary=x\n\n--x--\n"
	for _, tc := range []struct {
		name string
		keys []string
		user string
		want string
	}{
		{"neither", nil, "", ""},
		{"user data only", nil, testScript, testScript},
		{"key only", []string{testKey}, "", keyConfig},
		{"key and script: script unchanged", []string{testKey}, testScript, testScript},
		{"key and cloud-config: unchanged", []string{testKey}, "#cloud-config\nruncmd: [true]\n", "#cloud-config\nruncmd: [true]\n"},
		{"key and MIME user data: unchanged, no longer refused", []string{testKey}, mimeUser, mimeUser},
		{"raw user data that is not UTF-8: unchanged", nil, testRaw, testRaw},
		{"key and raw user data: unchanged", []string{testKey}, testRaw, testRaw},
	} {
		got, err := renderUserData(tc.keys, tc.user)
		if err != nil || got != tc.want {
			t.Errorf("%s: renderUserData = %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
}

func TestKeysData(t *testing.T) {
	if keysData(nil) != nil {
		t.Fatal("no keys must give no Secret data")
	}
	keys := make([]string, 12)
	for i := range keys {
		keys[i] = fmt.Sprintf("ssh-ed25519 AAAA%02d", i)
	}
	data := keysData(keys)
	names := make([]string, 0, len(data))
	for k := range data {
		names = append(names, k)
	}
	sort.Strings(names) // KubeVirt numbers the keys in file-name order
	// Entry "0" is the placeholder CirrOS skips; the keys follow from "1".
	if len(names) != len(keys)+1 || names[0] != keysPlaceholderName || !strings.HasPrefix(string(data[names[0]]), "#") {
		t.Fatalf("entries %v: want the comment placeholder first, then one per key", names)
	}
	for i, n := range names[1:] {
		if string(data[n]) != keys[i] {
			t.Fatalf("entry %d (%s) = %q, want %q: file-name order must keep the key order", i, n, data[n], keys[i])
		}
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

// strictAccessCredentials decodes the VM template's accessCredentials into
// the KubeVirt v1.9.0 type, failing on unknown fields.
func strictAccessCredentials(t *testing.T, vm *unstructured.Unstructured) []kubevirtv1.AccessCredential {
	t.Helper()
	raw, ok, err := unstructured.NestedSlice(vm.Object, "spec", "template", "spec", "accessCredentials")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return nil
	}
	out := make([]kubevirtv1.AccessCredential, 0, len(raw))
	for _, r := range raw {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		var ac kubevirtv1.AccessCredential
		if err := dec.Decode(&ac); err != nil {
			t.Fatalf("not a valid KubeVirt v1.9.0 AccessCredential: %v\n%s", err, b)
		}
		out = append(out, ac)
	}
	return out
}

// TestKeyAndUserDataReachCloudInit drives CreateInstance through the gRPC API
// and the reconciler to this agent on envtest, for a key pair only, user
// data only, and both. The user-data Secret holds the user's data unchanged
// (or, for a key alone, a key cloud-config). With a key pair, the VM's
// accessCredentials name Secret <id>-ssh-keys with noCloud propagation, so
// KubeVirt puts the key in the NoCloud meta-data public-keys. Terminate
// deletes both Secrets.
func TestKeyAndUserDataReachCloudInit(t *testing.T) {
	keyBlob := strings.Join(strings.Fields(testKey)[:2], " ") // the stored key drops the comment
	for i, tc := range []struct {
		name     string
		key      bool
		user     string
		raw      string
		wantUser string
	}{
		{"key pair only", true, "", "", "#cloud-config\nssh_authorized_keys: [\"" + keyBlob + "\"]\n"},
		{"user data only", false, testScript, "", testScript},
		{"key pair and user data", true, testScript, "", testScript},
		{"raw user data only", false, "", testRaw, testRaw},
		{"key pair and raw user data", true, "", testRaw, testRaw},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			spec := &computev1.InstanceSpec{
				MachineType: "vws.small", Power: computev1.InstancePower_INSTANCE_POWER_RUNNING,
				UserData: tc.user, UserDataRaw: []byte(tc.raw),
				Volumes: []*computev1.VolumeAttachment{{
					Volume: "projects/demo/zones/z1/volumes/root-1", DeviceName: "vda", Boot: true,
				}},
			}
			if tc.key {
				if _, err := computev1.NewKeyPairServiceClient(cc).CreateKeyPair(ctx, &computev1.CreateKeyPairRequest{
					Parent: "projects/demo/regions/demo", KeyPairId: "deploy", RequestId: fmt.Sprintf("c1c1c1c1-c1c1-41c1-81c1-c1c1c1c1c1c%d", i),
					KeyPair: &computev1.KeyPair{Spec: &computev1.KeyPairSpec{PublicKey: testKey}},
				}); err != nil {
					t.Fatal(err)
				}
				spec.KeyPair = "projects/demo/regions/demo/keyPairs/deploy"
			}
			ins := computev1.NewInstanceServiceClient(cc)
			if _, err := ins.CreateInstance(ctx, &computev1.CreateInstanceRequest{
				Parent: "projects/demo/zones/z1", InstanceId: "web-1", RequestId: fmt.Sprintf("c2c2c2c2-c2c2-42c2-82c2-c2c2c2c2c2c%d", i),
				Instance: &computev1.Instance{Spec: spec},
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
			// Strict decoding into the KubeVirt v1.9.0 types: a field KubeVirt
			// does not know (such as the old userDataSecretRef) fails here.
			var ref string
			for _, v := range strictVolumes(t, vm) {
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
				t.Fatalf("cloudinitdisk secretRef = %q, want web-1-cloudinit", ref)
			}
			var sec corev1.Secret
			if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref}, &sec); err != nil {
				t.Fatal(err)
			}
			if got := string(sec.Data[SecretKeyUserData]); got != tc.wantUser {
				t.Fatalf("user data = %q, want %q", got, tc.wantUser)
			}
			if sec.Labels[LabelManaged] != "true" || sec.Labels[LabelInstance] != "web-1" {
				t.Fatalf("secret labels = %v", sec.Labels)
			}

			acs := strictAccessCredentials(t, vm)
			var keys corev1.Secret
			keysErr := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "web-1-ssh-keys"}, &keys)
			if !tc.key {
				if len(acs) != 0 || !apierrors.IsNotFound(keysErr) {
					t.Fatalf("no key pair: accessCredentials = %+v, keys secret err = %v", acs, keysErr)
				}
			} else {
				if len(acs) != 1 || acs[0].SSHPublicKey == nil || acs[0].SSHPublicKey.Source.Secret == nil ||
					acs[0].SSHPublicKey.Source.Secret.SecretName != "web-1-ssh-keys" ||
					acs[0].SSHPublicKey.PropagationMethod.NoCloud == nil ||
					acs[0].SSHPublicKey.PropagationMethod.ConfigDrive != nil || acs[0].SSHPublicKey.PropagationMethod.QemuGuestAgent != nil {
					t.Fatalf("accessCredentials = %+v, want one sshPublicKey from web-1-ssh-keys with noCloud propagation", acs)
				}
				if keysErr != nil {
					t.Fatal(keysErr)
				}
				if len(keys.Data) != 2 || string(keys.Data[keysPlaceholderName]) != keysPlaceholderValue || string(keys.Data["key-000"]) != keyBlob || keys.Labels[LabelManaged] != "true" || keys.Labels[LabelInstance] != "web-1" {
					t.Fatalf("keys secret = %v %v", keys.Labels, keys.Data)
				}
			}

			if _, err := ins.DeleteInstance(ctx, &computev1.DeleteInstanceRequest{
				Name: "projects/demo/zones/z1/instances/web-1", RequestId: fmt.Sprintf("c3c3c3c3-c3c3-43c3-83c3-c3c3c3c3c3c%d", i),
			}); err != nil {
				t.Fatal(err)
			}
			if err := rec.Sweep(ctx); err != nil {
				t.Fatal(err)
			}
			for _, n := range []string{ref, "web-1-ssh-keys"} {
				if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: n}, &sec); !apierrors.IsNotFound(err) {
					t.Fatalf("secret %s after terminate: %v, want not found", n, err)
				}
			}
		})
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
	// The same holds for the keys Secret.
	fk := &corev1.Secret{}
	fk.Name, fk.Namespace = sshKeysSecret("web-9"), ns
	fk.Data = map[string][]byte{"theirs": []byte("ssh-ed25519 AAAA theirs")}
	if err := c.Create(ctx, fk); err != nil {
		t.Fatal(err)
	}
	if err := a.ensureSecret(ctx, "web-9", fk.Name, keysData([]string{testKey})); err == nil {
		t.Fatal("overwrote a foreign keys secret")
	}
	if err := a.deleteSecret(ctx, "web-9", fk.Name); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(fk), &got); err != nil || len(got.Data) != 1 {
		t.Fatalf("foreign keys secret after delete = %v, %v", got.Data, err)
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
