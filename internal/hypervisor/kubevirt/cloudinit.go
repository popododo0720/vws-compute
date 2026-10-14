package kubevirt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// SecretKeyUserData is the key KubeVirt reads from a cloudInitNoCloud
// userDataSecretRef Secret.
const SecretKeyUserData = "userdata"

// cloudInitSecret names the Secret that holds a machine's cloud-init user
// data, next to its VirtualMachine.
func cloudInitSecret(machine string) string { return machine + "-cloudinit" }

// keyMergeType makes cloud-init append the key part's lists to the
// cloud-config the user's part already set, so neither side's
// ssh_authorized_keys replaces the other's.
const keyMergeType = "list(append)+dict(recurse_array)+str()"

// renderUserData is the NoCloud user-data for keys and the user's data.
//
//   - Neither: "" (no cloud-init disk).
//   - Only keys: a #cloud-config with ssh_authorized_keys.
//   - Only user data: the user data, byte for byte.
//   - Both: MIME multipart. The user's data is the first part, base64 so
//     cloud-init gets its exact bytes, with the content type cloud-init
//     would have picked for it alone. The keys are a second #cloud-config
//     part whose Merge-Type appends, so a user cloud-config that sets its own
//     ssh_authorized_keys keeps them.
//
// User data that is already MIME cannot be wrapped as one part, so with keys
// it is a permanent error.
func renderUserData(keys []string, user string) (string, error) {
	if len(keys) == 0 {
		return user, nil
	}
	quoted, err := json.Marshal(keys) // a JSON array is a YAML flow sequence
	if err != nil {
		return "", hypervisor.Permanent(err)
	}
	keyConfig := "#cloud-config\nssh_authorized_keys: " + string(quoted) + "\n"
	if user == "" {
		return keyConfig, nil
	}
	if isMIME(user) {
		return "", hypervisor.Permanent(errors.New("user data that is already MIME multipart cannot be combined with a key pair; put the key in the user data instead"))
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	sum := sha256.Sum256([]byte(user + "\x00" + keyConfig))
	// A boundary from the content keeps the output stable across converges.
	if err := w.SetBoundary("vws-" + hex.EncodeToString(sum[:12])); err != nil {
		return "", hypervisor.Permanent(err)
	}
	part, err := w.CreatePart(textproto.MIMEHeader{
		"Content-Type":              {userContentType(user) + `; charset="utf-8"`},
		"Content-Transfer-Encoding": {"base64"},
		"Content-Disposition":       {`attachment; filename="user-data"`},
	})
	if err != nil {
		return "", hypervisor.Permanent(err)
	}
	enc := base64.StdEncoding.EncodeToString([]byte(user))
	for len(enc) > 76 {
		_, _ = part.Write([]byte(enc[:76] + "\r\n")) //nolint:errcheck // writes to a bytes.Buffer do not fail
		enc = enc[76:]
	}
	_, _ = part.Write([]byte(enc + "\r\n")) //nolint:errcheck // as above
	part, err = w.CreatePart(textproto.MIMEHeader{
		"Content-Type":        {`text/cloud-config; charset="utf-8"`},
		"Merge-Type":          {keyMergeType},
		"Content-Disposition": {`attachment; filename="vws-ssh-keys"`},
	})
	if err != nil {
		return "", hypervisor.Permanent(err)
	}
	_, _ = part.Write([]byte(keyConfig)) //nolint:errcheck // as above
	if err := w.Close(); err != nil {
		return "", hypervisor.Permanent(err)
	}
	return "Content-Type: multipart/mixed; boundary=\"" + w.Boundary() + "\"\r\nMIME-Version: 1.0\r\n\r\n" + body.String(), nil
}

// userContentType is the type cloud-init gives user data by its first line
// (cloud-init's user-data format table). Anything else is plain text, which
// cloud-init logs and ignores, as it would without the key part.
func userContentType(user string) string {
	for _, p := range []struct{ prefix, typ string }{
		{"#cloud-config-archive", "text/cloud-config-archive"},
		{"#cloud-config-jsonp", "text/cloud-config-jsonp"},
		{"#cloud-config", "text/cloud-config"},
		{"#cloud-boothook", "text/cloud-boothook"},
		{"#include-once", "text/x-include-once-url"},
		{"#include", "text/x-include-url"},
		{"#part-handler", "text/part-handler"},
		{"#upstart-job", "text/upstart-job"},
		{"## template: jinja", "text/jinja2"},
		{"#!", "text/x-shellscript"},
	} {
		if strings.HasPrefix(user, p.prefix) {
			return p.typ
		}
	}
	return "text/plain"
}

func isMIME(user string) bool {
	head := strings.ToLower(user[:min(len(user), 64)])
	return strings.HasPrefix(head, "content-type:") || strings.HasPrefix(head, "mime-version:")
}

// ensureCloudInitSecret makes the machine's user-data Secret hold data. It
// is written before the VirtualMachine, so the VM never starts without it,
// and a retried create finds it and converges it.
func (a *Agent) ensureCloudInitSecret(ctx context.Context, machine, data string) error {
	want := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: cloudInitSecret(machine), Namespace: a.cfg.Namespace,
			Labels: map[string]string{LabelManaged: "true", LabelInstance: machine},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{SecretKeyUserData: []byte(data)},
	}
	err := a.c.Create(ctx, want)
	if !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("machine %s: create cloud-init secret: %w", machine, err)
		}
		return nil
	}
	var have corev1.Secret
	if err := a.c.Get(ctx, client.ObjectKeyFromObject(want), &have); err != nil {
		return fmt.Errorf("machine %s: get cloud-init secret: %w", machine, err)
	}
	if have.Labels[LabelManaged] != "true" {
		return hypervisor.Permanent(fmt.Errorf("machine %s: secret %s exists and is not managed here", machine, want.Name))
	}
	if string(have.Data[SecretKeyUserData]) == data {
		return nil
	}
	have.Data = want.Data
	if err := a.c.Update(ctx, &have); err != nil {
		return fmt.Errorf("machine %s: update cloud-init secret: %w", machine, err)
	}
	return nil
}

// deleteCloudInitSecret removes the machine's user-data Secret. A Secret of
// that name without our label is left alone. One that is gone, or never
// existed (a machine without user data, or one created
// before Secrets were used), is success.
func (a *Agent) deleteCloudInitSecret(ctx context.Context, machine string) error {
	var s corev1.Secret
	err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: cloudInitSecret(machine)}, &s)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("machine %s: get cloud-init secret: %w", machine, err)
	}
	if s.Labels[LabelManaged] != "true" {
		return nil // not ours; leave it
	}
	if err := client.IgnoreNotFound(a.c.Delete(ctx, &s)); err != nil {
		return fmt.Errorf("machine %s: delete cloud-init secret: %w", machine, err)
	}
	return nil
}
