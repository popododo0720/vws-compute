package kubevirt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

// SecretKeyUserData is the key KubeVirt reads from the Secret named by
// cloudInitNoCloud.secretRef (Go field UserDataSecretRef).
const SecretKeyUserData = "userdata"

// cloudInitSecret names the Secret that holds a machine's cloud-init user
// data, next to its VirtualMachine.
func cloudInitSecret(machine string) string { return machine + "-cloudinit" }

// sshKeysSecret names the Secret that holds a machine's public keys, one
// per data entry. KubeVirt reads every entry of it for the VM's
// accessCredentials and writes the keys into the NoCloud meta-data as
// public-keys, which is how EC2 delivers a key pair (instance meta-data, not
// user data). It is separate from the user-data Secret because KubeVirt
// treats every value in an access-credential Secret as a key.
func sshKeysSecret(machine string) string { return machine + "-ssh-keys" }

// keysPlaceholder is the first entry of the keys Secret. KubeVirt numbers
// the NoCloud public-keys "0", "1", ... in file-name order, and CirrOS
// drops entry "0" (its JSON-to-files helper keeps an array's length there),
// so without a placeholder a single key never reaches a CirrOS guest. It is
// a comment line: cloud-init writes it to authorized_keys, where sshd
// ignores it. It sorts before the key-NNN entries.
const (
	keysPlaceholderName  = "00-placeholder"
	keysPlaceholderValue = "# vws: public-keys entry 0 is a placeholder; CirrOS skips entry 0"
)

// keysData is the data of the keys Secret: the placeholder, then entry
// key-NNN per key, in order. KubeVirt numbers the keys in file-name order,
// so the zero padding keeps the user's order for up to 1000 keys.
func keysData(keys []string) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}
	data := make(map[string][]byte, len(keys)+1)
	data[keysPlaceholderName] = []byte(keysPlaceholderValue)
	for i, k := range keys {
		data[fmt.Sprintf("key-%03d", i)] = []byte(k)
	}
	return data
}

// renderUserData is the NoCloud user-data for keys and the user's data.
//
//   - Neither: "" (no cloud-init disk).
//   - Only keys: a #cloud-config with ssh_authorized_keys. The keys also go
//     in the meta-data (sshKeysSecret); this keeps the path guests already
//     used, and the disk needs some user data to exist.
//   - User data, with or without keys: the user's data, unchanged, as on
//     EC2. The keys reach the guest only through the meta-data public-keys,
//     which cloud-init and CirrOS both read. Earlier this was a MIME
//     multipart of the user's data and a key cloud-config: CirrOS runs only
//     a user-data that starts with "#!", so it ran neither the script nor
//     the keys (vws-compute#83).
func renderUserData(keys []string, user string) (string, error) {
	if user != "" {
		return user, nil
	}
	if len(keys) == 0 {
		return "", nil
	}
	quoted, err := json.Marshal(keys) // a JSON array is a YAML flow sequence
	if err != nil {
		return "", hypervisor.Permanent(err)
	}
	return "#cloud-config\nssh_authorized_keys: " + string(quoted) + "\n", nil
}

func (a *Agent) ensureCloudInitSecret(ctx context.Context, machine, data string) error {
	return a.ensureSecret(ctx, machine, cloudInitSecret(machine), map[string][]byte{SecretKeyUserData: []byte(data)})
}

func (a *Agent) deleteCloudInitSecret(ctx context.Context, machine string) error {
	return a.deleteSecret(ctx, machine, cloudInitSecret(machine))
}

// ensureSecret creates the machine's managed Secret name with data, or
// converges one this agent made earlier. A Secret of that name it did not
// make is never touched.
func (a *Agent) ensureSecret(ctx context.Context, machine, name string, data map[string][]byte) error {
	want := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: a.cfg.Namespace,
			Labels: map[string]string{LabelManaged: "true", LabelInstance: machine},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	err := a.c.Create(ctx, want)
	if !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("machine %s: create secret %s: %w", machine, name, err)
		}
		return nil
	}
	var have corev1.Secret
	if err := a.c.Get(ctx, client.ObjectKeyFromObject(want), &have); err != nil {
		return fmt.Errorf("machine %s: get secret %s: %w", machine, name, err)
	}
	if have.Labels[LabelManaged] != "true" {
		return hypervisor.Permanent(fmt.Errorf("machine %s: secret %s exists and is not managed here", machine, name))
	}
	if sameData(have.Data, data) {
		return nil
	}
	have.Data = data
	if err := a.c.Update(ctx, &have); err != nil {
		return fmt.Errorf("machine %s: update secret %s: %w", machine, name, err)
	}
	return nil
}

func sameData(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}
	return true
}

// deleteSecret deletes the machine's Secret name if this agent made it.
func (a *Agent) deleteSecret(ctx context.Context, machine, name string) error {
	var s corev1.Secret
	err := a.c.Get(ctx, client.ObjectKey{Namespace: a.cfg.Namespace, Name: name}, &s)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("machine %s: get secret %s: %w", machine, name, err)
	}
	if s.Labels[LabelManaged] != "true" {
		return nil // not ours; leave it
	}
	if err := client.IgnoreNotFound(a.c.Delete(ctx, &s)); err != nil {
		return fmt.Errorf("machine %s: delete secret %s: %w", machine, name, err)
	}
	return nil
}
