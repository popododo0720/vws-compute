package compute_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/popododo0720/vws-compute/internal/compute"
)

func TestParsePublicKeyMatchesSSHKeygen(t *testing.T) {
	cases := []struct {
		line, fingerprint, authorized string
		alg                           compute.KeyAlgorithm
	}{
		{ed25519Line, ed25519SSHKeyFP, ed25519Bare, compute.KeyED25519},
		{rsa2048Line, rsa2048SSHKeyFP, rsa2048Line, compute.KeyRSA},
	}
	for _, c := range cases {
		k, err := compute.ParsePublicKey(c.line)
		if err != nil {
			t.Fatalf("ParsePublicKey(%.20q): %v", c.line, err)
		}
		if got := k.Fingerprint(); got != c.fingerprint {
			t.Errorf("Fingerprint = %q, want %q", got, c.fingerprint)
		}
		if got := k.AuthorizedKey(); got != c.authorized {
			t.Errorf("AuthorizedKey = %q, want %q", got, c.authorized)
		}
		if k.Algorithm() != c.alg {
			t.Errorf("Algorithm = %d, want %d", k.Algorithm(), c.alg)
		}
	}
}

func TestParsePublicKeyRejects(t *testing.T) {
	for name, line := range map[string]string{
		"rsa below 2048 bits": rsa1024Line,
		"ecdsa":               ecdsaLine,
		"options":             `command="/bin/true" ` + ed25519Line,
		"two lines":           ed25519Line + "\n" + rsa2048Line,
		"empty":               "",
		"garbage":             "ssh-ed25519 not-base64",
	} {
		if _, err := compute.ParsePublicKey(line); compute.CodeOf(err) != compute.CodeInvalid {
			t.Errorf("%s: err = %v, want CodeInvalid", name, err)
		}
	}
}

func TestCreateKeyPairSecretMatchesStoredPublicKey(t *testing.T) {
	in, err := compute.NewCreateKeyPair("deploy")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := compute.Plan(newView(), compute.Env{Now: t0, Rand: rand.Reader}, in)
	if err != nil {
		t.Fatal(err)
	}
	spec := ch.Spec.(compute.KeyPairSpec)
	if ch.Secret == nil {
		t.Fatal("CreateKeyPair plan has no private key")
	}
	raw, err := ssh.ParseRawPrivateKey(ch.Secret.OpenSSH())
	if err != nil {
		t.Fatalf("private key is not OpenSSH PEM: %v", err)
	}
	priv, ok := raw.(*ed25519.PrivateKey)
	if !ok {
		t.Fatalf("private key type = %T, want *ed25519.PrivateKey", raw)
	}
	pub, err := ssh.NewPublicKey(priv.Public())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ssh.FingerprintSHA256(pub), spec.PublicKey.Fingerprint(); got != want {
		t.Fatalf("private key fingerprint %q, stored public key %q", got, want)
	}
	if got := fmt.Sprint(*ch.Secret); got != "PrivateKey(REDACTED)" {
		t.Fatalf("fmt.Sprint(private key) = %q", got)
	}
}

var t0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
