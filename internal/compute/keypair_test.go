package compute_test

import (
	"fmt"
	"testing"
	"time"

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
		"ecdsa":     ecdsaLine,
		"options":   `command="/bin/true" ` + ed25519Line,
		"two lines": ed25519Line + "\n" + rsa2048Line,
		"empty":     "",
		"garbage":   "ssh-ed25519 not-base64",
	} {
		if _, err := compute.ParsePublicKey(line); compute.CodeOf(err) != compute.CodeInvalid {
			t.Errorf("%s: err = %v, want CodeInvalid", name, err)
		}
	}
}

func TestParseImportPublicKeyRSABitRange(t *testing.T) {
	for _, c := range []struct {
		bits  int
		line  string
		valid bool
	}{
		{1023, rsa1023Line, false},
		{1024, rsa1024Line, true},
		{4096, rsa4096Line, true},
		{4097, rsa4097Line, false},
	} {
		t.Run(fmt.Sprintf("rsa_%d", c.bits), func(t *testing.T) {
			_, err := compute.ParseImportPublicKey(c.line)
			if c.valid {
				if err != nil {
					t.Fatalf("ParseImportPublicKey(%d-bit rsa): %v", c.bits, err)
				}
				return
			}
			if compute.CodeOf(err) != compute.CodeInvalid {
				t.Fatalf("ParseImportPublicKey(%d-bit rsa) err = %v, want CodeInvalid", c.bits, err)
			}
		})
	}
}

func TestParsePublicKeyReadsKeysOutsideTheImportRange(t *testing.T) {
	for _, line := range []string{rsa1023Line, rsa4097Line} {
		if _, err := compute.ParsePublicKey(line); err != nil {
			t.Fatalf("ParsePublicKey of a stored key: %v", err)
		}
	}
}

var t0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
