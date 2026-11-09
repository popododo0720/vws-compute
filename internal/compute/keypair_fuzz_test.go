package compute_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// FuzzParseImportPublicKey checks ImportKeyPair's parser on any input: it
// never panics, it rejects with CodeInvalid, it accepts only what EC2
// ImportKeyPair accepts (ssh-ed25519, or ssh-rsa of 1024 to 4096 bits), and
// an accepted key's fingerprint is stable across a reparse of the input and
// of the key's own authorized_keys line. Seeds beyond these lines live in
// testdata/fuzz/FuzzParseImportPublicKey.
func FuzzParseImportPublicKey(f *testing.F) {
	for _, line := range []string{
		ed25519Line, ed25519Bare, rsa2048Line, rsa1024Line, rsa4096Line,
		rsa1023Line, rsa4097Line, ecdsaLine,
		`command="/bin/true" ` + ed25519Line,
		ed25519Line + "\n" + rsa2048Line,
		"", "ssh-ed25519 not-base64",
	} {
		f.Add(line)
	}
	f.Fuzz(func(t *testing.T, line string) {
		k, err := compute.ParseImportPublicKey(line)
		if err != nil {
			if compute.CodeOf(err) != compute.CodeInvalid {
				t.Fatalf("err = %v, want CodeInvalid", err)
			}
			return
		}
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(k.AuthorizedKey()))
		if err != nil {
			t.Fatalf("AuthorizedKey %q does not parse: %v", k.AuthorizedKey(), err)
		}
		switch k.Algorithm() {
		case compute.KeyED25519:
			if pub.Type() != ssh.KeyAlgoED25519 {
				t.Fatalf("Algorithm ED25519, key type %q", pub.Type())
			}
		case compute.KeyRSA:
			if pub.Type() != ssh.KeyAlgoRSA {
				t.Fatalf("Algorithm RSA, key type %q", pub.Type())
			}
			n := pub.(ssh.CryptoPublicKey).CryptoPublicKey().(*rsa.PublicKey).N
			if n.Sign() <= 0 {
				t.Fatalf("accepted an rsa key with modulus sign %d", n.Sign())
			}
			if bits := n.BitLen(); bits < 1024 || bits > 4096 {
				t.Fatalf("accepted a %d-bit rsa key", bits)
			}
		default:
			t.Fatalf("accepted algorithm %d", k.Algorithm())
		}
		if got, want := k.Fingerprint(), ssh.FingerprintSHA256(pub); got != want {
			t.Fatalf("Fingerprint = %q, ssh-keygen prints %q", got, want)
		}
		again, err := compute.ParseImportPublicKey(line)
		if err != nil || again != k {
			t.Fatalf("second parse = %v, %v; want the same key", again, err)
		}
		round, err := compute.ParseImportPublicKey(k.AuthorizedKey())
		if err != nil || round != k || round.Fingerprint() != k.Fingerprint() {
			t.Fatalf("parse of AuthorizedKey = %v, %v; want the same key", round, err)
		}
		if stored, err := compute.ParsePublicKey(line); err != nil || stored != k {
			t.Fatalf("ParsePublicKey = %v, %v; want the imported key", stored, err)
		}
	})
}

// TestImportKeyRoundTripsEveryEncoding generates keys of every type and size
// edge ImportKeyPair accepts and checks that each authorized_keys spelling an
// OpenSSH tool or a pasted file produces parses to that key, with the
// fingerprint ssh-keygen prints. The parser reads only the OpenSSH format.
func TestImportKeyRoundTripsEveryEncoding(t *testing.T) {
	_, edPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]any{"ed25519": edPriv.Public()}
	for _, bits := range []int{1024, 2048, 3072, 4096} {
		rk, err := rsa.GenerateKey(rand.Reader, bits)
		if err != nil {
			t.Fatalf("generate %d-bit rsa: %v", bits, err)
		}
		keys["rsa_"+strconv.Itoa(bits)] = rk.Public()
	}
	for name, cryptoKey := range keys {
		t.Run(name, func(t *testing.T) {
			pub, err := ssh.NewPublicKey(cryptoKey)
			if err != nil {
				t.Fatal(err)
			}
			marshaled := string(ssh.MarshalAuthorizedKey(pub))
			bare := strings.TrimSuffix(marshaled, "\n")
			encodings := map[string]string{
				"ssh-keygen .pub":     bare + " user@host\n",
				"marshaled":           marshaled,
				"bare":                bare,
				"crlf":                bare + " user@host\r\n",
				"comment with spaces": bare + " my laptop key",
				"indented":            "\t " + bare,
				"after comment lines": "# deploy key\n\n" + bare + "\n",
			}
			for enc, line := range encodings {
				k, err := compute.ParseImportPublicKey(line)
				if err != nil {
					t.Fatalf("%s: %v", enc, err)
				}
				if got := k.AuthorizedKey(); got != bare {
					t.Errorf("%s: AuthorizedKey = %q, want %q", enc, got, bare)
				}
				if got, want := k.Fingerprint(), ssh.FingerprintSHA256(pub); got != want {
					t.Errorf("%s: Fingerprint = %q, want %q", enc, got, want)
				}
			}
		})
	}
}
