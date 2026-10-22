package compute

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/pem"
	"io"
	"time"

	"golang.org/x/crypto/ssh"
)

// KeyAlgorithm is a public key algorithm.
type KeyAlgorithm uint8

const (
	KeyED25519 KeyAlgorithm = iota + 1
	KeyRSA
)

const (
	minRSABits = 1024
	maxRSABits = 4096
)

// PublicKey is a parsed SSH public key: ssh-ed25519, or ssh-rsa of 1024 to
// 4096 bits. It is comparable, and two PublicKeys are equal iff their SSH
// wire encodings are equal.
type PublicKey struct {
	alg  KeyAlgorithm
	wire string
}

// ParsePublicKey parses exactly one authorized_keys line without options.
// It fails with CodeInvalid for anything else.
func ParsePublicKey(line string) (PublicKey, error) {
	pub, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		return PublicKey{}, Errorf(CodeInvalid, "invalid public key: %v", err)
	}
	if len(options) > 0 || len(bytes.TrimSpace(rest)) > 0 {
		return PublicKey{}, Errorf(CodeInvalid, "invalid public key: want exactly one authorized_keys line without options")
	}
	var alg KeyAlgorithm
	switch pub.Type() {
	case ssh.KeyAlgoED25519:
		alg = KeyED25519
	case ssh.KeyAlgoRSA:
		ck, ok := pub.(ssh.CryptoPublicKey)
		if !ok {
			return PublicKey{}, Errorf(CodeInvalid, "invalid public key: unreadable rsa key")
		}
		rk, ok := ck.CryptoPublicKey().(*rsa.PublicKey)
		if !ok || rk.N.BitLen() < minRSABits || rk.N.BitLen() > maxRSABits {
			return PublicKey{}, Errorf(CodeInvalid, "invalid public key: rsa keys need %d to %d bits", minRSABits, maxRSABits)
		}
		alg = KeyRSA
	default:
		return PublicKey{}, Errorf(CodeInvalid, "invalid public key: type %q is not ssh-ed25519 or ssh-rsa", pub.Type())
	}
	return PublicKey{alg: alg, wire: string(pub.Marshal())}, nil
}

func (k PublicKey) Algorithm() KeyAlgorithm { return k.alg }

// Fingerprint returns "SHA256:" and the unpadded base64 SHA-256 of the wire
// encoding, as ssh-keygen -l prints it.
func (k PublicKey) Fingerprint() string {
	sum := sha256.Sum256([]byte(k.wire))
	return "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:])
}

// AuthorizedKey returns the key as one authorized_keys line with no comment.
func (k PublicKey) AuthorizedKey() string {
	typ := ssh.KeyAlgoED25519
	if k.alg == KeyRSA {
		typ = ssh.KeyAlgoRSA
	}
	return typ + " " + base64.StdEncoding.EncodeToString([]byte(k.wire))
}

// PrivateKey is a PEM-encoded OpenSSH private key. It exists only on the
// response path of the CreateKeyPair call that inserted the key pair. No
// store method accepts one.
type PrivateKey struct{ openSSH []byte }

// OpenSSH returns a copy of the PEM-encoded OpenSSH private key.
func (k PrivateKey) OpenSSH() []byte { return append([]byte(nil), k.openSSH...) }

// String redacts, so a stray %v in a log line cannot leak the key.
func (PrivateKey) String() string { return "PrivateKey(REDACTED)" }

// GoString redacts, so %#v cannot leak the key.
func (PrivateKey) GoString() string { return "compute.PrivateKey{}" }

// generateEd25519 draws a new ed25519 key pair from r.
func generateEd25519(r io.Reader) (PublicKey, PrivateKey, error) {
	if r == nil {
		return PublicKey{}, PrivateKey{}, Errorf(CodeInternal, "no random source")
	}
	pub, priv, err := ed25519.GenerateKey(r)
	if err != nil {
		return PublicKey{}, PrivateKey{}, Errorf(CodeInternal, "generate ed25519 key: %v", err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return PublicKey{}, PrivateKey{}, Errorf(CodeInternal, "encode ed25519 public key: %v", err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return PublicKey{}, PrivateKey{}, Errorf(CodeInternal, "encode ed25519 private key: %v", err)
	}
	return PublicKey{alg: KeyED25519, wire: string(sshPub.Marshal())}, PrivateKey{openSSH: pem.EncodeToMemory(block)}, nil
}

// KeyPairSpec is the desired state of one key pair incarnation.
type KeyPairSpec struct {
	ID         KeyPairID
	Generation int64
	CreatedAt  time.Time
	ChangedAt  time.Time
	Name       KeyName
	PublicKey  PublicKey
	// Deleted is absorbing. Deleting releases Name for a new incarnation;
	// the row stays as a tombstone.
	Deleted bool
}

func (s KeyPairSpec) Ref() Ref   { return s.ID.Ref() }
func (s KeyPairSpec) Gen() int64 { return s.Generation }
func (KeyPairSpec) isSpec()      {}

// KeyPair is the read model. It has no stored status.
type KeyPair struct {
	Spec KeyPairSpec
	Meta Metadata
}

// Observed is derived, not stored. A key pair has no agent-side state, so
// every generation is converged the moment it is committed, and key pair
// operations settle inside CommitDesired through the same Settle call as
// everything else.
func (k KeyPair) Observed() Observed { return Observed{Generation: k.Spec.Generation} }
