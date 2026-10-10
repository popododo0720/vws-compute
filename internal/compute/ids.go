package compute

import (
	"encoding/hex"
	"io"
	"strings"
)

// InstanceID identifies an instance: "i-" followed by 17 lowercase hex
// digits. A non-zero InstanceID was validated, because the only
// constructors are ParseInstanceID and the planner.
type InstanceID struct{ s string }

// ImageID is the image resource path segment. It uses the
// vws.common.v1.ResourceId grammar: a lowercase letter, then up to 62 more
// characters from [a-z0-9-], not ending in a hyphen. Capture still mints
// "img-" plus 17 lowercase hex digits, which is one of those ids.
type ImageID struct{ s string }

// KeyPairID identifies one incarnation of a key pair: "key-" followed by 17
// lowercase hex digits. A deleted key name can be reused, and the new key
// pair gets a new KeyPairID, so an operation never points at the wrong
// incarnation.
type KeyPairID struct{ s string }

// OperationID identifies an operation: "op-" followed by 20 lowercase hex
// digits.
type OperationID struct{ s string }

const (
	resourceIDDigits  = 17
	operationIDDigits = 20
)

// ParseInstanceID validates s. It fails with CodeInvalid.
func ParseInstanceID(s string) (InstanceID, error) {
	if !validID(s, "i-", resourceIDDigits) {
		return InstanceID{}, Errorf(CodeInvalid, "invalid instance id %q", s)
	}
	return InstanceID{s}, nil
}

// ParseImageID validates s as a ResourceId. It fails with CodeInvalid.
func ParseImageID(s string) (ImageID, error) {
	if !validResourceID(s) {
		return ImageID{}, Errorf(CodeInvalid, "invalid image id %q", s)
	}
	return ImageID{s}, nil
}

// ParseKeyPairID validates s. It fails with CodeInvalid.
func ParseKeyPairID(s string) (KeyPairID, error) {
	if !validID(s, "key-", resourceIDDigits) {
		return KeyPairID{}, Errorf(CodeInvalid, "invalid key pair id %q", s)
	}
	return KeyPairID{s}, nil
}

// ParseOperationID validates s. It fails with CodeInvalid.
func ParseOperationID(s string) (OperationID, error) {
	if !validID(s, "op-", operationIDDigits) {
		return OperationID{}, Errorf(CodeInvalid, "invalid operation id %q", s)
	}
	return OperationID{s}, nil
}

func (id InstanceID) String() string  { return id.s }
func (id ImageID) String() string     { return id.s }
func (id KeyPairID) String() string   { return id.s }
func (id OperationID) String() string { return id.s }

func (id InstanceID) IsZero() bool  { return id.s == "" }
func (id ImageID) IsZero() bool     { return id.s == "" }
func (id KeyPairID) IsZero() bool   { return id.s == "" }
func (id OperationID) IsZero() bool { return id.s == "" }

// Ref returns the generic pointer to this instance.
func (id InstanceID) Ref() Ref { return Ref{Kind: KindInstance, ID: id.s} }

// Ref returns the generic pointer to this image.
func (id ImageID) Ref() Ref { return Ref{Kind: KindImage, ID: id.s} }

// Ref returns the generic pointer to this key pair.
func (id KeyPairID) Ref() Ref { return Ref{Kind: KindKeyPair, ID: id.s} }

// validResourceID reports whether s matches vws.common.v1.ResourceId.
func validResourceID(s string) bool {
	if len(s) < 1 || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
		case i > 0 && c >= '0' && c <= '9':
		case i > 0 && i < len(s)-1 && c == '-':
		default:
			return false
		}
	}
	return true
}

func validID(s, prefix string, digits int) bool {
	rest, ok := strings.CutPrefix(s, prefix)
	if !ok || len(rest) != digits {
		return false
	}
	for _, c := range rest {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// mintID draws an ID from r. Only planners and NewOperation mint IDs, so an
// ID exists in storage only if the plan that minted it was committed.
func mintID(r io.Reader, prefix string, digits int) (string, error) {
	if r == nil {
		return "", Errorf(CodeInternal, "no random source")
	}
	buf := make([]byte, (digits+1)/2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", Errorf(CodeInternal, "mint %s id: %v", prefix, err)
	}
	return prefix + hex.EncodeToString(buf)[:digits], nil
}

func newInstanceID(r io.Reader) (InstanceID, error) {
	s, err := mintID(r, "i-", resourceIDDigits)
	return InstanceID{s}, err
}

func newImageID(r io.Reader) (ImageID, error) {
	s, err := mintID(r, "img-", resourceIDDigits)
	return ImageID{s}, err
}

func newKeyPairID(r io.Reader) (KeyPairID, error) {
	s, err := mintID(r, "key-", resourceIDDigits)
	return KeyPairID{s}, err
}

func newOperationID(r io.Reader) (OperationID, error) {
	s, err := mintID(r, "op-", operationIDDigits)
	return OperationID{s}, err
}
