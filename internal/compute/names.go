package compute

import (
	"net/url"
	"strings"
)

// RequestID is the caller-chosen idempotency key every mutation carries:
// 1-64 characters from [A-Za-z0-9_-]. The zero value is invalid and
// controller.Submit rejects it, so no mutation runs without a key.
type RequestID struct{ s string }

// ParseRequestID validates s. It fails with CodeInvalid, including for the
// empty string.
func ParseRequestID(s string) (RequestID, error) {
	if len(s) < 1 || len(s) > 64 || strings.IndexFunc(s, notRequestIDRune) >= 0 {
		return RequestID{}, Errorf(CodeInvalid, "invalid request id %q: want 1-64 characters from [A-Za-z0-9_-]", s)
	}
	return RequestID{s}, nil
}

func notRequestIDRune(c rune) bool {
	return (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-'
}

func (r RequestID) String() string { return r.s }
func (r RequestID) IsZero() bool   { return r.s == "" }

// KeyName is a key pair's caller-facing name: 1-255 printable ASCII
// characters. At most one live key pair holds a given name.
type KeyName struct{ s string }

// ParseKeyName validates s. It fails with CodeInvalid.
func ParseKeyName(s string) (KeyName, error) {
	if len(s) < 1 || len(s) > 255 || strings.IndexFunc(s, func(c rune) bool { return c < 0x20 || c > 0x7e }) >= 0 {
		return KeyName{}, Errorf(CodeInvalid, "invalid key name %q: want 1-255 printable ASCII characters", s)
	}
	return KeyName{s}, nil
}

func (n KeyName) String() string { return n.s }

// IsZero reports whether n is unset. RunInstance treats the zero KeyName as
// "launch without an authorized key".
func (n KeyName) IsZero() bool { return n.s == "" }

// ImageName is an image's caller-facing label: 3-128 characters from
// [A-Za-z0-9()[]./_' @-]. Names are not unique.
type ImageName struct{ s string }

// ParseImageName validates s. It fails with CodeInvalid.
func ParseImageName(s string) (ImageName, error) {
	const extra = "()[]./_' @-"
	bad := func(c rune) bool {
		return (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune(extra, c)
	}
	if len(s) < 3 || len(s) > 128 || strings.IndexFunc(s, bad) >= 0 {
		return ImageName{}, Errorf(CodeInvalid, "invalid image name %q", s)
	}
	return ImageName{s}, nil
}

func (n ImageName) String() string { return n.s }

// ImageURL is the absolute https location of a disk image to register.
type ImageURL struct{ s string }

// ParseImageURL accepts absolute https URLs with a host and no credentials.
// It fails with CodeInvalid.
func ParseImageURL(s string) (ImageURL, error) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return ImageURL{}, Errorf(CodeInvalid, "invalid image url %q: want an https URL", s)
	}
	return ImageURL{s}, nil
}

func (u ImageURL) String() string { return u.s }

// InstanceType is one entry of the static catalog. ParseInstanceType is the
// only constructor, so every non-zero InstanceType is a catalog entry.
type InstanceType struct {
	name      string
	vcpus     int
	memoryMiB int
}

var instanceTypes = [...]InstanceType{
	{name: "vws.small", vcpus: 1, memoryMiB: 1024},
	{name: "vws.medium", vcpus: 2, memoryMiB: 4096},
}

// ParseInstanceType looks s up in the catalog. It fails with CodeInvalid.
func ParseInstanceType(s string) (InstanceType, error) {
	for _, t := range instanceTypes {
		if t.name == s {
			return t, nil
		}
	}
	return InstanceType{}, Errorf(CodeInvalid, "unknown instance type %q", s)
}

func (t InstanceType) Name() string   { return t.name }
func (t InstanceType) VCPUs() int     { return t.vcpus }
func (t InstanceType) MemoryMiB() int { return t.memoryMiB }
