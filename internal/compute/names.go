package compute

import (
	"net/url"
	"path"
	"strings"
	"unicode/utf8"
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

// ImagePath is an absolute, clean POSIX path of a disk image file on the
// agent's host. This process parses it and does not open it.
type ImagePath struct{ s string }

const maxImagePathBytes = 4096

// ParseImagePath accepts an absolute path other than "/" that path.Clean
// leaves unchanged, of at most 4096 bytes, with no ASCII control characters.
// It fails with CodeInvalid.
func ParseImagePath(s string) (ImagePath, error) {
	if len(s) == 0 || len(s) > maxImagePathBytes || !utf8.ValidString(s) || strings.IndexFunc(s, isASCIIControl) >= 0 ||
		!path.IsAbs(s) || path.Clean(s) != s || s == "/" {
		return ImagePath{}, Errorf(CodeInvalid, "invalid image path %q: want an absolute, clean file path", s)
	}
	return ImagePath{s}, nil
}

func (p ImagePath) String() string { return p.s }

// VolumeRef is projects/{project}/zones/{zone}/volumes/{volume}. Every
// segment is a ResourceId. The string is stored and returned whole.
type VolumeRef struct{ s string }

// SnapshotRef is projects/{project}/regions/{region}/snapshots/{snapshot}.
type SnapshotRef struct{ s string }

// ParseVolumeRef validates a volume resource name. It fails with CodeInvalid.
func ParseVolumeRef(s string) (VolumeRef, error) {
	if err := parseResourceName(s, "zones", "volumes"); err != nil {
		return VolumeRef{}, Errorf(CodeInvalid, "invalid volume %q", s)
	}
	return VolumeRef{s}, nil
}

// ParseSnapshotRef validates a snapshot resource name. It fails with CodeInvalid.
func ParseSnapshotRef(s string) (SnapshotRef, error) {
	if err := parseResourceName(s, "regions", "snapshots"); err != nil {
		return SnapshotRef{}, Errorf(CodeInvalid, "invalid snapshot %q", s)
	}
	return SnapshotRef{s}, nil
}

func (r VolumeRef) String() string   { return r.s }
func (r SnapshotRef) String() string { return r.s }

func parseResourceName(s, scopeKind, collection string) error {
	parts := strings.Split(s, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != scopeKind || parts[4] != collection {
		return Errorf(CodeInvalid, "bad name")
	}
	for _, i := range []int{1, 3, 5} {
		if !validResourceID(parts[i]) {
			return Errorf(CodeInvalid, "bad name")
		}
	}
	return nil
}

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

// ParseMachineType accepts a catalog entry, and otherwise any name in the
// proto's machine_type grammar, with no vCPU or memory figures. It exists
// only to decode instances stored before unknown types were rejected (#45),
// so they still load, list, and delete. New requests use ParseInstanceType.
func ParseMachineType(s string) (InstanceType, error) {
	if t, err := ParseInstanceType(s); err == nil {
		return t, nil
	}
	if len(s) < 1 || len(s) > 63 || s[0] < 'a' || s[0] > 'z' {
		return InstanceType{}, Errorf(CodeInvalid, "invalid machine type %q", s)
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || (c == '.' || c == '-') && i < len(s)-1
		if !ok {
			return InstanceType{}, Errorf(CodeInvalid, "invalid machine type %q", s)
		}
	}
	return InstanceType{name: s}, nil
}

func (t InstanceType) Name() string   { return t.name }
func (t InstanceType) VCPUs() int     { return t.vcpus }
func (t InstanceType) MemoryMiB() int { return t.memoryMiB }
