package compute

import (
	"maps"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxAnnotations, and the key and value limits, are the vws.common.v1
	// resource envelope rules. Lengths count Unicode characters, as EC2
	// counts tag lengths. Reserved keys do not count toward MaxAnnotations.
	MaxAnnotations          = 50
	maxAnnotationKeyChars   = 128
	maxAnnotationValueChars = 256
	// ReservedAnnotationPrefix marks a key only the server sets, as AWS
	// reserves the aws: tag prefix.
	ReservedAnnotationPrefix = "aws:"
)

// Annotations is a validated annotation set: user metadata stored and
// returned verbatim that does not fit the label grammar, such as EC2 tags.
// Keys are 1-128 and values 0-256 characters of UTF-8 without control
// characters, with at most MaxAnnotations keys outside
// ReservedAnnotationPrefix.
//
// Annotations is immutable, like Labels. The zero value is the empty set,
// and every constructor returns it for empty input.
type Annotations struct{ m map[string]string }

// ParseAnnotations copies and validates m. It fails with CodeInvalid and
// the EC2 InvalidParameterValue sentence. It accepts reserved keys, because
// a stored set may hold them. User input passes through
// NewCreateInstance or MetadataPatch.ReplaceAnnotations, which refuse a
// reserved key the user did not keep as stored.
func ParseAnnotations(m map[string]string) (Annotations, error) {
	user := 0
	for k, v := range m {
		if !validAnnotationText(k, 1, maxAnnotationKeyChars) {
			return Annotations{}, invalidTag("tagKey", k)
		}
		if !validAnnotationText(v, 0, maxAnnotationValueChars) {
			return Annotations{}, invalidTag("tagValue", v)
		}
		if !reservedAnnotation(k) {
			user++
		}
	}
	if user > MaxAnnotations {
		return Annotations{}, Errorf(CodeInvalid, "annotations exceed %d entries", MaxAnnotations)
	}
	if len(m) == 0 {
		return Annotations{}, nil
	}
	return Annotations{m: maps.Clone(m)}, nil
}

// Len is the number of entries.
func (a Annotations) Len() int { return len(a.m) }

// Map returns a copy the caller owns, or nil when a is empty.
func (a Annotations) Map() map[string]string { return maps.Clone(a.m) }

// Equal compares contents.
func (a Annotations) Equal(o Annotations) bool { return maps.Equal(a.m, o.m) }

// userOnly fails with CodeInvalid when a holds a reserved key.
func (a Annotations) userOnly() error {
	for k := range a.m {
		if reservedAnnotation(k) {
			return reservedTag(k)
		}
	}
	return nil
}

// over returns a with every reserved entry of stored kept. A reserved key
// in a must carry its stored value, so a user can resend one but never
// set, change, or delete it.
func (a Annotations) over(stored Annotations) (Annotations, error) {
	m := map[string]string{}
	for k, v := range a.m {
		if reservedAnnotation(k) {
			if sv, ok := stored.m[k]; !ok || sv != v {
				return Annotations{}, reservedTag(k)
			}
		}
		m[k] = v
	}
	for k, v := range stored.m {
		if reservedAnnotation(k) {
			m[k] = v
		}
	}
	if len(m) == 0 {
		return Annotations{}, nil
	}
	return Annotations{m: m}, nil
}

func reservedAnnotation(k string) bool { return strings.HasPrefix(k, ReservedAnnotationPrefix) }

func validAnnotationText(s string, minChars, maxChars int) bool {
	if !utf8.ValidString(s) {
		return false
	}
	n := utf8.RuneCountInString(s)
	return n >= minChars && n <= maxChars && strings.IndexFunc(s, unicode.IsControl) < 0
}

func invalidTag(param, v string) error {
	return Errorf(CodeInvalid, "Value (%s) for parameter %s is invalid.", escape(v), param)
}

func reservedTag(k string) error {
	return Errorf(
		CodeInvalid,
		"Value (%s) for parameter tagKey is invalid. Tag keys starting with '%s' are reserved.",
		escape(k),
		ReservedAnnotationPrefix,
	)
}

func escape(s string) string {
	q := strconv.QuoteToGraphic(s)
	return q[1 : len(q)-1]
}
