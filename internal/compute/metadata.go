package compute

import (
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// MaxLabels, and the key and value limits, are the vws.common.v1
	// resource envelope rules.
	MaxLabels        = 64
	maxLabelKeyLen   = 63
	maxLabelValueLen = 63
	// MaxDescriptionBytes is this service's limit. The proto sets none.
	MaxDescriptionBytes = 2048
)

// Labels is a validated label set: at most MaxLabels entries, keys matching
// [a-z][a-z0-9_-]{0,62}, values of 0-63 bytes of UTF-8 without ASCII control
// characters.
//
// Labels is immutable. No method returns or mutates the map, so copies of a
// Labels value may share it, and stores need not clone it. The zero value
// is the empty set, and every constructor returns the zero value for empty
// input, so unset and empty compare equal.
type Labels struct{ m map[string]string }

// ParseLabels copies and validates m. It fails with CodeInvalid.
func ParseLabels(m map[string]string) (Labels, error) {
	if len(m) > MaxLabels {
		return Labels{}, Errorf(CodeInvalid, "labels exceed %d entries", MaxLabels)
	}
	for k, v := range m {
		if !validLabelKey(k) {
			return Labels{}, Errorf(CodeInvalid, "invalid label key %q", k)
		}
		if !validLabelValue(v) {
			return Labels{}, Errorf(CodeInvalid, "invalid value for label %q", k)
		}
	}
	return labelsOf(maps.Clone(m)), nil
}

// labelsOf wraps m, which must already be valid and owned by the caller.
// It returns the zero value for an empty m.
func labelsOf(m map[string]string) Labels {
	if len(m) == 0 {
		return Labels{}
	}
	return Labels{m: m}
}

// Len is the number of entries.
func (l Labels) Len() int { return len(l.m) }

// Get returns the value for key.
func (l Labels) Get(key string) (string, bool) {
	v, ok := l.m[key]
	return v, ok
}

// Map returns a copy the caller owns, or nil when l is empty.
func (l Labels) Map() map[string]string { return maps.Clone(l.m) }

// Equal compares contents, so insertion order never matters.
func (l Labels) Equal(o Labels) bool { return maps.Equal(l.m, o.m) }

func (l Labels) keys() []string { return slices.Sorted(maps.Keys(l.m)) }

func validLabelKey(k string) bool {
	if len(k) < 1 || len(k) > maxLabelKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		switch {
		case c >= 'a' && c <= 'z':
		case i > 0 && (c >= '0' && c <= '9' || c == '_' || c == '-'):
		default:
			return false
		}
	}
	return true
}

func validLabelValue(v string) bool {
	return len(v) <= maxLabelValueLen && utf8.ValidString(v) && strings.IndexFunc(v, isASCIIControl) < 0
}

func isASCIIControl(r rune) bool { return r < 0x20 || r == 0x7f }

// Description is free text stored beside a spec. At most
// MaxDescriptionBytes of UTF-8, with no ASCII control characters other than
// tab and newline. The zero value is the empty description. On the wire
// only ImageSpec has a description field; instance and key pair metadata
// keep the zero value.
type Description struct{ s string }

// ParseDescription validates s. It fails with CodeInvalid.
func ParseDescription(s string) (Description, error) {
	if len(s) > MaxDescriptionBytes || !utf8.ValidString(s) {
		return Description{}, Errorf(CodeInvalid, "invalid description")
	}
	for _, r := range s {
		if r != '\t' && r != '\n' && isASCIIControl(r) {
			return Description{}, Errorf(CodeInvalid, "invalid description")
		}
	}
	return Description{s: s}, nil
}

// String returns the description text.
func (d Description) String() string { return d.s }

// Metadata is what callers organize a resource by and the agent never sees.
//
// It is stored beside the spec, not in it. A change advances Revision and
// never Generation, so it creates no reconciler work and never makes an
// image unavailable.
type Metadata struct {
	Labels      Labels
	Description Description
	// Revision is 0 when the resource is created and advances by exactly 1
	// on every committed metadata change. CommitDesired rejects a write
	// whose Revision is not stored+1.
	Revision int64
	// ChangedAt is when Revision last advanced. It is zero while Revision
	// is 0. Spec.ChangedAt does not move on metadata changes, because the
	// reconciler measures its give-up deadline from Spec.ChangedAt.
	ChangedAt time.Time
}

// MetadataPatch is a validated change to a resource's metadata. The zero
// value changes nothing and encodes to zero bytes, so an intent that gained
// a patch field keeps its old fingerprint when the patch is zero.
type MetadataPatch struct {
	labels labelPatch
	desc   descriptionPatch
}

type labelMode uint8

const (
	keepLabels labelMode = iota
	replaceLabels
	editLabels
)

type labelPatch struct {
	mode labelMode
	// set is the new label set for replaceLabels, and the entries to upsert
	// for editLabels.
	set Labels
	// remove is used only by editLabels. Keys are unique, ascending, valid,
	// and not in set.
	remove []string
}

type descriptionPatch struct {
	set bool
	d   Description
}

// ReplaceLabels makes p replace the whole label set with l. An empty l
// clears the labels, which is not the same as the zero patch.
func (p MetadataPatch) ReplaceLabels(l Labels) MetadataPatch {
	p.labels = labelPatch{mode: replaceLabels, set: l}
	return p
}

// EditLabels makes p set each entry of set and delete each key in remove,
// leaving every other label alone. It fails with CodeInvalid for an invalid
// key in remove, or a key in both. An empty set and an empty remove make p
// keep the labels.
func (p MetadataPatch) EditLabels(set Labels, remove []string) (MetadataPatch, error) {
	rm := slices.Compact(slices.Sorted(slices.Values(remove)))
	for _, k := range rm {
		if !validLabelKey(k) {
			return p, Errorf(CodeInvalid, "invalid label key %q", k)
		}
		if _, ok := set.Get(k); ok {
			return p, Errorf(CodeInvalid, "label %q is both set and removed", k)
		}
	}
	if set.Len() == 0 && len(rm) == 0 {
		p.labels = labelPatch{}
		return p, nil
	}
	p.labels = labelPatch{mode: editLabels, set: set, remove: rm}
	return p, nil
}

// SetDescription makes p replace the description with d. An empty d clears
// it, which is not the same as the zero patch.
func (p MetadataPatch) SetDescription(d Description) MetadataPatch {
	p.desc = descriptionPatch{set: true, d: d}
	return p
}

// IsZero reports whether p changes nothing.
func (p MetadataPatch) IsZero() bool {
	return p.labels.mode == keepLabels && !p.desc.set
}

// apply returns cur with p applied and reports whether labels or
// description changed. It leaves Revision and ChangedAt as they were. It
// fails with CodeInvalid when an edit would leave more than MaxLabels
// entries.
func (p MetadataPatch) apply(cur Metadata) (Metadata, bool, error) {
	next := cur
	switch p.labels.mode {
	case replaceLabels:
		next.Labels = p.labels.set
	case editLabels:
		m := cur.Labels.Map()
		if m == nil {
			m = map[string]string{}
		}
		for _, k := range p.labels.set.keys() {
			v, _ := p.labels.set.Get(k)
			m[k] = v
		}
		for _, k := range p.labels.remove {
			delete(m, k)
		}
		if len(m) > MaxLabels {
			return Metadata{}, false, Errorf(CodeInvalid, "labels exceed %d entries", MaxLabels)
		}
		next.Labels = labelsOf(m)
	}
	if p.desc.set {
		next.Description = p.desc.d
	}
	changed := !next.Labels.Equal(cur.Labels) || next.Description != cur.Description
	return next, changed, nil
}

// next returns the metadata after p, or nil when p changes nothing. It is
// the only place Revision advances.
func (p MetadataPatch) next(cur Metadata, env Env) (*Metadata, error) {
	m, changed, err := p.apply(cur)
	if err != nil || !changed {
		return nil, err
	}
	m.Revision = cur.Revision + 1
	m.ChangedAt = env.Now
	return &m, nil
}

// encode writes only the parts of p that change something, each after its
// own tag. A zero patch writes nothing. Keys are written in ascending
// order, so map order never reaches the hash.
//
//	replace  "labels=" count k1 v1 ... kn vn
//	edit     "labels+" count k1 v1 ... kn vn  "labels-" count r1 ... rm
//	desc     "description=" text
func (p MetadataPatch) encode(e *encoder) {
	switch p.labels.mode {
	case replaceLabels:
		e.labelSet("labels=", p.labels.set)
	case editLabels:
		e.labelSet("labels+", p.labels.set)
		e.field("labels-")
		e.count(len(p.labels.remove))
		for _, k := range p.labels.remove {
			e.field(k)
		}
	}
	if p.desc.set {
		e.field("description=")
		e.field(p.desc.d.s)
	}
}

func (e *encoder) count(n int) { e.field(strconv.Itoa(n)) }

func (e *encoder) labelSet(tag string, l Labels) {
	keys := l.keys()
	e.field(tag)
	e.count(len(keys))
	for _, k := range keys {
		e.field(k)
		v, _ := l.Get(k)
		e.field(v)
	}
}
