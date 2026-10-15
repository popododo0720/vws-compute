package kubestore

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/popododo0720/vws-compute/internal/compute"
)

// A record is the JSON in one resource's ConfigMap. Every value is
// re-validated through compute's parsers on read, so a corrupted object
// surfaces as an error and never as an invalid domain value.

type failureJSON struct {
	Code    uint8  `json:"code"`
	Message string `json:"message,omitempty"`
	Reason  uint8  `json:"reason,omitempty"`
}

type observedJSON struct {
	Generation int64        `json:"generation"`
	Failure    *failureJSON `json:"failure,omitempty"`
}

type retryJSON struct {
	Attempts  int    `json:"attempts,omitempty"`
	At        int64  `json:"at,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

type metaJSON struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Description string            `json:"description,omitempty"`
	Revision    int64             `json:"revision,omitempty"`
	ChangedAt   int64             `json:"changed_at,omitempty"`
}

type opJSON struct {
	ID           string       `json:"id"`
	Request      string       `json:"request"`
	Fingerprint  string       `json:"fingerprint"`
	Kind         string       `json:"kind"`
	TargetKind   uint8        `json:"target_kind"`
	TargetID     string       `json:"target_id"`
	TargetGen    int64        `json:"target_generation"`
	MetadataOnly bool         `json:"metadata_only,omitempty"`
	CreatedAt    int64        `json:"created_at"`
	State        uint8        `json:"state"`
	Failure      *failureJSON `json:"failure,omitempty"`
	SettledAt    int64        `json:"settled_at,omitempty"`
}

type volumeJSON struct {
	Volume string `json:"volume"`
	Device string `json:"device"`
	Boot   bool   `json:"boot,omitempty"`
	Claim  string `json:"claim,omitempty"`
	// DeleteOnTermination is absent on rows written before it existed,
	// which keeps their volumes.
	DeleteOnTermination bool `json:"delete_on_termination,omitempty"`
}

type nicJSON struct {
	Device         string   `json:"device"`
	Subnet         string   `json:"subnet"`
	PrivateIPv4    string   `json:"private_ipv4,omitempty"`
	SecurityGroups []string `json:"security_groups,omitempty"`
}

type observedNICJSON struct {
	Device      string `json:"device"`
	Subnet      string `json:"subnet,omitempty"`
	PrivateIPv4 string `json:"private_ipv4,omitempty"`
	PublicIPv4  string `json:"public_ipv4,omitempty"`
}

type reportJSON struct {
	Node       string            `json:"node,omitempty"`
	State      uint8             `json:"state,omitempty"`
	Message    string            `json:"message,omitempty"`
	Power      uint8             `json:"power,omitempty"`
	Interfaces []observedNICJSON `json:"interfaces,omitempty"`
}

type instanceJSON struct {
	ID          string `json:"id"`
	Generation  int64  `json:"generation"`
	CreatedAt   int64  `json:"created_at"`
	ChangedAt   int64  `json:"changed_at"`
	Image       string `json:"image,omitempty"`
	Type        string `json:"type"`
	KeyName     string `json:"key_name,omitempty"`
	KeyPublic   string `json:"key_public,omitempty"`
	Power       uint8  `json:"power"`
	RebootEpoch uint32 `json:"reboot_epoch,omitempty"`
	Project     string `json:"project,omitempty"`
	Zone        string `json:"zone,omitempty"`
	UserData    string `json:"user_data,omitempty"`

	Volumes []volumeJSON `json:"volumes,omitempty"`
	NICs    []nicJSON    `json:"nics,omitempty"`

	Observed     observedJSON `json:"observed"`
	Machine      uint8        `json:"machine,omitempty"`
	AppliedEpoch uint32       `json:"applied_epoch,omitempty"`
	Retry        retryJSON    `json:"retry"`
	UpdatedAt    int64        `json:"updated_at,omitempty"`
	Report       reportJSON   `json:"report"`
	Released     bool         `json:"volumes_released,omitempty"`
	Meta         metaJSON     `json:"meta"`
}

type imageJSON struct {
	ID           string `json:"id"`
	Project      string `json:"project,omitempty"`
	Region       string `json:"region,omitempty"`
	Generation   int64  `json:"generation"`
	CreatedAt    int64  `json:"created_at"`
	ChangedAt    int64  `json:"changed_at"`
	Name         string `json:"name,omitempty"`
	SourceKind   string `json:"source_kind"`
	Source       string `json:"source"`
	Deregistered bool   `json:"deregistered,omitempty"`

	Observed  observedJSON `json:"observed"`
	Present   bool         `json:"present,omitempty"`
	SizeBytes int64        `json:"size_bytes,omitempty"`
	Retry     retryJSON    `json:"retry"`
	UpdatedAt int64        `json:"updated_at,omitempty"`
	Meta      metaJSON     `json:"meta"`
}

type keyPairJSON struct {
	ID         string   `json:"id"`
	Generation int64    `json:"generation"`
	CreatedAt  int64    `json:"created_at"`
	ChangedAt  int64    `json:"changed_at"`
	Name       string   `json:"name"`
	PublicKey  string   `json:"public_key"`
	Deleted    bool     `json:"deleted,omitempty"`
	Meta       metaJSON `json:"meta"`
}

type recordJSON struct {
	Instance *instanceJSON `json:"instance,omitempty"`
	Image    *imageJSON    `json:"image,omitempty"`
	KeyPair  *keyPairJSON  `json:"key_pair,omitempty"`
	Ops      []opJSON      `json:"ops,omitempty"`
}

func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func corrupt(what, id string, err error) error {
	return compute.Errorf(compute.CodeInternal, "corrupt %s record %q: %v", what, id, err)
}

func inRange(field string, v, lo, hi int64) error {
	if v < lo || v > hi {
		return fmt.Errorf("%s %d out of range", field, v)
	}
	return nil
}

func failureTo(f *compute.Failure) *failureJSON {
	if f == nil {
		return nil
	}
	return &failureJSON{Code: uint8(f.Code), Message: f.Message, Reason: uint8(f.Reason)}
}

func failureFrom(f *failureJSON) (*compute.Failure, error) {
	if f == nil {
		return nil, nil //nolint:nilnil // a nil failure is "no failure", the zero value of the field
	}
	if err := inRange("failure code", int64(f.Code), int64(compute.CodeInvalid), int64(compute.CodeInternal)); err != nil {
		return nil, err
	}
	if err := inRange("failure reason", int64(f.Reason), 0, int64(compute.LaunchImageNotFound)); err != nil {
		return nil, err
	}
	return &compute.Failure{Code: compute.Code(f.Code), Message: f.Message, Reason: compute.LaunchReason(f.Reason)}, nil
}

func observedTo(o compute.Observed) observedJSON {
	return observedJSON{Generation: o.Generation, Failure: failureTo(o.Failure)}
}

func observedFrom(o observedJSON) (compute.Observed, error) {
	f, err := failureFrom(o.Failure)
	return compute.Observed{Generation: o.Generation, Failure: f}, err
}

func retryTo(r compute.Retry) retryJSON {
	return retryJSON{Attempts: r.Attempts, At: nanos(r.At), LastError: r.LastError}
}

func retryFrom(r retryJSON) compute.Retry {
	return compute.Retry{Attempts: r.Attempts, At: fromNanos(r.At), LastError: r.LastError}
}

func metaTo(m compute.Metadata) metaJSON {
	return metaJSON{Labels: m.Labels.Map(), Description: m.Description.String(), Revision: m.Revision, ChangedAt: nanos(m.ChangedAt)}
}

func metaFrom(m metaJSON) (compute.Metadata, error) {
	labels, err1 := compute.ParseLabels(m.Labels)
	desc, err2 := compute.ParseDescription(m.Description)
	err3 := inRange("meta revision", m.Revision, 0, 1<<62)
	var err4 error
	if (m.Revision == 0) != (m.ChangedAt == 0) {
		err4 = errors.New("metadata revision and change time disagree")
	}
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return compute.Metadata{}, err
	}
	return compute.Metadata{Labels: labels, Description: desc, Revision: m.Revision, ChangedAt: fromNanos(m.ChangedAt)}, nil
}

func opTo(o compute.Operation) opJSON {
	return opJSON{
		ID: o.ID.String(), Request: o.Request.String(), Fingerprint: hex.EncodeToString(o.Fingerprint[:]), Kind: o.Kind.String(),
		TargetKind: uint8(o.Target.Kind), TargetID: o.Target.ID, TargetGen: o.TargetGeneration, MetadataOnly: o.MetadataOnly,
		CreatedAt: nanos(o.CreatedAt), State: uint8(o.State()), Failure: failureTo(o.Failure()), SettledAt: nanos(o.SettledAt()),
	}
}

// opFrom is the only caller of compute.RestoreOperation.
func opFrom(j opJSON) (compute.Operation, error) {
	id, err1 := compute.ParseOperationID(j.ID)
	req, err2 := compute.ParseRequestID(j.Request)
	kind, err3 := compute.ParseOperationKind(j.Kind)
	failure, err4 := failureFrom(j.Failure)
	err5 := inRange("target kind", int64(j.TargetKind), int64(compute.KindInstance), int64(compute.KindKeyPair))
	fp, err6 := hex.DecodeString(j.Fingerprint)
	if err6 == nil && len(fp) != len(compute.Fingerprint{}) {
		err6 = errors.New("fingerprint has the wrong length")
	}
	if err := errors.Join(err1, err2, err3, err4, err5, err6); err != nil {
		return compute.Operation{}, corrupt("operation", j.ID, err)
	}
	base := compute.Operation{
		ID: id, Request: req, Kind: kind,
		Target:           compute.Ref{Kind: compute.Kind(j.TargetKind), ID: j.TargetID},
		TargetGeneration: j.TargetGen, CreatedAt: fromNanos(j.CreatedAt), MetadataOnly: j.MetadataOnly,
	}
	copy(base.Fingerprint[:], fp)
	return compute.RestoreOperation(base, compute.OperationState(j.State), failure, fromNanos(j.SettledAt))
}

func instanceTo(in compute.Instance) *instanceJSON {
	s, st := in.Spec, in.Status
	j := &instanceJSON{
		ID: s.ID.String(), Generation: s.Generation, CreatedAt: nanos(s.CreatedAt), ChangedAt: nanos(s.ChangedAt),
		Image: s.Image.String(), Type: s.Type.Name(), Power: uint8(s.Power), RebootEpoch: s.RebootEpoch,
		Project: s.Project, Zone: s.Zone, UserData: s.UserData,
		Observed: observedTo(st.Observed), Machine: uint8(st.Machine), AppliedEpoch: st.RebootEpoch,
		Retry: retryTo(st.Retry), UpdatedAt: nanos(st.UpdatedAt), Meta: metaTo(in.Meta),
		Report:   reportJSON{Node: st.Report.Node, State: uint8(st.Report.State), Message: st.Report.Message, Power: uint8(st.Report.Power)},
		Released: st.VolumesReleased,
	}
	if s.Key != nil {
		j.KeyName, j.KeyPublic = s.Key.Name.String(), s.Key.PublicKey.AuthorizedKey()
	}
	for _, v := range s.Volumes {
		j.Volumes = append(j.Volumes, volumeJSON{Volume: v.Volume.String(), Device: v.DeviceName, Boot: v.Boot, Claim: v.Claim, DeleteOnTermination: v.DeleteOnTermination})
	}
	for _, n := range s.NICs {
		j.NICs = append(j.NICs, nicJSON{Device: n.DeviceName, Subnet: n.Subnet, PrivateIPv4: n.PrivateIPv4, SecurityGroups: n.SecurityGroups})
	}
	for _, i := range st.Report.Interfaces {
		j.Report.Interfaces = append(j.Report.Interfaces, observedNICJSON{Device: i.DeviceName, Subnet: i.Subnet, PrivateIPv4: i.PrivateIPv4, PublicIPv4: i.PublicIPv4})
	}
	return j
}

func instanceFrom(j *instanceJSON) (compute.Instance, error) {
	id, err1 := compute.ParseInstanceID(j.ID)
	var img compute.ImageID
	var err2 error
	if j.Zone == "" {
		img, err2 = compute.ParseImageID(j.Image)
	}
	typ, err3 := compute.ParseMachineType(j.Type)
	obs, err4 := observedFrom(j.Observed)
	err5 := inRange("power", int64(j.Power), int64(compute.PowerRunning), int64(compute.PowerTerminated))
	err6 := inRange("machine", int64(j.Machine), int64(compute.MachineUnknown), int64(compute.MachineOn))
	var key *compute.AuthorizedKey
	var err7 error
	if j.KeyName != "" {
		name, errName := compute.ParseKeyName(j.KeyName)
		pub, errPub := compute.ParsePublicKey(j.KeyPublic)
		key, err7 = &compute.AuthorizedKey{Name: name, PublicKey: pub}, errors.Join(errName, errPub)
	}
	meta, err8 := metaFrom(j.Meta)
	var vols []compute.VolumeAttachment
	var errs []error
	for _, v := range j.Volumes {
		ref, err := compute.ParseVolumeRef(v.Volume)
		errs = append(errs, err)
		vols = append(vols, compute.VolumeAttachment{Volume: ref, DeviceName: v.Device, Boot: v.Boot, Claim: v.Claim, DeleteOnTermination: v.DeleteOnTermination})
	}
	var nics []compute.NetworkInterface
	for _, n := range j.NICs {
		nics = append(nics, compute.NetworkInterface{DeviceName: n.Device, Subnet: n.Subnet, PrivateIPv4: n.PrivateIPv4, SecurityGroups: n.SecurityGroups})
	}
	err9 := inRange("report state", int64(j.Report.State), int64(compute.ReportUnspecified), int64(compute.ReportError))
	err10 := inRange("report power", int64(j.Report.Power), 0, int64(compute.PowerTerminated))
	if err := errors.Join(err1, err2, err3, err4, err5, err6, err7, err8, err9, err10, errors.Join(errs...)); err != nil {
		return compute.Instance{}, corrupt("instance", j.ID, err)
	}
	report := compute.Report{Node: j.Report.Node, State: compute.ReportState(j.Report.State), Message: j.Report.Message, Power: compute.Power(j.Report.Power)}
	for _, i := range j.Report.Interfaces {
		report.Interfaces = append(report.Interfaces, compute.ObservedInterface{DeviceName: i.Device, Subnet: i.Subnet, PrivateIPv4: i.PrivateIPv4, PublicIPv4: i.PublicIPv4})
	}
	return compute.Instance{
		Spec: compute.InstanceSpec{
			ID: id, Generation: j.Generation, CreatedAt: fromNanos(j.CreatedAt), ChangedAt: fromNanos(j.ChangedAt),
			Image: img, Type: typ, Key: key, Power: compute.Power(j.Power), RebootEpoch: j.RebootEpoch,
			Project: j.Project, Zone: j.Zone, UserData: j.UserData, Volumes: vols, NICs: nics,
		},
		Status: compute.InstanceStatus{
			Observed: obs, Machine: compute.MachineState(j.Machine), RebootEpoch: j.AppliedEpoch,
			Retry: retryFrom(j.Retry), UpdatedAt: fromNanos(j.UpdatedAt), Report: report,
			VolumesReleased: j.Released,
		},
		Meta: meta,
	}, nil
}

func imageTo(in compute.Image) (*imageJSON, error) {
	s, st := in.Spec, in.Status
	j := &imageJSON{
		ID: s.ID.String(), Project: s.Scope.Project(), Region: s.Scope.Region(),
		Generation: s.Generation, CreatedAt: nanos(s.CreatedAt), ChangedAt: nanos(s.ChangedAt),
		Name: s.Name.String(), Deregistered: s.Deregistered,
		Observed: observedTo(st.Observed), Present: st.Present, SizeBytes: st.SizeBytes,
		Retry: retryTo(st.Retry), UpdatedAt: nanos(st.UpdatedAt), Meta: metaTo(in.Meta),
	}
	switch src := s.Source.(type) {
	case compute.FromURL:
		j.SourceKind, j.Source = "url", src.URL.String()
	case compute.FromInstance:
		j.SourceKind, j.Source = "instance", src.Instance.String()
	case compute.FromFile:
		j.SourceKind, j.Source = "file", src.Path.String()
	case compute.FromVolume:
		j.SourceKind, j.Source = "volume", src.Ref.String()
	case compute.FromSnapshot:
		j.SourceKind, j.Source = "snapshot", src.Ref.String()
	default:
		return nil, compute.Errorf(compute.CodeInternal, "image has no source")
	}
	return j, nil
}

func imageSourceFrom(kind, v string) (compute.ImageSource, error) {
	switch kind {
	case "url":
		u, err := compute.ParseImageURL(v)
		return compute.FromURL{URL: u}, err
	case "instance":
		id, err := compute.ParseInstanceID(v)
		return compute.FromInstance{Instance: id}, err
	case "file":
		p, err := compute.ParseImagePath(v)
		return compute.FromFile{Path: p}, err
	case "volume":
		r, err := compute.ParseVolumeRef(v)
		return compute.FromVolume{Ref: r}, err
	case "snapshot":
		r, err := compute.ParseSnapshotRef(v)
		return compute.FromSnapshot{Ref: r}, err
	}
	return nil, fmt.Errorf("unknown image source kind %q", kind)
}

// imageFrom decodes an image. A record without a project predates image
// scopes (#92) and takes legacy, which may be zero.
func imageFrom(j *imageJSON, legacy compute.ImageScope) (compute.Image, error) {
	id, err1 := compute.ParseImageID(j.ID)
	scope := legacy
	var err6 error
	if j.Project != "" || j.Region != "" {
		scope, err6 = compute.NewImageScope(j.Project, j.Region)
	}
	var name compute.ImageName
	var err2 error
	if j.Name != "" {
		name, err2 = compute.ParseImageName(j.Name)
	}
	obs, err3 := observedFrom(j.Observed)
	src, err4 := imageSourceFrom(j.SourceKind, j.Source)
	meta, err5 := metaFrom(j.Meta)
	if err := errors.Join(err1, err2, err3, err4, err5, err6); err != nil {
		return compute.Image{}, corrupt("image", j.ID, err)
	}
	return compute.Image{
		Spec: compute.ImageSpec{
			ID: id, Scope: scope, Generation: j.Generation, CreatedAt: fromNanos(j.CreatedAt), ChangedAt: fromNanos(j.ChangedAt),
			Name: name, Source: src, Deregistered: j.Deregistered,
		},
		Status: compute.ImageStatus{
			Observed: obs, Present: j.Present, SizeBytes: j.SizeBytes, Retry: retryFrom(j.Retry), UpdatedAt: fromNanos(j.UpdatedAt),
		},
		Meta: meta,
	}, nil
}

func keyPairTo(k compute.KeyPair) *keyPairJSON {
	s := k.Spec
	return &keyPairJSON{
		ID: s.ID.String(), Generation: s.Generation, CreatedAt: nanos(s.CreatedAt), ChangedAt: nanos(s.ChangedAt),
		Name: s.Name.String(), PublicKey: s.PublicKey.AuthorizedKey(), Deleted: s.Deleted, Meta: metaTo(k.Meta),
	}
}

func keyPairFrom(j *keyPairJSON) (compute.KeyPair, error) {
	id, err1 := compute.ParseKeyPairID(j.ID)
	name, err2 := compute.ParseKeyName(j.Name)
	pub, err3 := compute.ParsePublicKey(j.PublicKey)
	meta, err4 := metaFrom(j.Meta)
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return compute.KeyPair{}, corrupt("key pair", j.ID, err)
	}
	return compute.KeyPair{
		Spec: compute.KeyPairSpec{
			ID: id, Generation: j.Generation, CreatedAt: fromNanos(j.CreatedAt), ChangedAt: fromNanos(j.ChangedAt),
			Name: name, PublicKey: pub, Deleted: j.Deleted,
		},
		Meta: meta,
	}, nil
}

func encode(v any) (string, error) {
	b, err := json.Marshal(v)
	return string(b), err
}
