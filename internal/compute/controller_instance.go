package compute

import (
	"fmt"
	"time"
)

// VolumeAttachment is one volume of a controller-driven instance. Volume is the
// storage service's resource name, so its project and zone are the volume's.
type VolumeAttachment struct {
	Volume     VolumeRef
	DeviceName string
	Boot       bool
	// Claim is the PersistentVolumeClaim that backs the volume, as the
	// storage service reports it in VolumeStatus.claim_name. The edge fills
	// it from that answer; it is not part of the request, so it takes no
	// part in fingerprints or in request matching.
	Claim string
}

// sameAttachment compares what a caller asked for, not the claim.
func sameAttachment(a, b VolumeAttachment) bool {
	return a.Volume == b.Volume && a.DeviceName == b.DeviceName && a.Boot == b.Boot
}

// checkAttachments applies the instance.proto volume rules: device names
// unique and non-empty, every volume in zone, exactly one boot entry.
func checkAttachments(zone string, vols []VolumeAttachment) error {
	devices := map[string]bool{}
	boot := 0
	for _, a := range vols {
		if a.DeviceName == "" || devices[a.DeviceName] {
			return Errorf(CodeInvalid, "volume device names must be unique and non-empty")
		}
		devices[a.DeviceName] = true
		if segment(a.Volume.s, 3) != zone {
			return Errorf(CodeInvalid, "volume %s is not in the instance zone", a.Volume)
		}
		if a.Boot {
			boot++
		}
	}
	if boot != 1 {
		return Errorf(CodeInvalid, "volumes must include exactly one boot entry")
	}
	return nil
}

// NetworkInterface is one requested interface of a controller-driven instance.
// Subnet and SecurityGroups are resource names. The control plane stores
// them as sent and does not look them up.
type NetworkInterface struct {
	DeviceName     string
	Subnet         string
	PrivateIPv4    string
	SecurityGroups []string
}

// ObservedInterface is what a controller reports for one interface.
type ObservedInterface struct {
	DeviceName  string
	Subnet      string
	PrivateIPv4 string
	PublicIPv4  string
}

// ReportState is the reconcile state a controller reports for one instance.
type ReportState uint8

const (
	ReportUnspecified ReportState = iota
	ReportPending
	ReportReconciling
	ReportReady
	ReportDeleting
	ReportError
)

// Report is the part of a controller's last accepted report that is not
// Observed: the state it reported, its message, the power it saw, and the
// interfaces it configured. The zero value means nothing was reported.
type Report struct {
	// Node is the name of the machine host the controller reported as
	// observed.node. It is informational. This service does not place
	// instances.
	Node       string
	State      ReportState
	Message    string
	Power      Power
	Interfaces []ObservedInterface
}

// ControllerDriven reports whether an external controller, not the in-process reconciler, converges
// this instance. CreateInstance sets Project and Zone, RunInstance leaves
// them empty.
func (s InstanceSpec) ControllerDriven() bool { return s.Zone != "" }

// Removed reports whether the instance row is gone as far as callers can
// tell: a delete was accepted and then converged by the controller's
// DELETING report.
func (i Instance) Removed() bool {
	return i.Spec.Power == PowerTerminated && i.Status.Observed.Generation == i.Spec.Generation
}

// InstanceReport is one controller report for one instance, already parsed.
type InstanceReport struct {
	// Node is observed.node.
	Node string
	// Generation is observed.generation.
	Generation int64
	State      ReportState
	Message    string
	Power      Power
	Interfaces []ObservedInterface
}

// ApplyReport returns the status that results from rep and whether it
// differs from the stored one. The rules:
//
//   - a report ahead of the spec generation fails with CodeAborted;
//   - a report behind the stored observed generation changes nothing;
//   - Observed.Generation advances to the reported generation only when the
//     report finishes it: READY on a live instance, DELETING on a terminated
//     one, or ERROR, which parks that generation. RECONCILING and PENDING
//     record the report and leave the generation where it was.
//
// A terminated spec never converges on READY, so a controller cannot keep a
// deleted instance alive by calling it ready.
func ApplyReport(inst Instance, rep InstanceReport, now time.Time) (InstanceStatus, bool, error) {
	spec, st := inst.Spec, inst.Status
	switch {
	case !spec.ControllerDriven():
		return st, false, Errorf(CodeFailedPrecondition, "instance %s is not controller-driven", spec.ID)
	case rep.Generation > spec.Generation:
		return st, false, Errorf(CodeAborted, "observed generation %d is ahead of the stored generation %d", rep.Generation, spec.Generation)
	case rep.State == ReportUnspecified || rep.State > ReportError:
		return st, false, Errorf(CodeInvalid, "observed.state is required")
	case rep.Generation < st.Observed.Generation:
		return st, false, nil
	}
	next := st
	next.Report = Report{
		Node: rep.Node, State: rep.State, Message: rep.Message, Power: rep.Power,
		Interfaces: append([]ObservedInterface(nil), rep.Interfaces...),
	}
	switch {
	case rep.State == ReportError:
		next.Observed = Observed{Generation: rep.Generation, Failure: &Failure{Code: CodeInternal, Message: rep.Message}}
	case rep.State == ReportReady && spec.Power != PowerTerminated,
		rep.State == ReportDeleting && spec.Power == PowerTerminated:
		next.Observed = Observed{Generation: rep.Generation}
	}
	next.UpdatedAt = now
	return next, true, nil
}

// MaxUserDataBytes is the InstanceSpec.user_data limit.
const MaxUserDataBytes = 32768

// CreateInstanceParams is a parsed CreateInstance request body. Strings are
// resource names the gRPC edge already shaped.
type CreateInstanceParams struct {
	ID          string
	Project     string
	Zone        string
	MachineType string
	Power       Power
	KeyPair     string
	UserData    string
	NICs        []NetworkInterface
	Volumes     []VolumeAttachment
	Labels      Labels
}

type createInstance struct {
	id       InstanceID
	project  string
	zone     string
	typ      InstanceType
	power    Power
	key      KeyName
	userData string
	nics     []NetworkInterface
	volumes  []VolumeAttachment
	labels   Labels
}

// NewCreateInstance creates a controller-driven instance under the caller's id.
// It checks what the spec rules in instance.proto say the server alone can
// check: power, volume and interface shape, and that every volume is in the
// instance's zone. Whether a volume exists is the storage service's answer
// and is asked at the edge. That no other live instance holds a volume is
// the store's.
func NewCreateInstance(p CreateInstanceParams) (Intent, error) {
	id, err := ParseInstanceID(p.ID)
	if err != nil {
		return nil, err
	}
	typ, err := ParseInstanceType(p.MachineType)
	if err != nil {
		return nil, err
	}
	if p.Power != PowerRunning && p.Power != PowerStopped {
		return nil, Errorf(CodeInvalid, "power must be running or stopped")
	}
	var key KeyName
	if p.KeyPair != "" {
		if key, err = ParseKeyName(p.KeyPair); err != nil {
			return nil, err
		}
	}
	if len(p.UserData) > MaxUserDataBytes {
		return nil, Errorf(CodeInvalid, "user_data exceeds %d bytes", MaxUserDataBytes)
	}
	if err := checkAttachments(p.Zone, p.Volumes); err != nil {
		return nil, err
	}
	nics := map[string]bool{}
	for _, n := range p.NICs {
		if n.DeviceName == "" || nics[n.DeviceName] {
			return nil, Errorf(CodeInvalid, "network interface device names must be unique and non-empty")
		}
		nics[n.DeviceName] = true
	}
	if !validResourceID(p.Project) || !validResourceID(p.Zone) {
		return nil, Errorf(CodeInvalid, "invalid project or zone")
	}
	return createInstance{
		id: id, project: p.Project, zone: p.Zone, typ: typ, power: p.Power, key: key,
		userData: p.UserData, nics: cloneNICs(p.NICs), volumes: append([]VolumeAttachment(nil), p.Volumes...), labels: p.Labels,
	}, nil
}

func cloneNICs(in []NetworkInterface) []NetworkInterface {
	out := make([]NetworkInterface, len(in))
	for i, n := range in {
		n.SecurityGroups = append([]string(nil), n.SecurityGroups...)
		out[i] = n
	}
	return out
}

func (createInstance) Kind() OperationKind      { return OpCreateInstance }
func (in createInstance) carriesMetadata() bool { return in.labels.Len() > 0 }

// encode is positional and length-prefixed. Order is the order of the
// proto fields: it is persisted, so it must stay byte-stable.
func (in createInstance) encode(e *encoder) {
	e.field(in.id.s)
	e.field(in.project)
	e.field(in.zone)
	e.field(in.typ.name)
	e.field(fmt.Sprint(int(in.power)))
	e.field(in.key.s)
	e.field(in.userData)
	e.field(fmt.Sprint(len(in.volumes)))
	for _, a := range in.volumes {
		e.field(a.Volume.s)
		e.field(a.DeviceName)
		e.field(fmt.Sprint(a.Boot))
	}
	e.field(fmt.Sprint(len(in.nics)))
	for _, n := range in.nics {
		e.field(n.DeviceName)
		e.field(n.Subnet)
		e.field(n.PrivateIPv4)
		e.field(fmt.Sprint(len(n.SecurityGroups)))
		for _, g := range n.SecurityGroups {
			e.field(g)
		}
	}
	if in.labels.Len() > 0 {
		e.labelSet("labels=", in.labels)
	}
}

// plan writes generation 1 under the caller's id. An id that already holds
// an instance, live or terminated, is CodeAlreadyExists, except that a live
// instance whose stored desired state equals this request is returned as
// is: a retry under a new request id, or a restart that lost the reply,
// gets an operation for the instance it created. Two callers of one id race
// at commit: the loser conflicts, replans, and then sees the winner.
func (in createInstance) plan(v View, env Env) (Change, error) {
	cur, err := v.Instance(in.id)
	switch {
	case err == nil:
		if in.matches(cur) {
			return Change{Spec: cur.Spec}, nil
		}
		return Change{}, Errorf(CodeAlreadyExists, "instance %s already exists", in.id)
	case CodeOf(err) != CodeNotFound:
		return Change{}, err
	}
	var key *AuthorizedKey
	if !in.key.IsZero() {
		kp, err := v.KeyPairByName(in.key)
		if err != nil {
			return Change{}, err
		}
		if kp.Spec.Deleted {
			return Change{}, Errorf(CodeNotFound, "key pair %q not found", in.key)
		}
		key = &AuthorizedKey{Name: kp.Spec.Name, PublicKey: kp.Spec.PublicKey}
	}
	ch := Change{Write: true, Spec: InstanceSpec{
		ID: in.id, Generation: 1, CreatedAt: env.Now, ChangedAt: env.Now,
		Type: in.typ, Key: key, Power: in.power,
		Project: in.project, Zone: in.zone, UserData: in.userData,
		Volumes: append([]VolumeAttachment(nil), in.volumes...), NICs: cloneNICs(in.nics),
	}}
	if in.labels.Len() > 0 {
		ch.Meta = &Metadata{Labels: in.labels}
	}
	return ch, nil
}

func (in createInstance) matches(cur Instance) bool {
	s := cur.Spec
	if s.Power == PowerTerminated || !s.ControllerDriven() || s.Project != in.project || s.Zone != in.zone ||
		s.Type.name != in.typ.name || s.Power != in.power || s.UserData != in.userData ||
		len(s.Volumes) != len(in.volumes) || len(s.NICs) != len(in.nics) || !cur.Meta.Labels.Equal(in.labels) {
		return false
	}
	if (s.Key == nil) != in.key.IsZero() || (s.Key != nil && s.Key.Name != in.key) {
		return false
	}
	for i, a := range s.Volumes {
		if !sameAttachment(a, in.volumes[i]) {
			return false
		}
	}
	for i, n := range s.NICs {
		o := in.nics[i]
		if n.DeviceName != o.DeviceName || n.Subnet != o.Subnet || n.PrivateIPv4 != o.PrivateIPv4 || len(n.SecurityGroups) != len(o.SecurityGroups) {
			return false
		}
		for j, g := range n.SecurityGroups {
			if g != o.SecurityGroups[j] {
				return false
			}
		}
	}
	return true
}

// segment returns the i'th slash-separated segment of a resource name, or ""
// when the name has fewer.
func segment(name string, i int) string {
	start := 0
	for ; i > 0; i-- {
		for start < len(name) && name[start] != '/' {
			start++
		}
		start++
	}
	end := start
	for end < len(name) && name[end] != '/' {
		end++
	}
	if start > len(name) {
		return ""
	}
	return name[start:end]
}
