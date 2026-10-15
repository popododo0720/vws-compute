package grpcapi

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"

	commonv1 "github.com/popododo0720/vws-proto/gen/go/vws/common/v1"
	computev1 "github.com/popododo0720/vws-proto/gen/go/vws/compute/v1"

	"github.com/popododo0720/vws-compute/internal/compute"
	"github.com/popododo0720/vws-compute/internal/store"
)

func (s *server) GetInstance(ctx context.Context, r *computev1.GetInstanceRequest) (*computev1.Instance, error) {
	inst, name, err := s.liveInstance(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	return instancePB(inst, name), nil
}

func (s *server) ListInstances(ctx context.Context, r *computev1.ListInstancesRequest) (*computev1.ListInstancesResponse, error) {
	project, zone, err := parseZonalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	var matched []compute.Instance
	after := ""
	for {
		page, err := s.read.Instances(ctx, store.InstanceQuery{PageSize: store.MaxPageSize, After: after})
		if err != nil {
			return nil, toStatus(err)
		}
		for _, inst := range page.Items {
			if inst.Removed() || inst.Lifecycle() == compute.InstanceTerminated {
				continue
			}
			if inst.Spec.ControllerDriven() && (inst.Spec.Project != project || inst.Spec.Zone != zone) {
				continue
			}
			matched = append(matched, inst)
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	page, next, err := cutPage(matched, r.GetPageSize(), r.GetPageToken(), func(i compute.Instance) string {
		return instanceResourceName(project, zone, i.Spec.ID.String())
	})
	if err != nil {
		return nil, err
	}
	out := make([]*computev1.Instance, 0, len(page))
	for _, inst := range page {
		out = append(out, instancePB(inst, instanceResourceName(project, zone, inst.Spec.ID.String())))
	}
	return &computev1.ListInstancesResponse{Instances: out, NextPageToken: next}, nil
}

func (s *server) CreateInstance(ctx context.Context, r *computev1.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, zone, err := parseZonalParent(r.GetParent())
	if err != nil {
		return nil, err
	}
	if err := validateResourceID(r.GetInstanceId()); err != nil {
		return nil, err
	}
	name := instanceResourceName(project, zone, r.GetInstanceId())
	if err := validateCreateInstance(zone, name, r.GetInstance()); err != nil {
		return nil, err
	}
	in, err := createInstanceIntent(project, zone, r.GetInstanceId(), r.GetInstance(), nil)
	if err != nil {
		return nil, err
	}
	if r.GetValidateOnly() {
		if _, err := s.verifyVolumes(ctx, r.GetInstance().GetSpec().GetVolumes(), name); err != nil {
			return nil, err
		}
		id, perr := compute.ParseInstanceID(r.GetInstanceId())
		if perr != nil {
			return nil, toStatus(perr)
		}
		if _, err := s.read.Instance(ctx, id); err == nil {
			return nil, alreadyExists("instance " + name + " already exists")
		} else if compute.CodeOf(err) != compute.CodeNotFound {
			return nil, toStatus(err)
		}
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_CREATE, r.GetRequestId(), nil)
	}
	// fresh runs only for a request id the store has not seen. A retry after
	// the instance was stored, or after a restart, returns the stored
	// operation without asking the storage service anything.
	rc, err := s.submit(ctx, r.GetRequestId(), in, nil, func() (compute.Intent, error) {
		claims, err := s.verifyVolumes(ctx, r.GetInstance().GetSpec().GetVolumes(), name)
		if err != nil {
			return nil, err
		}
		return createInstanceIntent(project, zone, r.GetInstanceId(), r.GetInstance(), claims)
	})
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, name, rc.Operation)
}

// createInstanceIntent builds the intent. claims maps volume names to the
// claim names storage reported; nil leaves them empty, which does not
// change the fingerprint.
func createInstanceIntent(project, zone, id string, body *computev1.Instance, claims map[string]string) (compute.Intent, error) {
	spec := body.GetSpec()
	labels, err := compute.ParseLabels(body.GetLabels())
	if err != nil {
		return nil, toStatus(err)
	}
	p := compute.CreateInstanceParams{
		ID: id, Project: project, Zone: zone, MachineType: spec.GetMachineType(), UserData: spec.GetUserData(), Labels: labels,
	}
	if spec.GetPower() == computev1.InstancePower_INSTANCE_POWER_RUNNING {
		p.Power = compute.PowerRunning
	} else {
		p.Power = compute.PowerStopped
	}
	if spec.GetKeyPair() != "" {
		if _, _, _, err := parseKeyPairName(spec.GetKeyPair()); err != nil {
			return nil, err
		}
		// KeyPairService stores a key pair under its full resource name, so
		// the instance resolves it by that name, not its last segment.
		p.KeyPair = spec.GetKeyPair()
	}
	if p.Volumes, err = attachments(spec.GetVolumes(), claims); err != nil {
		return nil, err
	}
	for _, n := range spec.GetNetworkInterfaces() {
		p.NICs = append(p.NICs, compute.NetworkInterface{
			DeviceName: n.GetDeviceName(), Subnet: n.GetSubnet(), PrivateIPv4: n.GetPrivateIpv4(),
			SecurityGroups: append([]string(nil), n.GetSecurityGroups()...),
		})
	}
	in, err := compute.NewCreateInstance(p)
	if err != nil {
		return nil, toStatus(err)
	}
	return in, nil
}

func attachments(vols []*computev1.VolumeAttachment, claims map[string]string) ([]compute.VolumeAttachment, error) {
	var out []compute.VolumeAttachment
	for _, v := range vols {
		ref, err := compute.ParseVolumeRef(v.GetVolume())
		if err != nil {
			return nil, toStatus(err)
		}
		// The root volume goes with the instance, like an EC2 root device
		// with DeleteOnTermination true. vws-proto has no
		// delete_on_termination yet, so data volumes stay.
		out = append(out, compute.VolumeAttachment{
			Volume: ref, DeviceName: v.GetDeviceName(), Boot: v.GetBoot(), Claim: claims[v.GetVolume()],
			DeleteOnTermination: v.GetBoot(),
		})
	}
	return out, nil
}

// verifyVolumes asks the storage service about each volume: it must exist,
// must not be going away, must not be attached to another instance, and
// must not have failed in storage. It returns the claim name per volume,
// empty for a volume storage is still provisioning: RunInstances creates the
// root volume from the image and the instance back to back, so such an
// instance is accepted and waits (the reconciler resolves the claim through
// Claims) instead of failing. The store enforces the same one-holder rule
// atomically with the write, so this check only turns a doomed request into
// an early, specific error.
func (s *server) verifyVolumes(ctx context.Context, vols []*computev1.VolumeAttachment, instance string) (map[string]string, error) {
	if s.volumes == nil {
		return nil, dependencyMissing("no storage service is configured, so volume " + vols[0].GetVolume() + " cannot be verified")
	}
	claims := make(map[string]string, len(vols))
	for _, v := range vols {
		info, err := s.volumes.Volume(ctx, v.GetVolume())
		switch {
		case errors.Is(err, ErrVolumeNotFound):
			return nil, dependencyMissing("volume " + v.GetVolume() + " does not exist")
		case err != nil:
			return nil, withReason(codes.Unavailable, "DEPENDENCY_UNAVAILABLE", "storage service: "+err.Error())
		case info.Deleting:
			return nil, dependencyMissing("volume " + v.GetVolume() + " is being deleted")
		case info.Instance != "" && info.Instance != instance:
			return nil, failedPre("VOLUME_IN_USE", "volume "+v.GetVolume()+" is attached to "+info.Instance)
		case info.State == commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR:
			// Storage has given up on this volume; retrying will not help.
			return nil, failedPre("VOLUME_FAILED", "volume "+v.GetVolume()+" failed in storage: "+info.Message)
		}
		claims[v.GetVolume()] = info.ClaimName
	}
	return claims, nil
}

func (s *server) UpdateInstance(ctx context.Context, r *computev1.UpdateInstanceRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	body := r.GetInstance()
	if body == nil {
		return nil, invalidSpec("instance is required")
	}
	project, _, id, err := parseInstanceName(body.GetName())
	if err != nil {
		return nil, err
	}
	paths := effectiveMask(r.GetUpdateMask().GetPaths())
	if err := checkMask(paths); err != nil {
		return nil, err
	}
	patch, err := metadataPatch(paths, body.GetLabels(), nil, maskIncludes(paths, "spec"))
	if err != nil {
		return nil, toStatus(err)
	}
	existing, name, err := s.liveInstance(ctx, body.GetName())
	if err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return nil, dependencyMissing("instance " + body.GetName() + " is absent and this process cannot create one without a volume")
		}
		return nil, err
	}
	etagCheck := func() error {
		cur, _, err := s.liveInstance(ctx, body.GetName())
		if err != nil {
			return err
		}
		return checkEtag(body.GetEtag(), etagOf(cur.Spec.Generation, cur.Meta.Revision))
	}
	fresh := check(etagCheck)
	var in compute.Intent
	if maskIncludes(paths, "spec") {
		spec := body.GetSpec()
		if spec == nil {
			return nil, invalidSpec("instance.spec is required")
		}
		// A bare "spec" replaces the mutable spec as a whole. "spec.power" and
		// "spec.volumes" touch only that field and ignore the rest of the body,
		// so a client may send its full spec with a narrow mask.
		whole := slices.Contains(paths, "spec")
		setPower, setVolumes := whole, whole
		for _, p := range paths {
			switch {
			case p == "spec.power":
				setPower = true
			case p == "spec.volumes":
				setVolumes = true
			case p != "spec" && strings.HasPrefix(p, "spec."):
				return nil, invalidSpec("field " + p + " cannot be updated")
			}
		}
		if whole {
			if err := instanceSpecDelta(spec, existing); err != nil {
				return nil, err
			}
		}
		power := existing.Spec.Power
		if setPower {
			switch spec.GetPower() {
			case computev1.InstancePower_INSTANCE_POWER_RUNNING:
				power = compute.PowerRunning
			case computev1.InstancePower_INSTANCE_POWER_STOPPED:
				power = compute.PowerStopped
			default:
				return nil, invalidSpec("instance.spec.power must be RUNNING or STOPPED")
			}
		}
		if vols := spec.GetVolumes(); setVolumes && (len(vols) > 0 || !whole) {
			if err := validateVolumes(existing.Spec.Zone, vols); err != nil {
				return nil, err
			}
			atts, aerr := attachments(vols, nil)
			if aerr != nil {
				return nil, aerr
			}
			in, err = compute.NewUpdateInstanceSpec(id, power, atts, patch)
			fresh = func() (compute.Intent, error) {
				if ferr := etagCheck(); ferr != nil {
					return nil, ferr
				}
				claims, verr := s.verifyVolumes(ctx, vols, body.GetName())
				if verr != nil {
					return nil, verr
				}
				atts, aerr := attachments(vols, claims)
				if aerr != nil {
					return nil, aerr
				}
				power := power
				if !setPower {
					// The mask leaves power alone: keep what is stored now,
					// not what was stored when the request arrived.
					cur, _, lerr := s.liveInstance(ctx, body.GetName())
					if lerr != nil {
						return nil, lerr
					}
					power = cur.Spec.Power
				}
				return compute.NewUpdateInstanceSpec(id, power, atts, patch)
			}
		} else {
			in, err = compute.NewSetPower(id, power, patch)
		}
	} else {
		in, err = compute.NewUpdateInstanceMetadata(id, patch)
	}
	if r.GetValidateOnly() {
		if err != nil {
			return nil, toStatus(err)
		}
		if err := checkEtag(body.GetEtag(), etagOf(existing.Spec.Generation, existing.Meta.Revision)); err != nil {
			return nil, err
		}
		return doneOperation(project, name, commonv1.OperationVerb_OPERATION_VERB_UPDATE, r.GetRequestId(), nil)
	}
	rc, err := s.submit(ctx, r.GetRequestId(), in, err, fresh)
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, name, rc.Operation)
}

func (s *server) DeleteInstance(ctx context.Context, r *computev1.DeleteInstanceRequest) (*longrunningpb.Operation, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	project, _, id, err := parseInstanceName(r.GetName())
	if err != nil {
		return nil, err
	}
	if _, err = s.storedInstance(ctx, r.GetName()); err != nil {
		if r.GetAllowMissing() && statusNotFound(err) {
			return doneOperation(project, r.GetName(), commonv1.OperationVerb_OPERATION_VERB_DELETE, r.GetRequestId(), nil)
		}
		return nil, err
	}
	in, err := compute.NewTerminateInstance(id)
	rc, err := s.submit(ctx, r.GetRequestId(), in, err, check(func() error {
		cur, err := s.storedInstance(ctx, r.GetName())
		if err != nil {
			return err
		}
		return etagUnlessTerminated(r.GetEtag(), cur)
	}))
	if err != nil {
		return nil, err
	}
	return s.operationPB(ctx, project, r.GetName(), rc.Operation)
}

func etagUnlessTerminated(got string, cur compute.Instance) error {
	if cur.Spec.Power == compute.PowerTerminated {
		return nil
	}
	return checkEtag(got, etagOf(cur.Spec.Generation, cur.Meta.Revision))
}

func (s *server) ReportInstanceStatus(ctx context.Context, r *computev1.ReportInstanceStatusRequest) (*computev1.ReportInstanceStatusResponse, error) {
	if err := checkRequestID(r.GetRequestId()); err != nil {
		return nil, err
	}
	if r.GetStatus() == nil || r.GetStatus().GetObserved() == nil {
		return nil, invalidSpec("status.observed is required")
	}
	// A converged delete is not a live instance, but its controller may still be
	// retrying the DELETING report that removed it.
	inst, err := s.storedInstance(ctx, r.GetName())
	if err != nil {
		return nil, err
	}
	name := r.GetName()
	obs := r.GetStatus().GetObserved()
	if inst.Removed() && (!inst.Spec.ControllerDriven() || obs.GetState() != commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING) {
		return nil, notFound("instance " + name + " not found")
	}
	if obs.GetGeneration() > inst.Spec.Generation {
		return nil, aborted("GENERATION_CONFLICT", "observed generation is ahead of the stored generation")
	}
	if !inst.Spec.ControllerDriven() {
		// The in-process reconciler commits status for this
		// instance. A report that is not ahead of the stored generation
		// changes nothing.
		return &computev1.ReportInstanceStatusResponse{Instance: instancePB(inst, name)}, nil
	}
	rep, err := parseInstanceReport(r.GetStatus())
	if err != nil {
		return nil, err
	}
	if inst.Removed() {
		return &computev1.ReportInstanceStatusResponse{Instance: instancePB(inst, name)}, nil
	}
	got, err := s.reports.ReportInstanceStatus(ctx, inst.Spec.ID, rep, s.now())
	if err != nil {
		if compute.CodeOf(err) == compute.CodeAborted {
			return nil, aborted("GENERATION_CONFLICT", err.Error())
		}
		return nil, toStatus(err)
	}
	return &computev1.ReportInstanceStatusResponse{Instance: instancePB(got, name)}, nil
}

func parseInstanceReport(st *computev1.InstanceStatus) (compute.InstanceReport, error) {
	obs := st.GetObserved()
	rep := compute.InstanceReport{Node: obs.GetNode(), Generation: obs.GetGeneration(), Message: obs.GetMessage()}
	switch obs.GetState() {
	case commonv1.ReconciliationState_RECONCILIATION_STATE_PENDING:
		rep.State = compute.ReportPending
	case commonv1.ReconciliationState_RECONCILIATION_STATE_RECONCILING:
		rep.State = compute.ReportReconciling
	case commonv1.ReconciliationState_RECONCILIATION_STATE_READY:
		rep.State = compute.ReportReady
	case commonv1.ReconciliationState_RECONCILIATION_STATE_DELETING:
		rep.State = compute.ReportDeleting
	case commonv1.ReconciliationState_RECONCILIATION_STATE_ERROR:
		rep.State = compute.ReportError
		for _, c := range obs.GetConditions() {
			if c.GetType() == commonv1.ConditionType_CONDITION_TYPE_READY {
				rep.Reason = compute.ParseLaunchReason(c.GetReason())
				break
			}
		}
	default:
		return compute.InstanceReport{}, invalidSpec("status.observed.state is required")
	}
	switch st.GetPower() {
	case computev1.InstancePower_INSTANCE_POWER_RUNNING:
		rep.Power = compute.PowerRunning
	case computev1.InstancePower_INSTANCE_POWER_STOPPED:
		rep.Power = compute.PowerStopped
	}
	for _, n := range st.GetNetworkInterfaces() {
		rep.Interfaces = append(rep.Interfaces, compute.ObservedInterface{
			DeviceName: n.GetDeviceName(), Subnet: n.GetSubnet(), PrivateIPv4: n.GetPrivateIpv4(), PublicIPv4: n.GetPublicIpv4(),
		})
	}
	return rep, nil
}

func (s *server) liveInstance(ctx context.Context, name string) (compute.Instance, string, error) {
	inst, err := s.storedInstance(ctx, name)
	if err != nil {
		return compute.Instance{}, name, err
	}
	if inst.Removed() || inst.Lifecycle() == compute.InstanceTerminated {
		return compute.Instance{}, name, notFound("instance " + name + " not found")
	}
	return inst, name, nil
}

// storedInstance reads the row named name, including one whose delete has
// converged. A controller-driven instance lives in the project and zone it was
// created in, so the same id under another parent is not found.
func (s *server) storedInstance(ctx context.Context, name string) (compute.Instance, error) {
	project, zone, id, err := parseInstanceName(name)
	if err != nil {
		return compute.Instance{}, err
	}
	iid, err := compute.ParseInstanceID(id)
	if err != nil {
		return compute.Instance{}, notFound("instance " + name + " not found")
	}
	inst, err := s.read.Instance(ctx, iid)
	if err != nil {
		return compute.Instance{}, toStatus(err)
	}
	if inst.Spec.ControllerDriven() && (inst.Spec.Project != project || inst.Spec.Zone != zone) {
		return compute.Instance{}, notFound("instance " + name + " not found")
	}
	return inst, nil
}

func validateCreateInstance(zone, name string, inst *computev1.Instance) error {
	if inst == nil || inst.GetSpec() == nil {
		return invalidSpec("instance.spec is required")
	}
	if inst.GetName() != "" && inst.GetName() != name {
		return invalidID("instance.name does not match parent and instance_id")
	}
	if inst.GetUid() != "" || inst.GetGeneration() != 0 || inst.GetEtag() != "" || inst.GetStatus() != nil ||
		inst.GetCreateTime() != nil || inst.GetUpdateTime() != nil || inst.GetDeleteTime() != nil {
		return invalidSpec("output-only fields must be unset")
	}
	if _, err := compute.ParseLabels(inst.GetLabels()); err != nil {
		return toStatus(err)
	}
	spec := inst.GetSpec()
	if !machineTypePattern.MatchString(spec.GetMachineType()) {
		return invalidSpec("instance.spec.machine_type is invalid")
	}
	if err := knownMachineType(spec.GetMachineType()); err != nil {
		return err
	}
	if spec.GetPower() != computev1.InstancePower_INSTANCE_POWER_RUNNING && spec.GetPower() != computev1.InstancePower_INSTANCE_POWER_STOPPED {
		return invalidSpec("instance.spec.power must be RUNNING or STOPPED")
	}
	if spec.GetKeyPair() != "" {
		if _, _, _, err := parseKeyPairName(spec.GetKeyPair()); err != nil {
			return err
		}
	}
	if len(spec.GetUserData()) > 32768 {
		return invalidSpec("instance.spec.user_data exceeds 32768 bytes")
	}
	if err := validateNICs(spec.GetNetworkInterfaces()); err != nil {
		return err
	}
	return validateVolumes(zone, spec.GetVolumes())
}

func validateNICs(nics []*computev1.NetworkInterface) error {
	seen := map[string]bool{}
	for _, nic := range nics {
		if nic.GetDeviceName() == "" || seen[nic.GetDeviceName()] {
			return invalidSpec("network interface device names must be unique and non-empty")
		}
		seen[nic.GetDeviceName()] = true
		if err := parseSubnetName(nic.GetSubnet()); err != nil {
			return err
		}
		for _, g := range nic.GetSecurityGroups() {
			if err := parseSecurityGroupName(g); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateVolumes(zone string, vols []*computev1.VolumeAttachment) error {
	seen := map[string]bool{}
	boot := 0
	for _, v := range vols {
		if v.GetDeviceName() == "" || seen[v.GetDeviceName()] {
			return invalidSpec("volume device names must be unique and non-empty")
		}
		seen[v.GetDeviceName()] = true
		_, volZone, _, err := parseVolumeName(v.GetVolume())
		if err != nil {
			return err
		}
		if volZone != zone {
			return invalidSpec("volume " + v.GetVolume() + " is not in the instance zone")
		}
		if v.GetBoot() {
			boot++
		}
	}
	if boot != 1 {
		return invalidSpec("volumes must include exactly one boot entry")
	}
	return nil
}

func instanceSpecDelta(spec *computev1.InstanceSpec, cur compute.Instance) error {
	if spec.GetMachineType() != "" && spec.GetMachineType() != cur.Spec.Type.Name() {
		if err := knownMachineType(spec.GetMachineType()); err != nil {
			return err
		}
		return invalidSpec("machine_type is immutable")
	}
	key := ""
	if cur.Spec.Key != nil {
		key = cur.Spec.Key.Name.String()
	}
	if spec.GetKeyPair() != key && spec.GetKeyPair() != "" {
		return invalidSpec("key_pair is immutable")
	}
	if spec.GetUserData() != "" || len(spec.GetNetworkInterfaces()) > 0 {
		return invalidSpec("network interfaces and user_data are not mutable")
	}
	return nil
}

// knownMachineType rejects a machine type outside the catalog. Storing one
// would give the machine 0 vCPU and 0 memory, which no agent can create.
func knownMachineType(name string) error {
	if _, err := compute.ParseInstanceType(name); err != nil {
		return invalidSpec("instance.spec.machine_type " + strconv.Quote(name) + " is not a supported machine type")
	}
	return nil
}
