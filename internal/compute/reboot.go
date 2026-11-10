package compute

import "math"

type rebootInstance struct {
	instance InstanceID
	name     string
}

func NewRebootInstance(id, name string) (Intent, error) {
	parsed, err := ParseInstanceID(id)
	if err != nil {
		return nil, err
	}
	return rebootInstance{instance: parsed, name: name}, nil
}

func (rebootInstance) Kind() OperationKind   { return OpRebootInstance }
func (rebootInstance) carriesMetadata() bool { return false }
func (in rebootInstance) encode(e *encoder)  { e.field(in.name) }

func (in rebootInstance) plan(v View, env Env) (Change, error) {
	inst, err := v.Instance(in.instance)
	if err != nil {
		return Change{}, err
	}
	next := inst.Spec
	if next.Power != PowerRunning {
		return Change{}, Errorf(CodeIncorrectState, "instance %s is not running", in.instance)
	}
	if next.RebootEpoch == math.MaxInt64 || next.Generation == math.MaxInt64 {
		return Change{}, Errorf(CodeLimitExceeded, "instance reboot counter exhausted")
	}
	next.RebootEpoch++
	next.Generation++
	next.ChangedAt = env.Now
	return Change{Spec: next, Write: true}, nil
}
