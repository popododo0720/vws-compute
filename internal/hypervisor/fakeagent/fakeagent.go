// Package fakeagent is an in-memory hypervisor.Agent for tests and for running
// the service without a hypervisor.
//
// It keeps the agent contract: converging is idempotent, a machine is created
// only when absent, booting applies the reboot epoch, and a powered-on machine
// reboots once per epoch it falls behind. Every call is counted on entry, so
// tests can prove a code path never reached the agent.
package fakeagent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/popododo0720/vws-compute/internal/hypervisor"
)

var _ hypervisor.Agent = (*Agent)(nil)

// ImageSize is the size every fake image artifact reports.
const ImageSize int64 = 2 << 30

// Machine is a snapshot of one fake machine.
type Machine struct {
	Image          string
	VCPUs          int
	MemoryMiB      int
	AuthorizedKeys []string
	// Power is PowerOff or PowerOn. Destroyed machines are not returned.
	Power        hypervisor.Power
	AppliedEpoch uint32
}

// Agent is safe for concurrent use. The zero value is not usable. Call New.
type Agent struct {
	mu        sync.Mutex
	machines  map[string]Machine
	images    map[string]int64
	creations map[string]int
	reboots   map[string]int
	failures  map[string][]error
	calls     int
	gate      chan struct{}
}

func New() *Agent {
	return &Agent{
		machines:  map[string]Machine{},
		images:    map[string]int64{},
		creations: map[string]int{},
		reboots:   map[string]int{},
		failures:  map[string][]error{},
	}
}

// Block makes every later call wait until release is called or the call's
// context ends. Calls still count on entry.
func (a *Agent) Block() (release func()) {
	gate := make(chan struct{})
	a.mu.Lock()
	a.gate = gate
	a.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			if a.gate == gate {
				a.gate = nil
			}
			a.mu.Unlock()
			close(gate)
		})
	}
}

// Fail queues errs for the next calls that converge name, one error per call.
// Wrap an error with hypervisor.Permanent to make it permanent.
func (a *Agent) Fail(name string, errs ...error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failures[name] = append(a.failures[name], errs...)
}

// Calls is the number of Converge calls made so far.
func (a *Agent) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// Creations is how many times machine name was created. Destroying the
// machine does not reset it.
func (a *Agent) Creations(name string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.creations[name]
}

// Reboots is how many reboots machine name went through while powered on.
func (a *Agent) Reboots(name string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reboots[name]
}

// Machine returns machine name, or false if it does not exist.
func (a *Agent) Machine(name string) (Machine, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	m, ok := a.machines[name]
	m.AuthorizedKeys = append([]string(nil), m.AuthorizedKeys...)
	return m, ok
}

// HasImage reports whether image artifact name exists.
func (a *Agent) HasImage(name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.images[name]
	return ok
}

// enter counts the call, waits out any Block, and pops a queued failure. The
// caller holds a.mu when enter returns nil.
func (a *Agent) enter(ctx context.Context, name string) error {
	a.mu.Lock()
	a.calls++
	for a.gate != nil {
		gate := a.gate
		a.mu.Unlock()
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
		a.mu.Lock()
	}
	if q := a.failures[name]; len(q) > 0 {
		a.failures[name] = q[1:]
		a.mu.Unlock()
		return q[0]
	}
	return nil
}

func (a *Agent) ConvergeMachine(ctx context.Context, spec hypervisor.MachineSpec) error {
	if err := a.enter(ctx, spec.Name); err != nil {
		return err
	}
	defer a.mu.Unlock()

	if spec.Power == hypervisor.PowerAbsent {
		delete(a.machines, spec.Name)
		return nil
	}
	if spec.Power != hypervisor.PowerOff && spec.Power != hypervisor.PowerOn {
		return hypervisor.Permanent(fmt.Errorf("machine %s: invalid power %d", spec.Name, spec.Power))
	}
	m, exists := a.machines[spec.Name]
	if !exists {
		if _, ok := a.images[spec.Image]; !ok {
			return hypervisor.Permanent(fmt.Errorf("machine %s: image artifact %s not found", spec.Name, spec.Image))
		}
		m = Machine{Image: spec.Image, Power: hypervisor.PowerOff}
		a.creations[spec.Name]++
	}
	m.VCPUs, m.MemoryMiB = spec.VCPUs, spec.MemoryMiB
	m.AuthorizedKeys = append([]string(nil), spec.AuthorizedKeys...)
	switch {
	case spec.Power == hypervisor.PowerOff:
		m.Power = hypervisor.PowerOff
	case m.Power == hypervisor.PowerOff:
		m.Power, m.AppliedEpoch = hypervisor.PowerOn, spec.RebootEpoch
	case m.AppliedEpoch < spec.RebootEpoch:
		m.AppliedEpoch = spec.RebootEpoch
		a.reboots[spec.Name]++
	}
	a.machines[spec.Name] = m
	return nil
}

func (a *Agent) ConvergeImage(ctx context.Context, spec hypervisor.ImageSpec) (int64, error) {
	if err := a.enter(ctx, spec.Name); err != nil {
		return 0, err
	}
	defer a.mu.Unlock()

	if !spec.Present {
		delete(a.images, spec.Name)
		return 0, nil
	}
	if size, ok := a.images[spec.Name]; ok {
		return size, nil
	}
	switch {
	case spec.SourceURL != "" && spec.SourceMachine == "":
	case spec.SourceMachine != "" && spec.SourceURL == "":
		if _, ok := a.machines[spec.SourceMachine]; !ok {
			return 0, fmt.Errorf("image %s: source machine %s does not exist yet", spec.Name, spec.SourceMachine)
		}
	default:
		return 0, hypervisor.Permanent(errors.New("image " + spec.Name + ": need exactly one source"))
	}
	a.images[spec.Name] = ImageSize
	return ImageSize, nil
}
