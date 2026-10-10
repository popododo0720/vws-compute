// Package compute is the domain model of the VWS compute control plane.
//
// It owns the validated value types, the desired specs and observed statuses
// of instances, images, and key pairs, the intents a caller can submit, the
// pure planner that turns an intent into a desired-state change, and the
// operation state machine.
//
// The model is level-triggered. Every instance and image carries a
// Generation that advances on each desired change, and an observed
// generation that the reconciler advances when it has finished converging
// (or has given up on) that generation. Work is the gap between the two. An
// Operation tracks exactly one (resource, generation) pair and leaves
// Pending only through Settle. Nothing else drives progress, which is why
// restart recovery needs nothing but the repository.
//
// The package performs no I/O and never reads the clock or the process
// random source for decisions. Time and randomness arrive through Env.
package compute
