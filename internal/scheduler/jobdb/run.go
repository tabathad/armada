package jobdb

import "github.com/google/uuid"

// Run is the scheduler-internal representation of a job run.
type Run struct {
	// Unique identifier for the run
	RunID uuid.UUID
	// The name of the executor this run has been leased to
	Executor string
	// True if the job has been reported as running by the executor
	Running bool
	// True if the job has been reported as succeeded by the executor
	Succeeded bool
	// True if the job has been reported as failed by the executor
	Failed bool
	// True if the job has been reported as cancelled by the executor
	Cancelled bool
	// True if the job has been returned by the executor
	Returned bool
	// True if the job has been expired by the scheduler
	Expired bool
}

// InTerminalState returns true if the Run is in a terminal state
func (run *Run) InTerminalState() bool {
	return run.Succeeded || run.Failed || run.Cancelled || run.Expired || run.Returned
}

// DeepCopy deep copies the entire Run
// This is needed because when runs are stored in the JobDb they cannot be modified in-place
func (run *Run) DeepCopy() *Run {
	return &Run{
		RunID:     run.RunID,
		Executor:  run.Executor,
		Running:   run.Running,
		Succeeded: run.Succeeded,
		Failed:    run.Failed,
		Cancelled: run.Cancelled,
		Returned:  run.Returned,
		Expired:   run.Expired,
	}
}
