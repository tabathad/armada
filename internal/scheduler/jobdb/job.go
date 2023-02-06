package jobdb

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
)

// Job is the scheduler-internal representation of a job.
type Job struct {
	// String representation of the job id
	JobId string
	// Name of the queue this job belongs to.
	Queue string
	// Jobset the job belongs to
	// We store this as it's needed for sending job event messages
	Jobset string
	// Per-queue priority of this job.
	Priority uint32
	// Logical timestamp indicating the order in which jobs are submitted.
	// Jobs with identical Queue and Priority
	// are sorted by timestamp.
	Timestamp int64
	// True if the job is currently queued.
	// If this is set then the job will not be considered for scheduling
	Queued bool
	// Scheduling requirements of this job.
	jobSchedulingInfo *schedulerobjects.JobSchedulingInfo
	// True if the user has requested this job be cancelled
	CancelRequested bool
	// True if the scheduler has cancelled the job
	Cancelled bool
	// True if the scheduler has failed the job
	Failed bool
	// True if the scheduler has marked the job as succeeded
	Succeeded bool
	// Job Runs in the order they were received.
	// For now there can be only one active job run which will be the last element of the slice
	Runs []*Run
}

func (job *Job) GetRequirements(_ map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo {
	return job.jobSchedulingInfo
}

// GetQueue returns the queue this job belongs to.
func (job *Job) GetQueue() string {
	return job.Queue
}

// GetAnnotations returns the annotations on the job.
func (job *Job) GetAnnotations() map[string]string {
	requirements := job.jobSchedulingInfo.GetObjectRequirements()
	if len(requirements) == 0 {
		return nil
	}
	if podReqs := requirements[0].GetPodRequirements(); podReqs != nil {
		return podReqs.GetAnnotations()
	}
	return nil
}

// GetId returns the id of the Job.
func (job *Job) GetId() string {
	return job.JobId
}

// InTerminalState returns true if the job  is in a terminal state
func (job *Job) InTerminalState() bool {
	return job.Succeeded || job.Cancelled || job.Failed
}

// NumReturned returns the number of times this job has been returned by executors
// Note that this is O(N) on Runs, but this should be fine as the number of runs should be small
func (job *Job) NumReturned() uint {
	returned := uint(0)
	for _, run := range job.Runs {
		if run.Returned {
			returned++
		}
	}
	return returned
}

// CurrentRun returns the currently active job run or nil if there are no runs yet
func (job *Job) CurrentRun() *Run {
	if len(job.Runs) == 0 {
		return nil
	}
	return job.Runs[len(job.Runs)-1]
}

// RunById returns the Run corresponding to the provided run id or nil if no such Run exists
// Note that this is O(N) on Runs, but this should be fine as the number of runs should be small
func (job *Job) RunById(id uuid.UUID) *Run {
	for _, run := range job.Runs {
		if run.RunID == id {
			return run
		}
	}
	return nil
}

// DeepCopy deep copies the entire job including the runs.
// This is needed because when jobs are stored in the JobDb they cannot be modified in-place
func (job *Job) DeepCopy() *Job {
	if job == nil {
		return nil
	}
	runs := make([]*Run, len(job.Runs))
	for k, v := range job.Runs {
		runs[k] = v.DeepCopy()
	}
	return &Job{
		JobId:             job.JobId,
		Queue:             job.Queue,
		Jobset:            job.Jobset,
		Priority:          job.Priority,
		Timestamp:         job.Timestamp,
		Queued:            job.Queued,
		jobSchedulingInfo: proto.Clone(job.jobSchedulingInfo).(*schedulerobjects.JobSchedulingInfo),
		CancelRequested:   job.CancelRequested,
		Cancelled:         job.Cancelled,
		Failed:            job.Failed,
		Succeeded:         job.Succeeded,
		Runs:              runs,
	}
}
