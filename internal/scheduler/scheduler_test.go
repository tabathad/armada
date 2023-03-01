package scheduler

import (
	"context"
	"fmt"
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/disgoorg/log"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	dbcommon "github.com/armadaproject/armada/internal/common/database"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Data to be used in tests
const maxLeaseReturns = 1

var (
	schedulingInfo      = &schedulerobjects.JobSchedulingInfo{AtMostOnce: true}
	schedulingInfoBytes = protoutil.MustMarshall(schedulingInfo)
)

var queuedJob = jobdb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	false,
	false,
	false,
	1)

var leasedJob = jobdb.NewJob(
	util.NewULID(),
	"testJobset",
	"testQueue",
	uint32(10),
	schedulingInfo,
	false,
	false,
	false,
	1).WithQueued(false).WithNewRun("testExecutor", "test-node")

// Test a single scheduler cycle
func TestScheduler_TestCycle_Perf(t *testing.T) {
	tests := map[string]struct {
	}{
		"Perf": {},
	}
	for name, _ := range tests {
		t.Run(name, func(t *testing.T) {
			clusterTimeout := 1 * time.Hour

			// Test objects
			pool, err := dbcommon.OpenPgxPool(configuration.PostgresConfig{
				Connection: map[string]string{
					"host":     "localhost",
					"port":     "5432",
					"user":     "postgres",
					"password": "psw",
					"dbname":   "postgres",
					"sslmode":  "disable",
				},
			})
			require.NoError(t, err)
			jobRepo := database.NewPostgresJobRepository(pool, 50000)
			testClock := clock.NewFakeClock(time.Now())
			schedulingAlgo := &testSchedulingAlgo{jobsToSchedule: []string{}, shouldError: false}
			publisher := &testPublisher{shouldError: false}
			stringInterner, err := util.NewStringInterner(100000)
			require.NoError(t, err)

			heartbeatTime := testClock.Now()
			clusterRepo := &testExecutorRepository{
				updateTimes: map[string]time.Time{"testExecutor": heartbeatTime},
			}
			sched, err := NewScheduler(
				jobRepo,
				clusterRepo,
				schedulingAlgo,
				NewStandaloneLeaderController(),
				publisher,
				stringInterner,
				1*time.Second,
				clusterTimeout,
				maxLeaseReturns)
			require.NoError(t, err)

			sched.clock = testClock

			// run a scheduler cycle
			log.Infof("starting cycle")
			err = sched.cycle(context.Background(), false, sched.leaderController.GetToken())
			log.Infof("finished cycle")
		})
	}
}

// Test running multiple scheduler cycles
func TestRun(t *testing.T) {
	// Test objects
	jobRepo := testJobRepository{numReceivedPartitions: 100}
	testClock := clock.NewFakeClock(time.Now())
	schedulingAlgo := &testSchedulingAlgo{}
	publisher := &testPublisher{}
	clusterRepo := &testExecutorRepository{}
	leaderController := NewStandaloneLeaderController()
	stringInterner, err := util.NewStringInterner(100)
	require.NoError(t, err)

	sched, err := NewScheduler(
		&jobRepo,
		clusterRepo,
		schedulingAlgo,
		leaderController,
		publisher,
		stringInterner,
		1*time.Second,
		1*time.Hour,
		maxLeaseReturns)
	require.NoError(t, err)

	sched.clock = testClock

	ctx, cancel := context.WithCancel(context.Background())

	//nolint:errcheck
	go sched.Run(ctx)

	time.Sleep(1 * time.Second)

	// Function that runs a cycle and waits until it sees published messages
	fireCycle := func() {
		publisher.Reset()
		wg := sync.WaitGroup{}
		wg.Add(1)
		sched.onCycleCompleted = func() { wg.Done() }
		jobId := util.NewULID()
		jobRepo.updatedJobs = []database.Job{{JobID: jobId, Queue: "testQueue"}}
		schedulingAlgo.jobsToSchedule = []string{jobId}
		testClock.Step(10 * time.Second)
		wg.Wait()
	}

	// fire a cycle and assert that we became leader and published
	fireCycle()
	assert.Equal(t, 1, len(publisher.events))

	// invalidate our leadership: we should not publish
	leaderController.token = InvalidLeaderToken()
	fireCycle()
	assert.Equal(t, 0, len(publisher.events))

	// become master again: we should publish
	leaderController.token = NewLeaderToken()
	fireCycle()
	assert.Equal(t, 1, len(publisher.events))

	cancel()
}
func TestScheduler_TestSyncState(t *testing.T) {
	tests := map[string]struct {
		initialJobs         []*jobdb.Job   // jobs in the jobdb at the start of the cycle
		jobUpdates          []database.Job // job updates from the database
		runUpdates          []database.Run // run updates from the database
		expectedUpdatedJobs []*jobdb.Job
		expectedJobDbIds    []string
	}{
		"insert job": {
			jobUpdates: []database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         queuedJob.Jobset(),
					Queue:          queuedJob.Queue(),
					Submitted:      queuedJob.Created(),
					Priority:       int64(queuedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Serial:         1,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{queuedJob},
			expectedJobDbIds:    []string{queuedJob.Id()},
		},
		"insert job that already exists": {
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         queuedJob.Jobset(),
					Queue:          queuedJob.Queue(),
					Submitted:      queuedJob.Created(),
					Priority:       int64(queuedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Serial:         1,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{queuedJob},
			expectedJobDbIds:    []string{queuedJob.Id()},
		},
		"add job run": {
			initialJobs: []*jobdb.Job{queuedJob},
			runUpdates: []database.Run{
				{
					RunID:    uuid.UUID{},
					JobID:    queuedJob.Id(),
					JobSet:   queuedJob.Jobset(),
					Executor: "test-executor",
					Node:     "test-node",
					Created:  123,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{
				queuedJob.WithUpdatedRun(
					jobdb.CreateRun(
						uuid.UUID{},
						123,
						"test-executor",
						"test-node",
						false,
						false,
						false,
						false,
						false)).
					WithQueued(false),
			},
			expectedJobDbIds: []string{queuedJob.Id()},
		},
		"job succeeded": {
			initialJobs: []*jobdb.Job{queuedJob},
			jobUpdates: []database.Job{
				{
					JobID:          queuedJob.Id(),
					JobSet:         queuedJob.Jobset(),
					Queue:          queuedJob.Queue(),
					Submitted:      queuedJob.Created(),
					Priority:       int64(queuedJob.Priority()),
					SchedulingInfo: schedulingInfoBytes,
					Succeeded:      true,
					Serial:         1,
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{},
			expectedJobDbIds:    []string{},
		},
		"lease returned": {
			initialJobs: []*jobdb.Job{leasedJob},
			runUpdates: []database.Run{
				{
					JobID:    leasedJob.Id(),
					JobSet:   leasedJob.Id(),
					RunID:    leasedJob.LatestRun().Id(),
					Failed:   true,
					Returned: true,
					Created:  leasedJob.LatestRun().Created(),
				},
			},
			expectedUpdatedJobs: []*jobdb.Job{
				leasedJob.
					WithUpdatedRun(leasedJob.LatestRun().WithReturned(true)).
					WithQueued(true),
			},
			expectedJobDbIds: []string{leasedJob.Id()},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Test objects
			// Test objects
			jobRepo := &testJobRepository{
				updatedJobs: tc.jobUpdates,
				updatedRuns: tc.runUpdates,
			}
			schedulingAlgo := &testSchedulingAlgo{}
			publisher := &testPublisher{}
			clusterRepo := &testExecutorRepository{}
			leaderController := NewStandaloneLeaderController()
			stringInterner, err := util.NewStringInterner(100)
			require.NoError(t, err)

			sched, err := NewScheduler(
				jobRepo,
				clusterRepo,
				schedulingAlgo,
				leaderController,
				publisher,
				stringInterner,
				1*time.Second,
				1*time.Hour,
				maxLeaseReturns)
			require.NoError(t, err)

			// insert initial jobs
			txn := sched.jobDb.WriteTxn()
			err = sched.jobDb.Upsert(txn, tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			updatedJobs, err := sched.syncState(ctx)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedUpdatedJobs, updatedJobs)
			allDbJobs := sched.jobDb.GetAll(sched.jobDb.ReadTxn())

			expectedIds := stringSet(tc.expectedJobDbIds)
			require.Equal(t, len(tc.expectedJobDbIds), len(allDbJobs))
			for _, job := range allDbJobs {
				_, ok := expectedIds[job.Id()]
				assert.True(t, ok)
			}
		})
	}
}

// Test implementations of the interfaces needed by the Scheduler
type testJobRepository struct {
	updatedJobs           []database.Job
	updatedRuns           []database.Run
	errors                map[uuid.UUID]*armadaevents.Error
	shouldError           bool
	numReceivedPartitions uint32
}

func (t *testJobRepository) FindInactiveRuns(ctx context.Context, runIds []uuid.UUID) ([]uuid.UUID, error) {
	// TODO implement me
	panic("implement me")
}

func (t *testJobRepository) FetchJobRunLeases(ctx context.Context, executor string, maxResults uint, excludedRunIds []uuid.UUID) ([]*database.JobRunLease, error) {
	// TODO implement me
	panic("implement me")
}

func (t *testJobRepository) FetchJobUpdates(ctx context.Context, jobSerial int64, jobRunSerial int64) ([]database.Job, []database.Run, error) {
	if t.shouldError {
		return nil, nil, errors.New("error fetchiung job updates")
	}
	return t.updatedJobs, t.updatedRuns, nil
}

func (t *testJobRepository) FetchJobRunErrors(ctx context.Context, runIds []uuid.UUID) (map[uuid.UUID]*armadaevents.Error, error) {
	if t.shouldError {
		return nil, errors.New("error fetching job run errors")
	}
	return t.errors, nil
}

func (t *testJobRepository) CountReceivedPartitions(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	if t.shouldError {
		return 0, errors.New("error counting received partitions")
	}
	return t.numReceivedPartitions, nil
}

type testExecutorRepository struct {
	updateTimes map[string]time.Time
	shouldError bool
}

func (t testExecutorRepository) GetExecutors(ctx context.Context) ([]*schedulerobjects.Executor, error) {
	panic("implement me")
}

func (t testExecutorRepository) GetLastUpdateTimes(ctx context.Context) (map[string]time.Time, error) {
	if t.shouldError {
		return nil, errors.New("error getting last update time")
	}
	return t.updateTimes, nil
}

func (t testExecutorRepository) StoreExecutor(ctx context.Context, executor *schedulerobjects.Executor) error {
	panic("implement me")
}

type testSchedulingAlgo struct {
	jobsToSchedule []string
	shouldError    bool
}

func (t *testSchedulingAlgo) Schedule(ctx context.Context, txn *jobdb.Txn, jobDb *jobdb.JobDb) ([]*jobdb.Job, error) {
	if t.shouldError {
		return nil, errors.New("error scheduling jobs")
	}
	jobs := make([]*jobdb.Job, 0, len(t.jobsToSchedule))
	for _, id := range t.jobsToSchedule {
		job := jobDb.GetById(txn, id)
		if job != nil {
			if !job.Queued() {
				return nil, errors.New(fmt.Sprintf("Was asked to lease %s but job was already leased", job.Id()))
			}
			job = job.WithQueued(false).WithNewRun("test-executor", "test-node")
			jobs = append(jobs, job)
		} else {
			return nil, errors.New(fmt.Sprintf("Was asked to lease %s but job does not exist", job.Id()))
		}
	}
	if len(jobs) > 0 {
		err := jobDb.Upsert(txn, jobs)
		if err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

type testPublisher struct {
	events      []*armadaevents.EventSequence
	shouldError bool
}

func (t *testPublisher) PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, _ func() bool) error {
	t.events = events
	if t.shouldError {
		return errors.New("Error when publishing")
	}
	return nil
}

func (t *testPublisher) Reset() {
	t.events = nil
}

func (t *testPublisher) PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	return 100, nil
}

func stringSet(src []string) map[string]bool {
	set := make(map[string]bool, len(src))
	for _, s := range src {
		set[s] = true
	}
	return set
}
