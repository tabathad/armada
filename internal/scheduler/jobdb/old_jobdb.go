package jobdb

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
)

const (
	jobsTable       = "jobs"
	runsByJobTable  = "runsByJob" // table that maps runs to jobs
	idIndex         = "id"        // index for looking up jobs by id
	orderIndex      = "order"     // index for looking up jobs on a given queue by the order in which they should be scheduled
	jobIdIndex      = "jobId"     // index for looking up jobs by id
	queuedJobsIndex = "queued"    // index for looking up queued/non-queued jobs
)

type Identifiable interface {
	DbId(key string) []byte
}

func (job *Job) DbId(key string) []byte {
	if key != "id" {
		panic(fmt.Sprintf("expected id 'key', but got %s", key))
	}
	return job.byteId
}

func (run *JobRun) DbId(key string) []byte {
	if key == "runId" {
		return run.runId
	} else if key == "jobId" {
		return run.jobId
	} else {
		panic(fmt.Sprintf("expected id 'runId' or 'jobId', but got %s", key))
	}
}

func (run *jobRunPair) DbId(key string) []byte {
	if key == "runId" {
		return run.byteRunId
	} else if key == "jobId" {
		return run.byteJobId
	} else {
		panic(fmt.Sprintf("expected id 'runId' or 'jobId', but got %s", key))
	}
}

type DbIdIndex struct {
	Key string
}

// FromArgs computes the index key from a set of arguments.
// Takes a single argument resourceAmount of type resource.Quantity.
func (index *DbIdIndex) FromArgs(args ...any) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("must provide exactly one argument")
	}
	key := args[0].([]byte)
	return key, nil
}

// FromObject extracts the index key from a ByteObject.
func (index *DbIdIndex) FromObject(raw any) (bool, []byte, error) {
	obj, ok := raw.(Identifiable)
	if !ok {
		return false, nil, errors.Errorf("expected *Job, but got %T", raw)
	}
	return true, obj.DbId(index.Key), nil
}

type OldJobDb struct {
	// In-memory database. Stores *Job.
	// Used to efficiently iterate over jobs in sorted order.
	Db *memdb.MemDB
}

// Internal row which allows us to map jobs to job runs (and vice versa)
type jobRunPair struct {
	runId string
	jobId string
	// []byte representation of the run id.
	// Used for efficient go-memdb lookup.
	byteRunId []byte
	// []byte representation of the job id.
	// Used for efficient go-memdb lookup.
	byteJobId []byte
}

func NewOldJobDb() (*OldJobDb, error) {
	db, err := memdb.NewMemDB(jobDbSchema())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &OldJobDb{
		Db: db,
	}, nil
}

// Upsert will insert the given jobs if they don't already exist or update them if they do
func (jobDb *OldJobDb) Upsert(txn *memdb.Txn, jobs []*Job) error {
	for _, job := range jobs {
		if err := txn.Insert(jobsTable, job); err != nil {
			return errors.WithStack(err)
		}
		for _, jobRunPair := range job.jobRunPairs {
			if err := txn.Insert(runsByJobTable, jobRunPair); err != nil {
				return errors.WithStack(err)
			}
		}

		// for _, run := range job.runsById {
		// 	err := txn.Insert(runsByJobTable, &jobRunPair{
		// 		runId: run.id.String(),
		// 		jobId: job.id,
		// 	})
		// 	if err != nil {
		// 		return errors.WithStack(err)
		// 	}
		// }
	}
	return nil
}

// GetById returns the job with the given Id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *OldJobDb) GetById(txn *memdb.Txn, id string) (*Job, error) {
	var job *Job = nil
	iter, err := txn.Get(jobsTable, idIndex, id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := iter.Next()
	if result != nil {
		job = result.(*Job)
	}
	return job, err
}

// GetByRunId returns the job with the given run id or nil if no such job exists
// The Job returned by this function *must not* be subsequently modified
func (jobDb *OldJobDb) GetByRunId(txn *memdb.Txn, runId uuid.UUID) (*Job, error) {
	iter, err := txn.Get(runsByJobTable, idIndex, runId.String())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := iter.Next()
	if result == nil {
		return nil, nil
	}
	job := result.(*jobRunPair)
	return jobDb.GetById(txn, job.jobId)
}

// GetAll returns all jobs in the database.
// The Jobs returned by this function *must not* be subsequently modified
func (jobDb *OldJobDb) GetAll(txn *memdb.Txn) ([]*Job, error) {
	iter, err := txn.Get(jobsTable, idIndex)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result := make([]*Job, 0)
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		p := obj.(*Job)
		result = append(result, p)
	}
	return result, nil
}

// BatchDelete removes the jobs with the given ids from the database.  Any ids that are not in the database will be
// ignored
func (jobDb *OldJobDb) BatchDelete(txn *memdb.Txn, ids []string) error {
	for _, id := range ids {
		err := txn.Delete(jobsTable, &Job{id: id})
		if err != nil {
			// this could be because the job doesn't exist
			// unfortunately the error from memdb isn't nice for parsing, so we do an explicit check
			job, err := jobDb.GetById(txn, id)
			if err != nil {
				return err
			}
			if job != nil {
				return errors.WithStack(err)
			}
		}
		runsIter, err := txn.Get(runsByJobTable, jobIdIndex, id)
		if err != nil {
			return errors.WithStack(err)
		}
		for obj := runsIter.Next(); obj != nil; obj = runsIter.Next() {
			err = txn.Delete(runsByJobTable, obj)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// ReadTxn returns a read-only transaction.
// Multiple read-only transactions can access the db concurrently
func (jobDb *OldJobDb) ReadTxn() *memdb.Txn {
	return jobDb.Db.Txn(false)
}

// WriteTxn returns a writeable transaction.
// Only a single write transaction may access the db at any given time
func (jobDb *OldJobDb) WriteTxn() *memdb.Txn {
	return jobDb.Db.Txn(true)
}

// jobDbSchema() creates the database schema.
// This is a simple schema consisting of a single "jobs" table with indexes for fast lookups
func jobDbSchema() *memdb.DBSchema {
	jobIndexes := map[string]*memdb.IndexSchema{
		idIndex: {
			Name:    idIndex, // lookup by primary key
			Unique:  true,
			Indexer: &DbIdIndex{Key: "id"},
		},
		// idIndex: {
		// 	Name:    idIndex, // lookup by primary key
		// 	Unique:  true,
		// 	Indexer: &memdb.StringFieldIndex{Field: "id"},
		// },

		orderIndex: {
			Name:   orderIndex, // lookup queued jobs for a given queue
			Unique: false,
			Indexer: &memdb.CompoundIndex{
				Indexes: []memdb.Indexer{
					&memdb.StringFieldIndex{Field: "queue"},
					&memdb.BoolFieldIndex{Field: "queued"},
					&memdb.UintFieldIndex{Field: "priority"},
					&memdb.IntFieldIndex{Field: "created"},
				},
			},
		},

		// queuedJobsIndex: {
		// 	Name:   queuedJobsIndex, // lookup queued/leased jobs globally
		// 	Unique: false,
		// 	Indexer: &memdb.CompoundIndex{
		// 		Indexes: []memdb.Indexer{
		// 			&memdb.BoolFieldIndex{Field: "queued"},
		// 		},
		// 	},
		// },
	}

	runsByJobIndexes := map[string]*memdb.IndexSchema{
		// idIndex: {
		// 	Name:    idIndex, // lookup by primary key
		// 	Unique:  true,
		// 	Indexer: &memdb.StringFieldIndex{Field: "runId"},
		// },
		// jobIdIndex: {
		// 	Name:    jobIdIndex, // lookup by job id
		// 	Unique:  true,
		// 	Indexer: &memdb.StringFieldIndex{Field: "jobId"},
		// },
		idIndex: {
			Name:    idIndex, // lookup by primary key
			Unique:  true,
			Indexer: &DbIdIndex{Key: "runId"},
		},
		jobIdIndex: {
			Name:    jobIdIndex, // lookup by job id
			Unique:  true,
			Indexer: &DbIdIndex{Key: "jobId"},
		},
	}

	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			jobsTable: {
				Name:    jobsTable,
				Indexes: jobIndexes,
			},
			runsByJobTable: {
				Name:    runsByJobTable,
				Indexes: runsByJobIndexes,
			},
		},
	}
}
