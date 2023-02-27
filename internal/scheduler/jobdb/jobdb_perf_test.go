package jobdb

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/disgoorg/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestNewJobDBPerf(t *testing.T) {
	jobs := createJobs()
	db := NewJobDb()

	// Test how long it takes to insert new rows
	txn := db.WriteTxn()
	startInsert := time.Now()
	err := db.Upsert(txn, jobs)
	require.NoError(t, err)
	txn.Commit()
	log.Infof("Inserted in %s", time.Since(startInsert))

	// Test how long it takes to overwrite rows
	txn = db.WriteTxn()
	startInsert = time.Now()
	err = db.Upsert(txn, jobs)
	require.NoError(t, err)
	txn.Commit()
	log.Infof("overwritten in %s", time.Since(startInsert))
}

func TestOldJobDBPerf(t *testing.T) {
	jobs := createJobs()
	db, err := NewOldJobDb()
	require.NoError(t, err)

	// Test how long it takes to insert new rows
	txn := db.WriteTxn()
	startInsert := time.Now()
	err = db.Upsert(txn, jobs)
	require.NoError(t, err)
	txn.Commit()
	log.Infof("Inserted in %s", time.Since(startInsert))

	// Test how long it takes to overwrite rows
	txn = db.WriteTxn()
	startInsert = time.Now()
	err = db.Upsert(txn, jobs)
	require.NoError(t, err)
	txn.Commit()
	log.Infof("overwritten in %s", time.Since(startInsert))
}

func createJobs() []*Job {
	const numJobs = 500000
	const numQueues = 10
	jobs := make([]*Job, numJobs)
	for i := 0; i < numJobs; i++ {
		j := &Job{
			id:       util.NewULID(),
			queue:    fmt.Sprintf("batch-%d", rand.Intn(numQueues)),
			priority: uint32(rand.Intn(3)),
			created:  int64(i),
			queued:   true,
			runsById: map[uuid.UUID]*JobRun{},
		}
		jobs[i] = j.WithNewRun("testExecutor", "testNode")
	}
	return jobs
}
