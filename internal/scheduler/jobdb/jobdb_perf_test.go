package jobdb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/disgoorg/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Starting point.
// BenchmarkNewJobDb-20                   1        1 408 036 100 ns/op        1608772448 B/op  9968852 allocs/op
// BenchmarkOldJobDb-20                   1        7 314 230 900 ns/op        2375102464 B/op 62750602 allocs/op

// Remove orderIndex, queuedJobsIndex.
// BenchmarkOldJobDb-20                   1        4 374 883 200 ns/op        1571174208 B/op 35256985 allocs/op

// Change idIndex to return a cached []byte.
// BenchmarkOldJobDb-20                   1        3 957 000 900 ns/op        1461660080 B/op 30752414 allocs/op

// Remove the runs table from both old and new job db.
// BenchmarkNewJobDb-20                   1        1 076 779 400 ns/op        1570502160 B/op  9968847 allocs/op
// BenchmarkOldJobDb-20                   2          536 889 400 ns/op        269799752 B/op   5412040 allocs/op

// Add runs table back. Avoid creating jobRunPairs during upsert.
// BenchmarkOldJobDb-20                   1        3 938 754 200 ns/op        1422136976 B/op 29761558 allocs/op

// For jobRunPair, use cached []byte as indexes.
// BenchmarkOldJobDb-20                   1        3 410 861 000 ns/op        1156004752 B/op 21637698 allocs/op

// Re-enable orderIndex, queuedJobsIndex.
// BenchmarkOldJobDb-20                   1        6 499 492 400 ns/op        1961305704 B/op 49149685 allocs/op

// Remove the runs table.
// BenchmarkOldJobDb-20                   1        3 009 329 000 ns/op        1075550448 B/op 32930910 allocs/op

// Add back the runs table. Remove queuedJobsIndex.
// BenchmarkOldJobDb-20                   1        5 344 117 700 ns/op        1636747600 B/op 39743925 allocs/op

// Remove orderIndex.
// BenchmarkOldJobDb-20                   1        3 554 851 300 ns/op        1156470848 B/op 21650188 allocs/op

// Add orderIndex back. Double number of jobs.
// BenchmarkNewJobDb-20                   1        2 532 365 900 ns/op        3780879112 B/op 19937632 allocs/op
// BenchmarkOldJobDb-20                   1        14 659 410 900 ns/op       3413306800 B/op 81316160 allocs/op

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

func BenchmarkNewJobDb(b *testing.B) {
	jobs := createJobs()
	db := NewJobDb()

	// Populate db.
	txn := db.WriteTxn()
	err := db.Upsert(txn, jobs)
	require.NoError(b, err)
	txn.Commit()

	// Benchmark upsert.
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.WriteTxn()
		err := db.Upsert(txn, jobs)
		require.NoError(b, err)
		txn.Commit()
	}
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

func BenchmarkOldJobDb(b *testing.B) {
	jobs := createJobs()
	db, err := NewOldJobDb()
	require.NoError(b, err)

	// Populate db.
	txn := db.WriteTxn()
	err = db.Upsert(txn, jobs)
	require.NoError(b, err)
	txn.Commit()

	// Benchmark upsert.
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.WriteTxn()
		err := db.Upsert(txn, jobs)
		require.NoError(b, err)
		txn.Commit()
	}
}

func createJobs() []*Job {
	// const numJobs = 500000
	const numJobs = 1000000
	const numQueues = 100
	jobs := make([]*Job, numJobs)
	for i := 0; i < numJobs; i++ {
		id := util.NewULID()
		j := &Job{
			id:       id,
			byteId:   []byte(id),
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
