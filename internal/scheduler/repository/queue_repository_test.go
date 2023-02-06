package repository

import (
	"github.com/armadaproject/armada/internal/scheduler/database"
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	clientQueue "github.com/armadaproject/armada/pkg/client/queue"
)

func TestLegacyQueueRepository_GetAllQueues(t *testing.T) {
	tests := map[string]struct {
		queues         []clientQueue.Queue
		expectedQueues []*database.Queue
	}{
		"Not empty": {
			queues: []clientQueue.Queue{
				{
					Name:           "test-queue-1",
					PriorityFactor: 10,
				},
				{
					Name:           "test-queue-2",
					PriorityFactor: 20,
				},
			},
			expectedQueues: []*database.Queue{
				{
					Name:   "test-queue-1",
					Weight: 10,
				},
				{
					Name:   "test-queue-2",
					Weight: 20,
				},
			},
		},
		"Empty": {
			queues:         []clientQueue.Queue{},
			expectedQueues: []*database.Queue{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rc := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
			rc.FlushDB()
			defer func() {
				rc.FlushDB()
				_ = rc.Close()
			}()
			repo := NewLegacyQueueRepository(rc)
			for _, queue := range tc.queues {
				err := repo.backingRepo.CreateQueue(queue)
				require.NoError(t, err)
			}
			retrievedQueues, err := repo.GetAllQueues()
			require.NoError(t, err)
			sortFunc := func(a, b *database.Queue) bool { return a.Name > b.Name }
			slices.SortFunc(tc.expectedQueues, sortFunc)
			slices.SortFunc(retrievedQueues, sortFunc)
			assert.Equal(t, tc.expectedQueues, retrievedQueues)
		})
	}
}
