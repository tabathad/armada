package repository

import (
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/go-redis/redis"

	legacyrepository "github.com/armadaproject/armada/internal/armada/repository"
)

// QueueRepository is an interface to be implemented by structs which provide queue information
type QueueRepository interface {
	GetAllQueues() ([]*database.Queue, error)
}

// LegacyQueueRepository is a QueueRepository which is backed by Armada's redis store
type LegacyQueueRepository struct {
	backingRepo legacyrepository.QueueRepository
}

func NewLegacyQueueRepository(db redis.UniversalClient) *LegacyQueueRepository {
	return &LegacyQueueRepository{
		backingRepo: legacyrepository.NewRedisQueueRepository(db),
	}
}

func (r *LegacyQueueRepository) GetAllQueues() ([]*database.Queue, error) {
	legacyQueues, err := r.backingRepo.GetAllQueues()
	if err != nil {
		return nil, err
	}
	queues := make([]*database.Queue, len(legacyQueues))
	for i, legacyQueue := range legacyQueues {
		queues[i] = &database.Queue{
			Name:   legacyQueue.Name,
			Weight: float64(legacyQueue.PriorityFactor),
		}
	}
	return queues, nil
}
