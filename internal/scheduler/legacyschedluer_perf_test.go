package scheduler

import (
	ctx "context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/common/util"

	"github.com/google/uuid"

	v1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/resource"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/configuration"

	"github.com/go-redis/redis"

	"github.com/G-Research/armada/internal/armada/repository"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
)

func BenchmarkSchedule(b *testing.B) {

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		// 10 nodes
		nodes := make([]*schedulerobjects.Node, 0, 1000)
		for i := 0; i < 1000; i++ {
			node := &schedulerobjects.Node{
				Id: uuid.NewString(),
				NodeType: &schedulerobjects.NodeType{
					Id: "taintedCpu",
					Taints: []v1.Taint{
						{
							Key:    "armadaprojectio/batch",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    "armadaprojectio/cluster",
							Value:  "cluster1",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					Labels: map[string]string{
						"hostname": fmt.Sprintf("host-%d", i),
					},
				},
				NodeTypeId: "taintedCpu",
				TotalResources: schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("6"),
						"memory": resource.MustParse("24Gi"),
					},
				},
				AvailableByPriorityAndResource: schedulerobjects.NewAvailableByPriorityAndResourceType(
					[]int32{20000, 30000},
					map[string]resource.Quantity{
						"cpu":    resource.MustParse("6"),
						"memory": resource.MustParse("24Gi"),
					},
				),
			}
			usedCpu := rand.Int31n(6)
			if usedCpu > 6 {
				usedCpu = 6
			}
			schedulerobjects.AvailableByPriorityAndResourceType(node.AvailableByPriorityAndResource).MarkUsed(30000, schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse(fmt.Sprintf("%d", usedCpu)),
				},
			})
			nodes = append(nodes, node)
		}

		// 100 jobs
		jobs := make([]*api.Job, 0, 10000)
		for i := 0; i < 10000; i++ {
			job := &api.Job{
				Id:        util.NewULID(),
				JobSetId:  "test-perf",
				Queue:     "test-queue",
				Namespace: "test",
				Owner:     "test-user",
				Priority:  0,
				PodSpec: &v1.PodSpec{

					Volumes: nil,
					Containers: []v1.Container{
						{
							Name:  "container1",
							Image: "foo",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									"memory": resource.MustParse("1Gi"),
									"cpu":    resource.MustParse("1"),
								},
								Requests: v1.ResourceList{
									"memory": resource.MustParse("1Gi"),
									"cpu":    resource.MustParse("1"),
								},
							},
						},
					},
					Tolerations: []v1.Toleration{
						{
							Key:    "armadaprojectio/batch",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    "armadaprojectio/cluster",
							Value:  "cluster1",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					PriorityClassName: "armada-default",
					DNSConfig: &v1.PodDNSConfig{
						Nameservers: nil,
						Searches:    nil,
						Options:     nil,
					},
				},
				Created: time.Now(),
			}
			jobs = append(jobs, job)
		}

		priorityByQueue := map[string]float64{
			"test-queue": 100,
		}

		benchmarkSchedule(nodes, jobs, nil, priorityByQueue, b)
	}
}

func benchmarkSchedule(
	nodes []*schedulerobjects.Node,
	jobs []*api.Job,
	initialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType,
	priorityFactorByQueue map[string]float64,
	b *testing.B,
) {

	// Do setup
	withJobRepository(func(jobRepository *repository.RedisJobRepository) {
		// Add jobs to Redis (in batches of 100
		batch := make([]*api.Job, 0, 100)
		for _, job := range jobs {
			batch = append(batch, job)
			if len(batch)%100 == 0 {
				_, err := jobRepository.AddJobs(batch)
				if err != nil {
					panic(err)
				}
				batch = make([]*api.Job, 0, 100)
			}
		}
		if len(batch) > 0 {
			_, err := jobRepository.AddJobs(batch)
			if err != nil {
				panic(err)
			}
		}

		// Scheduling config
		cfg := configuration.SchedulingConfig{
			Preemption: configuration.PreemptionConfig{
				Enabled: true,
				PriorityClasses: map[string]int32{
					"armada-default":     30000,
					"armada-preemptible": 20000,
				},
				DefaultPriorityClass: "armada-default",
			},
			UseProbabilisticSchedulingForAllResources: false,
			QueueLeaseBatchSize:                       1000,
			MinimumResourceToSchedule: map[string]float64{
				"memory": 100000000,
				"cpu":    0.25,
			},
			MaximumLeasePayloadSizeBytes: 0,
			MaximalClusterFractionToSchedule: map[string]float64{
				"memory": 0.99,
				"cpu":    0.99,
			},
			MaximalResourceFractionToSchedulePerQueue: map[string]float64{
				"memory": 0.99,
				"cpu":    0.99,
			},
			MaximalResourceFractionPerQueue: map[string]float64{
				"memory": 0.75,
				"cpu":    0.75,
			},
			MaximumJobsToSchedule:          5000,
			ProbabilityOfUsingNewScheduler: 1.0,
			MaxQueueReportsToStore:         100,
			MaxJobReportsToStore:           1000,
			Lease: configuration.LeaseSettings{
				ExpireAfter: 1 * time.Hour,
			},
			DefaultJobLimits: map[string]resource.Quantity{
				"memory": resource.MustParse("1Gi"),
				"cpu":    resource.MustParse("1"),
			},
			MaxRetries: 0,
			ResourceScarcity: map[string]float64{
				"cpu": 1.0,
				"gpu": 1.0,
			},
			PoolResourceScarcity: map[string]map[string]float64{
				"cpu": map[string]float64{
					"cpu": 1.0,
				},
				"gpu": map[string]float64{
					"gpu": 1.0,
				},
			},
			MaxPodSpecSizeBytes: 0,
			MinJobResources: v1.ResourceList{
				"memory": resource.MustParse("1Mi"),
			},
			IndexedResources: []string{"cpu", "gpu", "memory"},
		}

		// Set total resources equal to the aggregate over tc.Nodes.
		// TODO: We may want to provide totalResources separately.
		totalResources := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
		for _, node := range nodes {
			totalResources.Add(node.TotalResources)
		}

		// Do the scheduling- timing starts here
		b.StartTimer()
		scheduler, err := NewLegacyScheduler(
			cfg,
			"test-executor",
			totalResources,
			nodes,
			jobRepository,
			priorityFactorByQueue,
		)
		//scheduler.SchedulingReportsRepository = NewSchedulingReportsRepository(1000, 1000)

		if err != nil {
			panic(err)
		}
		scheduledJobs, err := scheduler.Schedule(ctx.Background(), initialUsageByQueue)
		if err != nil {
			panic(err)
		}
		log.Infof("Scheduled %d jobs", len(scheduledJobs))
	})

}

func withJobRepository(action func(r *repository.RedisJobRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()
	client.FlushDB()
	repo := repository.NewRedisJobRepository(client, configuration.DatabaseRetentionPolicy{JobRetentionDuration: time.Hour})
	action(repo)
}
