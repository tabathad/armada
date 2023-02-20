package testconfig

import (
	"time"

	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestSchedulingConfig() configuration.SchedulingConfig {
	return configuration.SchedulingConfig{
		ResourceScarcity: map[string]float64{"cpu": 1, "memory": 0},
		Preemption: configuration.PreemptionConfig{
			PriorityClasses:      maps.Clone(testfixtures.TestPriorityClasses),
			DefaultPriorityClass: testfixtures.TestDefaultPriorityClass,
		},
		IndexedResources:          []string{"cpu", "memory"},
		GangIdAnnotation:          testfixtures.TestGangIdAnnotation,
		GangCardinalityAnnotation: testfixtures.TestGangCardinalityAnnotation,
		ExecutorTimeout:           15 * time.Minute,
	}
}

func WithRoundLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalClusterFractionToSchedule = limits
	return config
}

func WithPerQueueLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionPerQueue = limits
	return config
}

func WithPerPriorityLimitsConfig(limits map[int32]map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	for k, v := range config.Preemption.PriorityClasses {
		config.Preemption.PriorityClasses[k] = configuration.PriorityClass{
			Priority:                        v.Priority,
			MaximalResourceFractionPerQueue: limits[v.Priority],
		}
	}
	return config
}

func WithPerQueueRoundLimitsConfig(limits map[string]float64, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximalResourceFractionToSchedulePerQueue = limits
	return config
}

func WithMaxJobsToScheduleConfig(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.MaximumJobsToSchedule = n
	return config
}

func WithMaxLookbackPerQueueConfig(n uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	// For legacy reasons, it's called QueueLeaseBatchSize in config.
	config.QueueLeaseBatchSize = n
	return config
}

func WithIndexedTaintsConfig(indexedTaints []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedTaints = append(config.IndexedTaints, indexedTaints...)
	return config
}

func WithIndexedNodeLabelsConfig(indexedNodeLabels []string, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.IndexedNodeLabels = append(config.IndexedNodeLabels, indexedNodeLabels...)
	return config
}

func WithQueueLeaseBatchSizeConfig(queueLeasebatchSize uint, config configuration.SchedulingConfig) configuration.SchedulingConfig {
	config.QueueLeaseBatchSize = queueLeasebatchSize
	return config
}
