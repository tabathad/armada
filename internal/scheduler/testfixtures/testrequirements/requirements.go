package testrequirements

import (
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/config"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestUnitCpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("1Gi"),
			},
		},
		Annotations: map[string]string{
			config.JobIdAnnotation: util.NewULID(),
			config.QueueAnnotation: queue,
		},
	}
}

func TestSmallCpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("4Gi"),
			},
		},
		Annotations: map[string]string{
			config.JobIdAnnotation: util.NewULID(),
			config.QueueAnnotation: queue,
		},
	}
}

func TestNSmallCpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = TestSmallCpuJob(queue, priority)
	}
	return rv
}

func TestNLargeCpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = TestLargeCpuJob(queue, priority)
	}
	return rv
}

func TestNGpuJob(queue string, priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = TestGpuJob(queue, priority)
	}
	return rv
}

func TestLargeCpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "largeJobsOnly",
				Value: "true",
			},
		},
		Annotations: map[string]string{
			config.JobIdAnnotation: util.NewULID(),
			config.QueueAnnotation: queue,
		},
	}
}

func TestGpuJob(queue string, priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("4"),
				"memory": resource.MustParse("16Gi"),
				"gpu":    resource.MustParse("1"),
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "gpu",
				Value: "true",
			},
		},
		Annotations: map[string]string{
			config.JobIdAnnotation: util.NewULID(),
			config.QueueAnnotation: queue,
		},
	}
}

func WithGangAnnotations(reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	gangId := uuid.NewString()
	gangCardinality := fmt.Sprintf("%d", len(reqs))
	return WithAnnotations(
		map[string]string{testfixtures.TestGangIdAnnotation: gangId, testfixtures.TestGangCardinalityAnnotation: gangCardinality},
		reqs,
	)
}

func WithAnnotations(annotations map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		if req.Annotations == nil {
			req.Annotations = make(map[string]string)
		}
		maps.Copy(req.Annotations, annotations)
	}
	return reqs
}

func WithRequests(rl schedulerobjects.ResourceList, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		maps.Copy(
			req.ResourceRequirements.Requests,
			schedulerobjects.V1ResourceListFromResourceList(rl),
		)
	}
	return reqs
}

func WithNodeAffinity(nodeSelectorTerms []v1.NodeSelectorTerm, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		if req.Affinity == nil {
			req.Affinity = &v1.Affinity{}
		}
		if req.Affinity.NodeAffinity == nil {
			req.Affinity.NodeAffinity = &v1.NodeAffinity{}
		}
		if req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
		}
		req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(
			req.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
			nodeSelectorTerms...,
		)
	}
	return reqs
}
