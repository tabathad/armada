package testfixtures

// This file contains test fixtures to be used throughout the tests for this package.
import (
	"fmt"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func intRange(a, b int) []int {
	rv := make([]int, b-a+1)
	for i := range rv {
		rv[i] = a + i
	}
	return rv
}

func repeat[T any](v T, n int) []T {
	rv := make([]T, n)
	for i := 0; i < n; i++ {
		rv[i] = v
	}
	return rv
}



f

func withUsedResourcesNodes(p int32, rl schedulerobjects.ResourceList, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).MarkAllocated(p, rl)
	}
	return nodes
}

func withLabelsNodes(labels map[string]string, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		if node.Labels == nil {
			node.Labels = maps.Clone(labels)
		} else {
			maps.Copy(node.Labels, labels)
		}
	}
	return nodes
}

func withNodeSelectorPodReqs(selector map[string]string, reqs []*schedulerobjects.PodRequirements) []*schedulerobjects.PodRequirements {
	for _, req := range reqs {
		req.NodeSelector = maps.Clone(selector)
	}
	return reqs
}



func testNCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testCpuNode(priorities)
	}
	return rv
}

func testNTaintedCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testTaintedCpuNode(priorities)
	}
	return rv
}

func testNGpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testGpuNode(priorities)
	}
	return rv
}

func testCpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	return &schedulerobjects.Node{
		Id: id,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			},
		),
		Labels: map[string]string{
			testHostnameLabel: id,
		},
	}
}

func testTaintedCpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	taints := []v1.Taint{
		{
			Key:    "largeJobsOnly",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	labels := map[string]string{
		testHostnameLabel: id,
		"largeJobsOnly":   "true",
	}
	return &schedulerobjects.Node{
		Id:     id,
		Taints: taints,
		Labels: labels,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			},
		),
	}
}

func testGpuNode(priorities []int32) *schedulerobjects.Node {
	id := uuid.NewString()
	labels := map[string]string{
		testHostnameLabel: id,
		"gpu":             "true",
	}
	return &schedulerobjects.Node{
		Id:     id,
		Labels: labels,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("64"),
				"memory": resource.MustParse("1024Gi"),
				"gpu":    resource.MustParse("8"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("64"),
					"memory": resource.MustParse("1024Gi"),
					"gpu":    resource.MustParse("8"),
				},
			},
		),
	}
}

func createNodeDb(nodes []*schedulerobjects.Node) (*scheduling.NodeDb, error) {
	db, err := scheduling.NewNodeDb(
		testPriorityClasses,
		testResources,
		testIndexedTaints,
		testIndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	if err := db.UpsertMany(nodes); err != nil {
		return nil, err
	}
	return db, nil
}
