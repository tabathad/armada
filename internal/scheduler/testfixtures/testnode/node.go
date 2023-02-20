package testnode

import (
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"k8s.io/apimachinery/pkg/api/resource"
)

const NodeType1 = "foo"
const NodeType2 = "bar"

var Node1 = &schedulerobjects.Node{
	Id:         "node1",
	NodeTypeId: NodeType1,
	NodeType:   &schedulerobjects.NodeType{Id: NodeType1},
	AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
		0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
		1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}},
		2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")}},
	},
	TotalResources: schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{
			"cpu":    resource.MustParse("3"),
			"memory": resource.MustParse("3Gi"),
		},
	},
	Labels: map[string]string{
		testfixtures.TestHostnameLabel: "node1",
	},
}

var Node2 = &schedulerobjects.Node{
	Id:         "node2",
	NodeTypeId: NodeType1,
	NodeType:   &schedulerobjects.NodeType{Id: NodeType1},
	AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
		0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("4"), "memory": resource.MustParse("4Gi")}},
		1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")}},
		2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")}},
	},
	TotalResources: schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{
			"cpu":    resource.MustParse("6"),
			"memory": resource.MustParse("6Gi"),
		},
	},
	Labels: map[string]string{
		testfixtures.TestHostnameLabel: "node2",
	},
}

var Node3 = &schedulerobjects.Node{
	Id:         "node3",
	NodeTypeId: NodeType2,
	NodeType:   &schedulerobjects.NodeType{Id: NodeType2},
	AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
		0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("7"), "memory": resource.MustParse("7Gi")}},
		1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("8"), "memory": resource.MustParse("8Gi")}},
		2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("9"), "memory": resource.MustParse("9Gi")}},
	},
	TotalResources: schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{
			"cpu":    resource.MustParse("9"),
			"memory": resource.MustParse("9Gi"),
		},
	},
	Labels: map[string]string{
		testfixtures.TestHostnameLabel: "node3",
	},
}

func WithPodReqsNodes(reqs map[int][]*schedulerobjects.PodRequirements, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for i := range nodes {
		for _, req := range reqs[i] {
			node, err := scheduling.BindPodToNode(req, nodes[i])
			if err != nil {
				panic(err)
			}
			nodes[i] = node
		}
	}
	return nodes
}

func TestCluster() []*schedulerobjects.Node {
	return []*schedulerobjects.Node{Node1, Node2, Node3}
}
