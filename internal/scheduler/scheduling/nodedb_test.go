package scheduling

import (
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/config"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestNodeDbSchema(t *testing.T) {
	err := nodeDbSchema(scheduler.testPriorities, scheduler.testResources).Validate()
	assert.NoError(t, err)
}

// Test the accounting of total resources across all nodes.
func TestTotalResources(t *testing.T) {
	nodeDb, err := scheduler.createNodeDb([]*schedulerobjects.Node{})
	if !assert.NoError(t, err) {
		return
	}
	expected := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting nodes for the first time should increase the resource count.
	nodes := scheduler.testNCpuNode(2, scheduler.testPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting the same nodes again should not affect total resource count.
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting new nodes should increase the resource count.
	nodes = scheduler.testNGpuNode(3, scheduler.testPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))
}

func TestSelectNodeForPod_TargetNodeIdAnnotation_Success(t *testing.T) {
	nodes := scheduler.testNCpuNode(1, scheduler.testPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := scheduler.createNodeDb(nodes)
	require.NoError(t, err)
	reqs := scheduler.withAnnotationsPodReqs(
		map[string]string{config.TargetNodeIdAnnotation: nodeId},
		scheduler.testNSmallCpuJob("A", 0, 1),
	)
	for _, req := range reqs {
		report, err := db.SelectNodeForPod(req)
		if !assert.NoError(t, err) {
			continue
		}
		require.NotNil(t, report.Node)
		assert.Equal(t, nodes[0].Id, report.Node.Id)
		assert.Equal(t, 0, len(report.NumExcludedNodesByReason))
	}
}

func TestSelectNodeForPod_TargetNodeIdAnnotation_Failure(t *testing.T) {
	nodes := scheduler.testNCpuNode(1, scheduler.testPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := scheduler.createNodeDb(nodes)
	require.NoError(t, err)
	reqs := scheduler.withAnnotationsPodReqs(
		map[string]string{config.TargetNodeIdAnnotation: "this node does not exist"},
		scheduler.testNSmallCpuJob("A", 0, 1),
	)
	for _, req := range reqs {
		report, err := db.SelectNodeForPod(req)
		if !assert.NoError(t, err) {
			continue
		}
		assert.Nil(t, report.Node)
		assert.Equal(t, 0, len(report.NumExcludedNodesByReason))
	}
}

func TestPodToFromNodeBinding(t *testing.T) {
	node := scheduler.testGpuNode(scheduler.testPriorities)
	req := scheduler.testNGpuJob("A", 0, 1)[0]
	request := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)

	newNode, err := BindPodToNode(req, node)
	require.NoError(t, err)

	jobId, err := JobIdFromPodRequirements(req)
	require.NoError(t, err)
	assert.Equal(t, []string{jobId}, maps.Keys(newNode.AllocatedByJobId))
	assert.True(
		t,
		request.Equal(newNode.AllocatedByJobId[jobId]),
	)

	assert.Equal(t, []string{"A"}, maps.Keys(newNode.AllocatedByQueue))
	assert.True(
		t,
		request.Equal(newNode.AllocatedByQueue["A"]),
	)

	expectedAllocatable := newNode.TotalResources.DeepCopy()
	expectedAllocatable.Sub(request)
	assert.True(t, expectedAllocatable.Equal(newNode.AllocatableByPriorityAndResource[req.Priority]))

	newNode, err = UnbindPodFromNode(req, newNode)
	require.NoError(t, err)
	assert.Empty(t, newNode.AllocatedByJobId)
	assert.Empty(t, newNode.AllocatedByQueue)
	expectedAllocatable = newNode.TotalResources.DeepCopy()
	assert.True(t, expectedAllocatable.Equal(newNode.AllocatableByPriorityAndResource[req.Priority]))
}

func TestSelectAndBindNodeToPod(t *testing.T) {
	tests := map[string]struct {
		Nodes         []*schedulerobjects.Node
		Reqs          []*schedulerobjects.PodRequirements
		ExpectSuccess []bool
	}{
		"all jobs fit": {
			Nodes:         scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs:          scheduler.testNSmallCpuJob("A", 0, 32),
			ExpectSuccess: scheduler.repeat(true, 32),
		},
		"not all jobs fit": {
			Nodes:         scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs:          scheduler.testNSmallCpuJob("A", 0, 33),
			ExpectSuccess: append(scheduler.repeat(true, 32), scheduler.repeat(false, 1)...),
		},
		"unavailable resource": {
			Nodes:         scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs:          scheduler.testNGpuJob("A", 0, 1),
			ExpectSuccess: scheduler.repeat(false, 1),
		},
		"unsupported resource": {
			Nodes: scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs: scheduler.withRequestsPodReqs(
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"gibberish": resource.MustParse("1"),
					},
				},
				scheduler.testNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: scheduler.repeat(false, 1),
		},
		"preemption": {
			Nodes:         scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs:          append(append(scheduler.testNSmallCpuJob("A", 0, 32), scheduler.testNSmallCpuJob("A", 1, 32)...), scheduler.testNSmallCpuJob("A", 0, 32)...),
			ExpectSuccess: append(scheduler.repeat(true, 64), scheduler.repeat(false, 32)...),
		},
		"taints/tolerations": {
			Nodes:         scheduler.testNTaintedCpuNode(1, scheduler.testPriorities),
			Reqs:          append(append(scheduler.testNSmallCpuJob("A", 0, 1), scheduler.testNGpuJob("A", 0, 1)...), scheduler.testNLargeCpuJob("A", 0, 1)...),
			ExpectSuccess: []bool{false, false, true},
		},
		"node selector": {
			Nodes: append(
				scheduler.testNCpuNode(1, scheduler.testPriorities),
				scheduler.withLabelsNodes(
					map[string]string{
						"key": "value",
					},
					scheduler.testNCpuNode(1, scheduler.testPriorities),
				)...,
			),
			Reqs: scheduler.withNodeSelectorPodReqs(
				map[string]string{
					"key": "value",
				},
				scheduler.testNSmallCpuJob("A", 0, 33),
			),
			ExpectSuccess: append(scheduler.repeat(true, 32), scheduler.repeat(false, 1)...),
		},
		"node selector with mismatched value": {
			Nodes: scheduler.withLabelsNodes(
				map[string]string{
					"key": "value",
				},
				scheduler.testNCpuNode(1, scheduler.testPriorities),
			),
			Reqs: scheduler.withNodeSelectorPodReqs(
				map[string]string{
					"key": "this is the wrong value",
				},
				scheduler.testNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: scheduler.repeat(false, 1),
		},
		"node selector with missing label": {
			Nodes: scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs: scheduler.withNodeSelectorPodReqs(
				map[string]string{
					"this label does not exist": "value",
				},
				scheduler.testNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: scheduler.repeat(false, 1),
		},
		"node affinity": {
			Nodes: append(
				scheduler.testNCpuNode(1, scheduler.testPriorities),
				scheduler.withLabelsNodes(
					map[string]string{
						"key": "value",
					},
					scheduler.testNCpuNode(1, scheduler.testPriorities),
				)...,
			),
			Reqs: scheduler.withNodeAffinityPodReqs(
				[]v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "key",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{"value"},
							},
						},
					},
				},
				scheduler.testNSmallCpuJob("A", 0, 33),
			),
			ExpectSuccess: append(scheduler.repeat(true, 32), scheduler.repeat(false, 1)...),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := scheduler.createNodeDb(tc.Nodes)
			require.NoError(t, err)
			for i, req := range tc.Reqs {
				report, err := nodeDb.SelectAndBindNodeToPod(req)
				require.NoError(t, err)
				if !tc.ExpectSuccess[i] {
					assert.Nil(t, report.Node)
					continue
				}
				assert.NotNil(t, report.Node)

				node, err := nodeDb.GetNode(report.Node.Id)
				require.NoError(t, err)
				jobId, err := JobIdFromPodRequirements(req)
				require.NoError(t, err)
				expected := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
				actual, ok := node.AllocatedByJobId[jobId]
				require.True(t, ok)
				assert.True(t, actual.Equal(expected))
			}
		})
	}
}

func TestScheduleMany(t *testing.T) {
	tests := map[string]struct {
		// Nodes to schedule across.
		Nodes []*schedulerobjects.Node
		// Schedule one group of jobs at a time.
		// Each group is composed of a slice of pods.
		Reqs [][]*schedulerobjects.PodRequirements
		// For each group, whether we expect scheduling to succeed.
		ExpectSuccess []bool
	}{
		"simple success": {
			Nodes:         scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{scheduler.testNSmallCpuJob("A", 0, 32)},
			ExpectSuccess: []bool{true},
		},
		"simple failure": {
			Nodes:         scheduler.testNCpuNode(1, scheduler.testPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{scheduler.testNSmallCpuJob("A", 0, 33)},
			ExpectSuccess: []bool{false},
		},
		"correct rollback": {
			Nodes: scheduler.testNCpuNode(2, scheduler.testPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				scheduler.testNSmallCpuJob("A", 0, 32),
				scheduler.testNSmallCpuJob("A", 0, 33),
				scheduler.testNSmallCpuJob("A", 0, 32),
			},
			ExpectSuccess: []bool{true, false, true},
		},
		"varying job size": {
			Nodes: scheduler.testNCpuNode(2, scheduler.testPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				append(scheduler.testNLargeCpuJob("A", 0, 1), scheduler.testNSmallCpuJob("A", 0, 32)...),
				scheduler.testNSmallCpuJob("A", 0, 1),
			},
			ExpectSuccess: []bool{true, false},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := scheduler.createNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			for i, reqs := range tc.Reqs {
				reports, ok, err := nodeDb.ScheduleMany(reqs)
				if !assert.NoError(t, err) {
					return
				}
				if tc.ExpectSuccess[i] {
					assert.Equal(t, len(reqs), len(reports))
					for _, report := range reports {
						if !assert.NotNil(t, report.Node) {
							return
						}
					}
					assert.True(t, ok)
				} else {
					assert.False(t, ok)
				}
			}
		})
	}
}

func benchmarkUpsert(nodes []*schedulerobjects.Node, b *testing.B) {
	db, err := NewNodeDb(
		scheduler.testPriorityClasses,
		scheduler.testResources,
		scheduler.testIndexedTaints,
		scheduler.testIndexedNodeLabels,
	)
	if !assert.NoError(b, err) {
		return
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := db.UpsertMany(nodes)
		if !assert.NoError(b, err) {
			return
		}
	}
}

func BenchmarkUpsert1(b *testing.B) {
	benchmarkUpsert(scheduler.testNCpuNode(1, scheduler.testPriorities), b)
}
func BenchmarkUpsert1000(b *testing.B) {
	benchmarkUpsert(scheduler.testNCpuNode(1000, scheduler.testPriorities), b)
}
func BenchmarkUpsert100000(b *testing.B) {
	benchmarkUpsert(scheduler.testNCpuNode(100000, scheduler.testPriorities), b)
}

func benchmarkSelectAndBindNodeToPod(nodes []*schedulerobjects.Node, reqs []*schedulerobjects.PodRequirements, b *testing.B) {
	db, err := NewNodeDb(
		scheduler.testPriorityClasses,
		scheduler.testResources,
		scheduler.testIndexedTaints,
		scheduler.testIndexedNodeLabels,
	)
	if !assert.NoError(b, err) {
		return
	}

	err = db.UpsertMany(nodes)
	if !assert.NoError(b, err) {
		return
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.db.Txn(true)
		for _, req := range reqs {
			_, err := db.SelectAndBindNodeToPodWithTxn(txn, req)
			if !assert.NoError(b, err) {
				return
			}
		}
		txn.Abort()
	}
}

func BenchmarkSelectAndBindNodeToPodOneCpuNode(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		scheduler.testNCpuNode(1, scheduler.testPriorities),
		scheduler.testNSmallCpuJob("A", 0, 32),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		scheduler.testNCpuNode(100, scheduler.testPriorities),
		scheduler.testNSmallCpuJob("A", 0, 320),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		scheduler.testNCpuNode(10000, scheduler.testPriorities),
		scheduler.testNSmallCpuJob("A", 0, 32000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		scheduler.withUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			scheduler.testNCpuNode(100, scheduler.testPriorities),
		),
		scheduler.testNSmallCpuJob("A", 0, 100),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod1000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		scheduler.withUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			scheduler.testNCpuNode(1000, scheduler.testPriorities),
		),
		scheduler.testNSmallCpuJob("A", 0, 1000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		scheduler.withUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			scheduler.testNCpuNode(10000, scheduler.testPriorities),
		),
		scheduler.testNSmallCpuJob("A", 0, 10000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPodResourceConstrained(b *testing.B) {
	nodes := append(append(
		scheduler.testNCpuNode(500, scheduler.testPriorities),
		scheduler.testNGpuNode(1, scheduler.testPriorities)...),
		scheduler.testNCpuNode(499, scheduler.testPriorities)...,
	)
	benchmarkSelectAndBindNodeToPod(
		nodes,
		scheduler.testNGpuJob("A", 0, 1),
		b,
	)
}
