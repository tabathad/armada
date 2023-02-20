package testfixtures

import "github.com/armadaproject/armada/internal/armada/configuration"

const (
	TestGangIdAnnotation          = "armada.io/gangId"
	TestGangCardinalityAnnotation = "armada.io/gangCardinality"
	TestHostnameLabel             = "kubernetes.io/hostname"
)

var (
	TestPriorityClass0 = configuration.PriorityClass{0, true, nil}
	TestPriorityClass1 = configuration.PriorityClass{1, true, nil}
	TestPriorityClass2 = configuration.PriorityClass{2, true, nil}
	TestPriorityClass3 = configuration.PriorityClass{3, false, nil}

	TestPriorityClasses = map[string]configuration.PriorityClass{
		"priority-0": TestPriorityClass0,
		"priority-1": TestPriorityClass1,
		"priority-2": TestPriorityClass2,
		"priority-3": TestPriorityClass3,
	}
	TestDefaultPriorityClass = "priority-3"
	TestPriorities           = []int32{0, 1, 2, 3}
	TestResources            = []string{"cpu", "memory", "gpu"}
	TestIndexedTaints        = []string{"largeJobsOnly", "gpu"}
	TestIndexedNodeLabels    = []string{"largeJobsOnly", "gpu"}
)
