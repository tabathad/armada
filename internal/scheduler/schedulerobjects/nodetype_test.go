package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
)

func TestNewNodeType(t *testing.T) {
	tests := map[string]struct {
		Taints                     []v1.Taint
		Labels                     map[string]string
		IndexedTaints              []string
		IndexedLabels              []string
		ExpectedTaints             []v1.Taint
		ExpectedLabels             map[string]string
		ExpectedUnsetIndexedLabels map[string]string
	}{
		"IndexedTaints": {
			Taints: []v1.Taint{
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "bar",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "baz",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			IndexedTaints: []string{"foo", "baz"},
			ExpectedTaints: []v1.Taint{
				{
					Key:    "baz",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			ExpectedLabels:             make(map[string]string),
			ExpectedUnsetIndexedLabels: make(map[string]string),
		},
		"IndexedTaints index all by default": {
			Taints: []v1.Taint{
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "bar",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "baz",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			ExpectedTaints: []v1.Taint{
				{
					Key:    "bar",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "baz",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			ExpectedLabels:             make(map[string]string),
			ExpectedUnsetIndexedLabels: make(map[string]string),
		},
		"IndexedNodeLabels": {
			Labels: map[string]string{
				"foo-key": "foo-value",
				"bar-key": "bar-value",
				"baz-key": "baz-value",
			},
			IndexedLabels:  []string{"foo-key", "baz-key", "unset-key"},
			ExpectedTaints: make([]v1.Taint, 0),
			ExpectedLabels: map[string]string{
				"foo-key": "foo-value",
				"baz-key": "baz-value",
			},
			ExpectedUnsetIndexedLabels: map[string]string{
				"unset-key": "",
			},
		},
		"IndexedNodeLabels index none by default": {
			Labels: map[string]string{
				"foo-key": "foo-value",
				"bar-key": "bar-value",
				"baz-key": "baz-value",
			},
			ExpectedTaints:             make([]v1.Taint, 0),
			ExpectedLabels:             make(map[string]string),
			ExpectedUnsetIndexedLabels: make(map[string]string),
		},
	}
	mapFromSlice := func(vs []string) map[string]interface{} {
		if vs == nil {
			return nil
		}
		rv := make(map[string]interface{})
		for _, v := range vs {
			rv[v] = ""
		}
		return rv
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			taints := slices.Clone(tc.Taints)
			labels := maps.Clone(tc.Labels)
			nodeType := NewNodeType(taints, labels, mapFromSlice(tc.IndexedTaints), mapFromSlice(tc.IndexedLabels))
			assert.Equal(t, tc.ExpectedTaints, nodeType.Taints)
			assert.Equal(t, tc.ExpectedLabels, nodeType.Labels)
			assert.Equal(t, tc.ExpectedUnsetIndexedLabels, nodeType.UnsetIndexedLabels)
		})
	}
}

func TestNodeTypeIdFromTaintsAndLabels_NotEmpty(t *testing.T) {
	id := nodeTypeIdFromTaintsAndLabels(nil, nil, nil)
	assert.NotEmpty(t, id)
	id = nodeTypeIdFromTaintsAndLabels(make([]v1.Taint, 0), make(map[string]string), make(map[string]string))
	assert.NotEmpty(t, id)
}

func TestNodeTypeIdFromTaintsAndLabels_UniqueForLabels(t *testing.T) {
	assert.NotEqual(
		t,
		nodeTypeIdFromTaintsAndLabels(nil, map[string]string{"aa": "b"}, nil),
		nodeTypeIdFromTaintsAndLabels(nil, map[string]string{"a": "ab"}, nil),
	)
	assert.NotEqual(
		t,
		nodeTypeIdFromTaintsAndLabels(nil, map[string]string{"a": "aa", "b": "b"}, nil),
		nodeTypeIdFromTaintsAndLabels(nil, map[string]string{"a": "a", "ab": "b"}, nil),
	)
	assert.NotEqual(
		t,
		nodeTypeIdFromTaintsAndLabels(nil, nil, map[string]string{"aa": "b"}),
		nodeTypeIdFromTaintsAndLabels(nil, nil, map[string]string{"a": "ab"}),
	)
	assert.NotEqual(
		t,
		nodeTypeIdFromTaintsAndLabels(nil, nil, map[string]string{"a": "aa", "b": "b"}),
		nodeTypeIdFromTaintsAndLabels(nil, nil, map[string]string{"a": "a", "ab": "b"}),
	)
}

func TestNodeTypeIdFromTaintsAndLabels_ConsistentOrder(t *testing.T) {
	assert.Equal(
		t,
		nodeTypeIdFromTaintsAndLabels(
			[]v1.Taint{
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "bar",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "baz",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			nil,
			nil,
		),
		nodeTypeIdFromTaintsAndLabels(
			[]v1.Taint{
				{
					Key:    "baz",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "bar",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
				{
					Key:    "foo",
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
			nil,
			nil,
		),
	)
	assert.Equal(
		t,
		nodeTypeIdFromTaintsAndLabels(nil, map[string]string{"a": "b", "c": "d", "e": "f", "g": "h"}, nil),
		nodeTypeIdFromTaintsAndLabels(nil, map[string]string{"c": "d", "g": "h", "e": "f", "a": "b"}, nil),
	)
	assert.Equal(
		t,
		nodeTypeIdFromTaintsAndLabels(nil, nil, map[string]string{"a": "b", "c": "d", "e": "f", "g": "h"}),
		nodeTypeIdFromTaintsAndLabels(nil, nil, map[string]string{"c": "d", "g": "h", "e": "f", "a": "b"}),
	)
}
