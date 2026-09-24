/*
Copyright 2026 The HAMi Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

func nodeWithHyperNode(name, hyperNode string) *corev1.Node {
	labels := map[string]string{}
	if hyperNode != "" {
		labels[util.HyperNodeLabelKey] = hyperNode
	}
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func podWithAnnotations(annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", Annotations: annotations}}
}

func TestFilterNodesByHyperNodeAffinity(t *testing.T) {
	newNodes := func() *map[string]*NodeUsage {
		return &map[string]*NodeUsage{
			"node-hn-a": {Node: nodeWithHyperNode("node-hn-a", "hn-a")},
			"node-hn-b": {Node: nodeWithHyperNode("node-hn-b", "hn-b")},
			"node-none": {Node: nodeWithHyperNode("node-none", "")},
		}
	}

	t.Run("no affinity keeps every node", func(t *testing.T) {
		nodes := newNodes()
		failed := map[string]string{}
		filterNodesByHyperNodeAffinity(nodes, podWithAnnotations(nil), failed)
		if len(*nodes) != 3 {
			t.Errorf("expected 3 candidate nodes, got %d", len(*nodes))
		}
		if len(failed) != 0 {
			t.Errorf("expected no failed nodes, got %v", failed)
		}
	})

	t.Run("affinity removes foreign hypernodes", func(t *testing.T) {
		nodes := newNodes()
		failed := map[string]string{}
		pod := podWithAnnotations(map[string]string{util.HyperNodeAffinityAnnotationKey: "hn-a"})
		filterNodesByHyperNodeAffinity(nodes, pod, failed)
		if len(*nodes) != 1 {
			t.Errorf("expected 1 candidate node, got %d", len(*nodes))
		}
		if _, ok := (*nodes)["node-hn-a"]; !ok {
			t.Errorf("expected node-hn-a to be kept")
		}
		for _, nodeID := range []string{"node-hn-b", "node-none"} {
			if _, ok := (*nodes)[nodeID]; ok {
				t.Errorf("expected %s to be filtered out", nodeID)
			}
			if _, ok := failed[nodeID]; !ok {
				t.Errorf("expected %s to be reported in failedNodes", nodeID)
			}
		}
	})

	t.Run("affinity removes all nodes when none matches", func(t *testing.T) {
		nodes := newNodes()
		failed := map[string]string{}
		pod := podWithAnnotations(map[string]string{util.HyperNodeAffinityAnnotationKey: "hn-x"})
		filterNodesByHyperNodeAffinity(nodes, pod, failed)
		if len(*nodes) != 0 {
			t.Errorf("expected 0 candidate nodes, got %d", len(*nodes))
		}
		if len(failed) != 3 {
			t.Errorf("expected 3 failed nodes, got %v", failed)
		}
	})
}

func podInfoWithGroup(name, group string, nodeID string) *device.PodInfo {
	annotations := map[string]string{}
	if group != "" {
		annotations[util.HyperNodeGroupAnnotationKey] = group
	}
	return &device.PodInfo{
		Pod:    &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}},
		NodeID: nodeID,
	}
}

func TestCollectGroupHyperNodes(t *testing.T) {
	pods := []*device.PodInfo{
		podInfoWithGroup("p1", "job-1", "node-1"),
		podInfoWithGroup("p2", "job-1", "node-2"),
		podInfoWithGroup("p3", "job-2", "node-1"),
		podInfoWithGroup("p4", "", "node-3"),
	}
	lookup := func(nodeID string) (string, bool) {
		switch nodeID {
		case "node-1":
			return "hn-a", true
		case "node-2":
			return "hn-b", true
		case "node-3":
			return "", true
		}
		return "", false
	}

	got := collectGroupHyperNodes(pods, lookup, "job-1")
	if len(got) != 2 {
		t.Fatalf("expected 2 preferred hypernodes, got %v", got)
	}
	for _, hn := range []string{"hn-a", "hn-b"} {
		if _, ok := got[hn]; !ok {
			t.Errorf("expected hypernode %q in preferred set, got %v", hn, got)
		}
	}

	if got := collectGroupHyperNodes(pods, lookup, "job-unknown"); len(got) != 0 {
		t.Errorf("expected empty preferred set for unknown group, got %v", got)
	}
}

func TestCollectGroupHyperNodesLookupMiss(t *testing.T) {
	pods := []*device.PodInfo{podInfoWithGroup("p1", "job-1", "gone-node")}
	lookup := func(nodeID string) (string, bool) { return "", false }
	if got := collectGroupHyperNodes(pods, lookup, "job-1"); len(got) != 0 {
		t.Errorf("expected empty preferred set when node lookup fails, got %v", got)
	}
}

func scoreList(policyName string, scores map[string]float32, hyperNodes map[string]string) *policy.NodeScoreList {
	list := &policy.NodeScoreList{
		Policy:   policyName,
		NodeList: make([]*policy.NodeScore, 0, len(scores)),
	}
	for nodeID, score := range scores {
		list.NodeList = append(list.NodeList, &policy.NodeScore{
			NodeID: nodeID,
			Node:   nodeWithHyperNode(nodeID, hyperNodes[nodeID]),
			Score:  score,
		})
	}
	return list
}

func lastAfterSort(list *policy.NodeScoreList) string {
	sorted := &policy.NodeScoreList{NodeList: append([]*policy.NodeScore{}, list.NodeList...), Policy: list.Policy}
	sort.Sort(sorted)
	return sorted.NodeList[len(sorted.NodeList)-1].NodeID
}

func snapshotScores(list *policy.NodeScoreList) []float32 {
	out := make([]float32, len(list.NodeList))
	for i, ns := range list.NodeList {
		out[i] = ns.Score
	}
	return out
}

func scoresUnchanged(before, after []float32) bool {
	for i := range before {
		if before[i] != after[i] {
			return false
		}
	}
	return true
}

func TestApplyHyperNodeGroupAffinity(t *testing.T) {
	t.Run("empty preferred set keeps scores", func(t *testing.T) {
		list := scoreList(util.NodeSchedulerPolicyBinpack.String(),
			map[string]float32{"n1": 1, "n2": 2},
			map[string]string{"n1": "hn-a", "n2": "hn-b"})
		before := snapshotScores(list)
		applyHyperNodeGroupAffinity(list, util.NodeSchedulerPolicyBinpack.String(), nil)
		if !scoresUnchanged(before, snapshotScores(list)) {
			t.Errorf("expected unchanged scores with empty preferred set")
		}
	})

	t.Run("all nodes outside preferred keeps relative order", func(t *testing.T) {
		list := scoreList(util.NodeSchedulerPolicyBinpack.String(),
			map[string]float32{"n1": 1, "n2": 2, "n3": 3},
			map[string]string{"n1": "hn-a", "n2": "hn-b", "n3": "hn-c"})
		before := snapshotScores(list)
		applyHyperNodeGroupAffinity(list, util.NodeSchedulerPolicyBinpack.String(), map[string]struct{}{"hn-x": {}})
		if !scoresUnchanged(before, snapshotScores(list)) {
			t.Errorf("expected unchanged scores when no node matches the preferred set")
		}
	})

	t.Run("all nodes inside preferred keeps scores", func(t *testing.T) {
		list := scoreList(util.NodeSchedulerPolicyBinpack.String(),
			map[string]float32{"n1": 1, "n2": 2},
			map[string]string{"n1": "hn-a", "n2": "hn-a"})
		before := snapshotScores(list)
		applyHyperNodeGroupAffinity(list, util.NodeSchedulerPolicyBinpack.String(), map[string]struct{}{"hn-a": {}})
		if !scoresUnchanged(before, snapshotScores(list)) {
			t.Errorf("expected unchanged scores when every node matches the preferred set")
		}
	})

	t.Run("binpack prefers group hypernode", func(t *testing.T) {
		list := scoreList(util.NodeSchedulerPolicyBinpack.String(),
			map[string]float32{"in-hn": 5, "out-hn": 1},
			map[string]string{"in-hn": "hn-a", "out-hn": "hn-b"})
		applyHyperNodeGroupAffinity(list, util.NodeSchedulerPolicyBinpack.String(), map[string]struct{}{"hn-a": {}})
		if got := lastAfterSort(list); got != "in-hn" {
			t.Errorf("binpack: expected in-hn to rank best after affinity, got %s", got)
		}
	})

	t.Run("spread prefers group hypernode", func(t *testing.T) {
		list := scoreList(util.NodeSchedulerPolicySpread.String(),
			map[string]float32{"in-hn": 1, "out-hn": 5},
			map[string]string{"in-hn": "hn-a", "out-hn": "hn-b"})
		applyHyperNodeGroupAffinity(list, util.NodeSchedulerPolicySpread.String(), map[string]struct{}{"hn-a": {}})
		if got := lastAfterSort(list); got != "in-hn" {
			t.Errorf("spread: expected in-hn to rank best after affinity, got %s", got)
		}
	})

	t.Run("device score gap smaller than penalty still overridden", func(t *testing.T) {
		list := scoreList(util.NodeSchedulerPolicyBinpack.String(),
			map[string]float32{"in-hn": 10000, "out-hn": 0},
			map[string]string{"in-hn": "hn-a", "out-hn": "hn-b"})
		applyHyperNodeGroupAffinity(list, util.NodeSchedulerPolicyBinpack.String(), map[string]struct{}{"hn-a": {}})
		if got := lastAfterSort(list); got != "in-hn" {
			t.Errorf("expected affinity to dominate a large device score gap, got %s", got)
		}
	})
}
