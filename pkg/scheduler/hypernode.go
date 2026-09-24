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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

// filterNodesByHyperNodeAffinity enforces the pod's hard hypernode affinity
// (hami.io/hypernode-affinity): candidate nodes outside the pinned hypernode
// are removed from the candidate set and reported in failedNodes. Nodes with
// no hypernode label never match an affinity.
func filterNodesByHyperNodeAffinity(nodes *map[string]*NodeUsage, task *corev1.Pod, failedNodes map[string]string) {
	affinity := util.GetHyperNodeAffinityByPod(task)
	if affinity == "" || nodes == nil {
		return
	}
	for nodeID, usage := range *nodes {
		if hyperNode := util.GetNodeHyperNode(usage.Node); hyperNode != affinity {
			reason := fmt.Sprintf("HyperNodeNotFit: pod requires hypernode %q, node belongs to %q", affinity, hyperNode)
			failedNodes[nodeID] = reason
			delete(*nodes, nodeID)
			klog.V(4).InfoS("Node filtered by hypernode affinity",
				"pod", klog.KObj(task), "node", nodeID, "required", affinity, "nodeHyperNode", hyperNode)
		}
	}
}

// collectGroupHyperNodes returns the set of hypernodes already occupied by the
// scheduled pods of one hypernode group. lookups for nodes that left the
// cluster return ok=false and are skipped; nodes without a hypernode label
// contribute nothing.
func collectGroupHyperNodes(pods []*device.PodInfo, lookup func(nodeID string) (string, bool), groupID string) map[string]struct{} {
	preferred := make(map[string]struct{})
	for _, podInfo := range pods {
		if podInfo == nil || podInfo.Pod == nil {
			continue
		}
		if util.GetHyperNodeGroupByPod(podInfo.Pod) != groupID {
			continue
		}
		hyperNode, ok := lookup(podInfo.NodeID)
		if !ok || hyperNode == "" {
			continue
		}
		preferred[hyperNode] = struct{}{}
	}
	return preferred
}

// applyHyperNodeGroupAffinity re-weights node scores so nodes inside the
// group's already occupied hypernodes rank ahead of the rest, mirroring the
// packing behaviour of Volcano's network-topology-aware scheduling in a
// single-tier hypernode topology. HAMi always picks the last element of the
// sorted NodeScoreList: binpack (the default) sorts ascending so the highest
// score wins, spread sorts descending so the lowest score wins. The penalty
// on non-matching nodes therefore lowers their score under binpack and raises
// it under spread. A single penalty magnitude is derived from the current
// score range, which keeps the relative order inside each affinity class
// intact: the hypernode preference dominates device-placement preferences but
// never reorders nodes within the same class.
func applyHyperNodeGroupAffinity(list *policy.NodeScoreList, nodePolicy string, preferred map[string]struct{}) {
	if list == nil || len(preferred) == 0 || len(list.NodeList) == 0 {
		return
	}
	matched := 0
	for _, score := range list.NodeList {
		if _, ok := preferred[util.GetNodeHyperNode(score.Node)]; ok {
			matched++
		}
	}
	// Without information to act on (nothing matches, or everything does)
	// scores must stay untouched so scheduling degrades to the plain policy.
	if matched == 0 || matched == len(list.NodeList) {
		return
	}

	var maxAbs float32
	for _, score := range list.NodeList {
		abs := score.Score
		if abs < 0 {
			abs = -abs
		}
		if abs > maxAbs {
			maxAbs = abs
		}
	}
	penalty := maxAbs*10 + 1

	for _, score := range list.NodeList {
		if _, ok := preferred[util.GetNodeHyperNode(score.Node)]; ok {
			continue
		}
		if nodePolicy == util.NodeSchedulerPolicySpread.String() {
			score.Score += penalty
		} else {
			score.Score -= penalty
		}
	}
	klog.V(4).InfoS("Applied hypernode group affinity",
		"preferredHyperNodes", len(preferred), "nodes", len(list.NodeList), "matched", matched, "policy", nodePolicy)
}

// groupHyperNodePreference resolves the preferred hypernode set of the task's
// hypernode group from the pods the scheduler has already placed.
func (s *Scheduler) groupHyperNodePreference(task *corev1.Pod) map[string]struct{} {
	groupID := util.GetHyperNodeGroupByPod(task)
	if groupID == "" {
		return nil
	}
	return collectGroupHyperNodes(s.podManager.ListPodsInfo(), func(nodeID string) (string, bool) {
		nodeInfo, err := s.GetNode(nodeID)
		if err != nil {
			return "", false
		}
		return util.GetNodeHyperNode(nodeInfo.Node), true
	}, groupID)
}
