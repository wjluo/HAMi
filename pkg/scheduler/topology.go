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
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/topotree"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

// superpodPolicyEnabled reports whether the superpod scheduling policy is on.
// The flag is a free-form string a non-chart deployment can set to anything,
// so an unrecognized value keeps the default rather than silently turning
// convergence off, in the same spirit as the other policy readers.
func superpodPolicyEnabled() bool {
	value := config.SuperpodSchedulerPolicy
	if !util.IsValidSuperpodSchedulerPolicy(value) {
		klog.Warningf("unrecognized superpod scheduler policy %q, using %q",
			value, util.SuperpodSchedulerPolicyEnable.String())
		return true
	}
	return util.SchedulerPolicyName(strings.TrimSpace(value)) == util.SuperpodSchedulerPolicyEnable
}

// applySuperpodPolicy holds a converged workload inside its superpod domain.
// It takes the candidate list the existing scoring already sorted, drops the
// nodes belonging to another superpod, and re-orders the survivors so the most
// topology-adjacent one sits last, which is where HAMi reads the winner.
//
// The returned map is the set of removed nodes keyed by the reason to report;
// an empty result means the candidate list still holds every node. A pod is
// only ever gated when it names the inference service it belongs to: without
// that label there is no statement of which pods must share a domain, so the
// policy orders such a pod towards the superpods already in use instead of
// cutting the rest away. Clusters whose nodes carry no topology annotations,
// and a workload nothing has placed yet, are left exactly as they were.
func (s *Scheduler) applySuperpodPolicy(scores *policy.NodeScoreList, pod *corev1.Pod) map[string]string {
	if scores == nil || pod == nil || len(scores.NodeList) == 0 || !superpodPolicyEnabled() {
		return nil
	}

	entries := make([]topotree.Entry, 0, len(scores.NodeList))
	byID := make(map[string]*policy.NodeScore, len(scores.NodeList))
	topologyKnown := false
	for _, ns := range scores.NodeList {
		superpodID := util.GetNodeSuperPodID(ns.Node)
		if superpodID >= 0 {
			topologyKnown = true
		}
		entries = append(entries, topotree.Entry{
			NodeID:     ns.NodeID,
			SuperPodID: superpodID,
			RackID:     util.GetNodeRackID(ns.Node),
			Score:      float64(ns.Score),
		})
		byID[ns.NodeID] = ns
	}
	if !topologyKnown {
		return nil
	}

	inferServiceID := util.GetInferServiceID(pod)
	var occupied topotree.Occupied
	if inferServiceID != "" {
		occupied = s.collectInferServiceOccupancy(inferServiceID, pod.UID)
	} else {
		occupied = s.collectClusterOccupancy(pod.UID)
	}

	var dropped []topotree.Entry
	if inferServiceID != "" {
		var kept []topotree.Entry
		kept, dropped = topotree.Gate(entries, occupied)
		if len(kept) > 0 {
			entries = kept
		} else {
			// Every candidate sits in another superpod. Reporting them as
			// unfit is the point of the policy, so they are dropped rather
			// than quietly kept.
			entries = nil
		}
	}

	ordered := topotree.Reorder(entries, occupied)
	rebuilt := make([]*policy.NodeScore, 0, len(ordered))
	tiers := make([]string, 0, len(ordered))
	hasRack := topotree.HasRackTier(entries)
	for _, entry := range ordered {
		rebuilt = append(rebuilt, byID[entry.NodeID])
		tiers = append(tiers, entry.NodeID+":"+tierName(topotree.Classify(entry, occupied, hasRack)))
	}
	scores.NodeList = rebuilt

	klog.V(2).InfoS("Applied superpod scheduling policy",
		"pod", klog.KObj(pod),
		"inferServiceID", inferServiceID,
		"rackTier", hasRack,
		"occupied", domainsString(occupied),
		"order", tiers,
		"dropped", len(dropped),
		"selected", selectedForLog(rebuilt))

	if len(dropped) == 0 {
		return nil
	}
	failed := make(map[string]string, len(dropped))
	for _, entry := range dropped {
		failed[entry.NodeID] = fmt.Sprintf(
			"node %s is in superpod %d, outside the superpod domain this workload converges into (%s)",
			entry.NodeID, entry.SuperPodID, domainsString(occupied))
	}
	return failed
}

// mergeFailedNodes reports why the superpod policy removed candidates
// alongside the nodes the device fit itself rejected.
func mergeFailedNodes(failedNodes, dropped map[string]string) map[string]string {
	if len(dropped) == 0 {
		return failedNodes
	}
	merged := make(map[string]string, len(failedNodes)+len(dropped))
	maps.Copy(merged, failedNodes)
	maps.Copy(merged, dropped)
	return merged
}

func selectedForLog(scores []*policy.NodeScore) string {
	if len(scores) == 0 {
		return ""
	}
	return scores[len(scores)-1].NodeID
}

// domainsString lists the occupied superpods in a stable order, so a log line
// or an event about the same cluster state reads the same twice.
func domainsString(occ topotree.Occupied) string {
	if len(occ.SuperPodIDs) == 0 {
		return "none"
	}
	ids := slices.Sorted(maps.Keys(occ.SuperPodIDs))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, fmt.Sprintf("%d", id))
	}
	return strings.Join(out, ",")
}

// collectInferServiceOccupancy records which superpods and racks the given
// inference service already holds. The pod being scheduled is excluded so a
// re-queued pod is not pinned to the superpod it is leaving.
func (s *Scheduler) collectInferServiceOccupancy(inferServiceID string, selfUID k8stypes.UID) topotree.Occupied {
	podsInfo := s.podManager.ListPodsInfo()
	scheduled := make([]topotree.ScheduledPod, 0, len(podsInfo))
	for _, podInfo := range podsInfo {
		if podInfo == nil || podInfo.Pod == nil || podInfo.UID == selfUID {
			continue
		}
		if util.GetInferServiceID(podInfo.Pod) != inferServiceID {
			continue
		}
		node := s.topologyNode(podInfo.NodeID)
		if node == nil {
			continue
		}
		scheduled = append(scheduled, topotree.ScheduledPod{
			NodeID:     podInfo.NodeID,
			SuperPodID: util.GetNodeSuperPodID(node),
			RackID:     util.GetNodeRackID(node),
		})
	}
	return topotree.CollectOccupied(scheduled)
}

// collectClusterOccupancy records which superpods the cluster already has
// devices placed in. Unlabeled compute pods use it to keep opening new
// domains, which is the only convergence a pod with no stated service can be
// held to.
func (s *Scheduler) collectClusterOccupancy(selfUID k8stypes.UID) topotree.Occupied {
	podsInfo := s.podManager.ListPodsInfo()
	scheduled := make([]topotree.ScheduledPod, 0, len(podsInfo))
	for _, podInfo := range podsInfo {
		if podInfo == nil || podInfo.Pod == nil || podInfo.UID == selfUID || podInfo.NodeID == "" {
			continue
		}
		node := s.topologyNode(podInfo.NodeID)
		if node == nil {
			continue
		}
		scheduled = append(scheduled, topotree.ScheduledPod{
			NodeID:     podInfo.NodeID,
			SuperPodID: util.GetNodeSuperPodID(node),
			RackID:     util.GetNodeRackID(node),
		})
	}
	return topotree.CollectOccupied(scheduled)
}

// topologyNode resolves a node's topology metadata, preferring the scheduler
// cache and falling back to the node lister when the node is not registered
// (for example right after its annotations were written).
func (s *Scheduler) topologyNode(nodeID string) *corev1.Node {
	if nodeID == "" {
		return nil
	}
	if info, err := s.GetNode(nodeID); err == nil && info != nil && info.Node != nil {
		return info.Node
	}
	if s.nodeLister != nil {
		if node, err := s.nodeLister.Get(nodeID); err == nil {
			return node
		}
	}
	return nil
}

func tierName(tier topotree.Tier) string {
	switch tier {
	case topotree.TierSameRack:
		return "same-rack"
	case topotree.TierSameSuperPod:
		return "same-superpod"
	case topotree.TierOtherSuperPod:
		return "other-superpod"
	default:
		return "no-topology"
	}
}
