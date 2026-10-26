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
	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/topology/plane"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

// The three adjacency states the production leaf-path scheme scores by, at the
// values it was tuned with, so a placement decided here stays comparable with
// the fleet that is running the same rule today. They are bands, not distances:
// distance itself comes from the one plane reader the gate and the audit use.
const (
	topoScoreSameNode  int64 = 100
	topoScoreSameLeaf  int64 = 80
	topoScoreCrossLeaf int64 = 0

	// topoScoreScale maps a band onto the 0-10 range the weight multiplies, which
	// is what makes config.TopologyScoreWeight mean the same thing it does in
	// the production scheme.
	topoScoreScale float32 = 10

	// The band names are the reason strings the per-node trace and the
	// operations runbook carry, so a grep for one of them finds the same state
	// the score applied.
	tierSameNode  = "SameNode"
	tierSameLeaf  = "SameLeaf"
	tierCrossLeaf = "CrossLeaf"
)

// ensureDomainRegistry reads the domain registry once per process and hands
// every reader the same table. Both the plane audit and the soft score ask
// through here: a round that fetched its own copy could score a candidate
// against a table the audit never reported, and putting a ConfigMap read in the
// scoring path would price every Filter. A registry written after startup is
// picked up on the scheduler's next restart, the same lifetime the audit
// already has.
func (s *Scheduler) ensureDomainRegistry() *plane.Registry {
	s.domainRegistryOnce.Do(func() {
		if s.kubeClient == nil {
			return
		}
		reg, err := s.loadDomainRegistry()
		if err != nil {
			klog.Warningf("topo-plane domain registry unavailable, vendor leaf paths stay unmapped: %v", err)
			return
		}
		if reg != nil {
			s.domainRegistry.Store(reg)
		}
	})
	return s.domainRegistry.Load()
}

// leafAdjacency is one scheduling round's answer to "where does this task
// already sit on the compute fabric". It is built once per pod, before the
// candidates are scored, and read-only afterwards, so every candidate is
// compared against the same view.
type leafAdjacency struct {
	// siblings are the nodes that already host the task, for the same-node band.
	siblings map[string]bool
	// anchors are the compute planes of those nodes. Several are kept because a
	// task may legitimately span leaves; a candidate is adjacent to the task if
	// it is adjacent to any anchor.
	anchors []plane.Domain
	// registry and cfg are the plane reader's inputs, captured once so the
	// round cannot resolve a node two different ways.
	registry *plane.Registry
	cfg      plane.Config
}

// prepareLeafAdjacency builds that view, or says why there is nothing to build.
//
// It stays off all three of these paths, and the order is not arbitrary:
//   - the unified switch. `disable` is the one lever that turns the topology
//     feature off, so a pod that is exempt from the gate must not still be
//     pulled sideways by a score (P0-4's acceptance);
//   - no task id. Without a stated task there is nothing to be adjacent to;
//   - no bound sibling. A task's first pod gets its domain from the gate's
//     deterministic intent, and a second, weaker vote on the same question
//     would only disagree with it under churn.
func (s *Scheduler) prepareLeafAdjacency(pod *corev1.Pod) (*leafAdjacency, string) {
	if !superpodPolicyEnabled(pod) {
		return nil, "policy-off"
	}
	taskID := util.GetTaskID(pod)
	if taskID == "" {
		return nil, "no-task"
	}
	adj := &leafAdjacency{
		siblings: map[string]bool{},
		registry: s.ensureDomainRegistry(),
		cfg:      plane.Config{ServiceDefault: config.TopologyServicePlaneDomain},
	}
	s.eachTaskSibling(taskID, pod.UID, func(sibling *corev1.Pod, node *corev1.Node) {
		// A sibling that left through the per-Pod valve exempted itself from
		// convergence; letting it anchor the score would drag the rest of the
		// task out of its domain, which is exactly what the valve was scoped to
		// avoid (B-1).
		if util.SuperpodEscapeValveUsed(sibling) {
			return
		}
		adj.siblings[node.Name] = true
		if domain := plane.ResolveWith(node, adj.registry, adj.cfg)[plane.Compute]; domain.State == plane.Labeled {
			adj.anchors = append(adj.anchors, domain)
		}
	})
	if len(adj.siblings) == 0 {
		return nil, "no-sibling-bound"
	}
	if len(adj.anchors) == 0 {
		// Nothing on this plane is comparable, so nothing may move. A cluster
		// whose compute fabric is not labelled yet has to score exactly as it
		// did before this layer existed.
		return nil, "no-compute-plane"
	}
	return adj, ""
}

// tier names how close one candidate sits to the task's own leaf.
func (a *leafAdjacency) tier(node *corev1.Node) (int64, string) {
	if a.siblings[node.Name] {
		return topoScoreSameNode, tierSameNode
	}
	candidate := plane.ResolveWith(node, a.registry, a.cfg)[plane.Compute]
	for _, anchor := range a.anchors {
		// Full depth means the candidate shares every tier the compute plane
		// reports, domain first: two switches with the same leaf name on
		// different fabrics are not neighbours, they are unreachable.
		if plane.LCP(candidate, anchor) == plane.Depth(plane.Compute) {
			return topoScoreSameLeaf, tierSameLeaf
		}
	}
	return topoScoreCrossLeaf, tierCrossLeaf
}

// applyLeafAdjacency adds the compute-leaf score to the candidates the device
// fit already accepted, before the gate cuts any of them. It is the soft half of
// the two-layer design: it never removes a candidate, it only orders the ones
// that survive, and the gate's tier order still outranks it.
//
// The boost is signed by the node policy because HAMi reads the winner off the
// end of the score list: under `spread` that end holds the lowest score, so a
// positive boost would push the adjacent node to the back and the topology
// layer would actively separate the task. Same convention as the policy-neutral
// device scorers, which invert their weight under spread for the same reason.
func (s *Scheduler) applyLeafAdjacency(scores *policy.NodeScoreList, pod *corev1.Pod) {
	if scores == nil || pod == nil || len(scores.NodeList) == 0 || config.TopologyScoreWeight <= 0 {
		return
	}
	adj, skip := s.prepareLeafAdjacency(pod)
	if adj == nil {
		// TopoSkip/TopoPrepare are the names the operations runbook greps for;
		// they stay so a verification command does not break on the rename.
		klog.V(4).InfoS("[TopoSkip]", "pod", klog.KObj(pod), "reason", skip, "status", "SKIPPED")
		return
	}
	sign := float32(1)
	if scores.Policy == util.NodeSchedulerPolicySpread.String() {
		sign = -1
	}
	labeled := 0
	for _, ns := range scores.NodeList {
		// Read the informer's copy: a node that moved between two leaves keeps
		// its devices registered, and a score computed from the stale snapshot
		// would chase a fabric position it no longer has.
		node := ns.Node
		if fresh := s.topologyNode(ns.NodeID); fresh != nil {
			node = fresh
		}
		score, tier := adj.tier(node)
		if tier != tierCrossLeaf {
			labeled++
		}
		boost := float32(score) / topoScoreScale * config.TopologyScoreWeight * sign
		if boost == 0 {
			continue
		}
		klog.V(4).InfoS("[TopoTrace]",
			"pod", klog.KObj(pod),
			"node", ns.NodeID,
			"baseScore", ns.Score,
			"topoScore", boost,
			"reason", tier,
			"policy", scores.Policy)
		ns.Score += boost
	}
	// V(2), the same level the gate reports its own decision at, so an operator
	// reading one round sees both halves of the topology layer or neither.
	klog.V(2).InfoS("[TopoPrepare]",
		"pod", klog.KObj(pod),
		"task", util.GetTaskID(pod),
		"mode", "SiblingFollow",
		"anchors", len(adj.anchors),
		"adjacent", labeled,
		"status", "APPROVED")
}

// eachTaskSibling calls fn for every live pod of one task other than selfUID,
// with the node it sits on. Escape-valve placements are handed to the caller
// rather than filtered here: the gate has to name the pods it left out of the
// domain, while the score has to ignore them, and two different rules in one
// walk is how the two halves start disagreeing about the same task.
func (s *Scheduler) eachTaskSibling(taskID string, selfUID k8stypes.UID, fn func(sibling *corev1.Pod, node *corev1.Node)) {
	for _, podInfo := range s.podManager.ListPodsInfo() {
		if podInfo == nil || podInfo.Pod == nil || podInfo.UID == selfUID {
			continue
		}
		if isPodFinished(podInfo.Pod) {
			continue
		}
		if util.GetTaskID(podInfo.Pod) != taskID {
			continue
		}
		node := s.topologyNode(podInfo.NodeID)
		if node == nil {
			continue
		}
		fn(podInfo.Pod, node)
	}
}
