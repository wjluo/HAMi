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

// resolveSuperpodSchedulerPolicy returns the superpod domain policy to apply to
// pod, preferring its SuperpodSchedulerPolicyAnnotationKey annotation over
// config.SuperpodSchedulerPolicy.
//
// The per-Pod escape valve exists because the cluster default is strict: one
// wrong node annotation can leave every Pod of a task pending, and without
// this the only remedy is to turn convergence off for the whole cluster. An
// annotation that does not name a supported policy is ignored with a warning,
// so a typo cannot silently drop a workload that was meant to converge.
func resolveSuperpodSchedulerPolicy(pod *corev1.Pod) string {
	if value, ok := pod.GetAnnotations()[util.SuperpodSchedulerPolicyAnnotationKey]; ok {
		if util.IsValidSuperpodSchedulerPolicy(value) {
			return strings.TrimSpace(value)
		}
		klog.Warningf("ignoring unrecognized %s=%q on pod %s/%s, using the configured policy %q",
			util.SuperpodSchedulerPolicyAnnotationKey, value, pod.Namespace, pod.Name,
			config.SuperpodSchedulerPolicy)
	}
	return config.SuperpodSchedulerPolicy
}

// superpodPolicyEnabled reports whether the superpod scheduling policy is on for
// pod. Both enable and spill run the policy; only disable turns it off. The
// flag is a free-form string a non-chart deployment can set to anything, so an
// unrecognized value keeps the default rather than silently turning convergence
// off, in the same spirit as the other policy readers.
func superpodPolicyEnabled(pod *corev1.Pod) bool {
	value := resolveSuperpodSchedulerPolicy(pod)
	if !util.IsValidSuperpodSchedulerPolicy(value) {
		klog.Warningf("unrecognized superpod scheduler policy %q, using %q",
			value, util.SuperpodSchedulerPolicyEnable.String())
		return true
	}
	return util.SchedulerPolicyName(strings.TrimSpace(value)) != util.SuperpodSchedulerPolicyDisable
}

// superpodPolicySpillEnabled reports whether pod asked for the spill variant:
// hold the workload inside its occupied superpod while any candidate in that
// superpod fits, and fall back to a foreign superpod the moment it does not.
// The default is strict (enable); spill is opt-in, because a fallback placement
// is a degraded one and the operator has to ask for delivery over topology.
func superpodPolicySpillEnabled(pod *corev1.Pod) bool {
	value := resolveSuperpodSchedulerPolicy(pod)
	if !util.IsValidSuperpodSchedulerPolicy(value) {
		return false
	}
	return util.SchedulerPolicyName(strings.TrimSpace(value)) == util.SuperpodSchedulerPolicySpill
}

// superpodOutcome is what the policy decided, kept apart from the nodes it
// removed so the decision can be reported where a user will actually look at it.
//
// HAMi's scheduler has no /score endpoint, so the tier ranking is invisible in
// kube-scheduler configuration: without this, "why did my spread pod land on the
// busy node" and "why is this task Pending" are only answerable from V(2)
// logs. The zero value means the policy changed nothing.
type superpodOutcome struct {
	// Policy is the node packing policy that produced the incoming order, kept
	// so an override can name what it overrode.
	Policy string
	// TaskID is the task the pod names, empty for an unlabeled pod. One value
	// covers both shapes HAMi converges: a training job, or an inference
	// service whose prefill and decode halves must share a domain.
	TaskID string
	// Domains lists the superpods the workload converges into.
	Domains string
	// IntentAnchor marks a first pod whose domain was chosen deterministically
	// (crc32 of the task id) rather than inherited from a bound sibling. It
	// separates "this is where the task decided to open" from "this is where
	// the task already sits" in the event and the decision log.
	IntentAnchor bool
	// RackTier reports whether the convergence is at rack granularity.
	RackTier bool
	// SelectedNodeID is the candidate left at the head of the list.
	SelectedNodeID string
	SuperPodID     int64
	RackID         int64
	// Tier names how close the winner sits to the workload's domain.
	Tier string
	// Gated counts the candidates removed for sitting in another superpod.
	Gated int
	// CapacitySpill names the occupied superpod this pod could not be placed
	// in because no candidate in that domain fit it this round, under the
	// spill policy. Empty unless a spill actually happened. It is the honest
	// whole-task capacity signal the extender /filter layer can observe:
	// pre-sizing a domain from a replica count is invisible here, but "the
	// occupied domain has run out of fitting candidates for this pod" is not.
	CapacitySpill string
	// ScorePreferred is the candidate the packing policy had put first before
	// topology moved it, and OverrodeScore says whether that happened.
	ScorePreferred string
	OverrodeScore  bool
	// MultiDomain says the workload's domain set holds more than one superpod,
	// which under a performance-first policy is an incident rather than a
	// scoring detail: traffic between the two halves leaves the fabric.
	MultiDomain bool
	// EscapedSiblings counts same-task pods that sit in a superpod outside the
	// domain, having been placed through the per-Pod escape valve. They are left
	// out of Domains on purpose, so this is the only place the split shows.
	EscapedSiblings string
	// Conflict describes a topology report that contradicts itself, empty when
	// the annotations agree.
	Conflict string
}

// EventSuffix renders the decision for a Pod Event, which is what
// kubectl describe pod shows. An empty string means there is nothing to add.
func (o superpodOutcome) EventSuffix() string {
	if o.SelectedNodeID == "" && o.Gated == 0 && o.Conflict == "" {
		return ""
	}
	var msg string
	if o.SelectedNodeID == "" {
		// Nothing survived the gate, so the only useful sentence is the one
		// about the domain the workload is holding to.
		msg = fmt.Sprintf("; superpod policy left no candidate: %s, %d node(s) gated out as foreign",
			o.domainsClause(), o.Gated)
	} else {
		msg = fmt.Sprintf("; superpod placement: node=%s tier=%s superpod=%d", o.SelectedNodeID, o.Tier, o.SuperPodID)
		if o.RackTier {
			msg += fmt.Sprintf(" rack=%d", o.RackID)
		}
		msg += " " + o.domainsClause()
		if o.Gated > 0 {
			msg += fmt.Sprintf(", %d node(s) gated out as foreign", o.Gated)
		}
		if o.OverrodeScore {
			msg += fmt.Sprintf(", topology outranked the %s score order (score preferred %s)", o.Policy, o.ScorePreferred)
		}
	}
	if o.MultiDomain {
		// A split service is not a placement preference. Say so in the one
		// channel a user reads without turning on scheduler verbosity.
		msg += fmt.Sprintf("; WARNING: task=%s spans %d superpod domains [%s] - traffic between them leaves the fabric",
			o.TaskID, domainCount(o.Domains), o.Domains)
	}
	if o.EscapedSiblings != "" {
		msg += fmt.Sprintf("; WARNING: %s of this task sits outside that domain, placed under the escape valve",
			o.EscapedSiblings)
	}
	if o.Conflict != "" {
		msg += "; WARNING: conflicting topology annotations: " + o.Conflict
	}
	return msg
}

// describeEscapedSiblings renders the same-task pods that hold a superpod the
// gate does not count, as "2 pods in superpod 3, 1 pod in superpod 5". A pod
// whose superpod is back inside the occupied set is not outside it any more, so
// it drops out of the report.
func describeEscapedSiblings(escaped map[int64]int, occ topotree.Occupied) string {
	if len(escaped) == 0 {
		return ""
	}
	ids := slices.Sorted(maps.Keys(escaped))
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		if id < 0 || occ.SuperPodIDs[id] {
			continue
		}
		noun := "pods"
		if escaped[id] == 1 {
			noun = "pod"
		}
		parts = append(parts, fmt.Sprintf("%d %s in superpod %d", escaped[id], noun, id))
	}
	return strings.Join(parts, ", ")
}

// domainsClause names the superpods the workload converges into. An empty set is
// not a domain, so the first pod of a task reads "has no placement yet"
// rather than claiming it converged into nothing.
func (o superpodOutcome) domainsClause() string {
	occupied := o.Domains != "" && o.Domains != "none"
	if o.CapacitySpill != "" {
		// The pod sits in a foreign superpod, so describing the domain it
		// converged into would contradict the node the placement line names.
		// Say what happened instead: the domain was full for this pod and the
		// spill policy traded topology for delivery.
		return fmt.Sprintf("task=%s domain [%s] had no fitting candidate this round, spilled to superpod=%d under the spill policy", o.TaskID, o.CapacitySpill, o.SuperPodID)
	}
	if o.TaskID != "" {
		if o.IntentAnchor {
			// Nothing is bound yet; naming the target as an intent keeps a
			// first-pod placement from being read as an existing occupancy.
			return fmt.Sprintf("task=%s opening superpod(s) [%s] by deterministic intent, no sibling bound yet", o.TaskID, o.Domains)
		}
		if !occupied {
			return fmt.Sprintf("task=%s has no placement in any superpod yet", o.TaskID)
		}
		return fmt.Sprintf("task=%s converged into superpod(s) [%s]", o.TaskID, o.Domains)
	}
	if !occupied {
		return "no superpod is occupied yet"
	}
	return fmt.Sprintf("preferring occupied superpod(s) [%s]", o.Domains)
}

// applySuperpodPolicy holds a converged workload inside its superpod domain.
// It takes the candidate list the existing scoring already sorted, drops the
// nodes belonging to another superpod, and re-orders the survivors so the most
// topology-adjacent one sits last, which is where HAMi reads the winner.
//
// The returned map is the set of removed nodes keyed by the reason to report;
// an empty result means the candidate list still holds every node. The outcome
// describes the decision itself and is returned even when nothing was dropped,
// because a placement that was only re-ordered is the case that needs an
// explanation most. A pod is only ever gated when it names the task it
// belongs to: without that label there is no statement of which
// pods must share a domain, so the policy orders such a pod towards the
// superpods already in use instead of cutting the rest away. Clusters whose
// nodes carry no topology annotations, and a workload nothing has placed yet,
// are left exactly as they were.
func (s *Scheduler) applySuperpodPolicy(scores *policy.NodeScoreList, pod *corev1.Pod) (map[string]string, superpodOutcome) {
	if scores == nil || pod == nil || len(scores.NodeList) == 0 || !superpodPolicyEnabled(pod) {
		return nil, superpodOutcome{}
	}
	outcome := superpodOutcome{Policy: scores.Policy}
	spill := superpodPolicySpillEnabled(pod)

	entries := make([]topotree.Entry, 0, len(scores.NodeList))
	reports := make([]serverReport, 0, len(scores.NodeList))
	byID := make(map[string]*policy.NodeScore, len(scores.NodeList))
	topologyReported := false
	for _, ns := range scores.NodeList {
		// Read the live informer copy when there is one; the score's Node is a
		// snapshot taken when the devices were registered and can predate the
		// topology annotations entirely.
		node := ns.Node
		if s.nodeLister != nil {
			if fresh, err := s.nodeLister.Get(ns.NodeID); err == nil && fresh != nil {
				node = fresh
			}
		}
		superpodID := util.GetNodeSuperPodID(node)
		rackID := util.GetNodeRackID(node)
		if superpodID >= 0 {
			topologyReported = true
		}
		entries = append(entries, topotree.Entry{
			NodeID:     ns.NodeID,
			SuperPodID: superpodID,
			RackID:     rackID,
			Score:      float64(ns.Score),
		})
		reports = append(reports, serverReport{
			nodeID:     ns.NodeID,
			superpodID: superpodID,
			serverID:   util.GetNodeServerID(node),
		})
		byID[ns.NodeID] = ns
	}
	if !topologyReported {
		// A diagnostic, not a switch. Whether the policy has anything to hold
		// the workload to is decided by the occupancy below: one annotated node
		// must not switch strict gating on for a cluster whose topology nobody
		// reports, and one round of candidates that all happen to be
		// unannotated must not switch it off for a cluster that has it.
		s.missingTopologyWarn.Do(func() {
			klog.Warningf("superpod scheduling policy is %q but no candidate node carries a usable hami.io/superpodid annotation; "+
				"check the device plugin topology reporting or the node annotations before relying on convergence",
				config.SuperpodSchedulerPolicy)
		})
	}
	outcome.Conflict = contradictoryServers(reports)
	if outcome.Conflict != "" {
		s.conflictingTopologyWarn.Do(func() {
			klog.Warningf("node topology annotations contradict each other: %s", outcome.Conflict)
		})
	}

	taskID := util.GetTaskID(pod)
	outcome.TaskID = taskID
	var occupied topotree.Occupied
	var escaped map[int64]int
	if taskID != "" {
		occupied, escaped = s.collectTaskOccupancy(taskID, pod.UID)
	} else {
		occupied = s.collectClusterOccupancy(pod.UID)
	}
	// The first pod of a labelled task has no bound sibling to follow, so a
	// purely reactive gate would let two pods of one task open in two
	// superpods at once. Seed a deterministic intent domain instead; once any
	// sibling is bound the real occupancy above is non-empty and this is inert.
	// A task whose every sibling came out through the escape valve reads as
	// unplaced, which is the intended reading: the valve exempted those pods, so
	// the domain is still the task's to choose.
	if taskID != "" && len(occupied.SuperPodIDs) == 0 {
		if intent := topotree.SyntheticFirstPodOccupied(taskID, entries); len(intent.SuperPodIDs) > 0 {
			occupied = intent
			outcome.IntentAnchor = true
		}
	}
	outcome.Domains = domainsString(occupied)
	outcome.MultiDomain = len(occupied.SuperPodIDs) > 1
	outcome.EscapedSiblings = describeEscapedSiblings(escaped, occupied)
	hasRack := occupied.HasRackTier()
	outcome.RackTier = hasRack

	var dropped []topotree.Entry
	if taskID != "" {
		var kept []topotree.Entry
		kept, dropped = topotree.Gate(entries, occupied)
		switch {
		case len(kept) > 0:
			entries = kept
		case spill && len(dropped) > 0:
			// The occupied domain has no candidate that fits this pod this
			// round and no unannotated node is available either. Under spill
			// the pod is placed in a foreign superpod rather than left pending:
			// delivery over topology. Nothing is actually dropped, so the gate
			// count stays zero and the decision is carried by CapacitySpill.
			entries = dropped
			outcome.CapacitySpill = outcome.Domains
			dropped = nil
		default:
			// Strict policy: every candidate sits in another superpod, and
			// reporting them as unfit is the point of the policy, so they are
			// dropped rather than quietly kept.
			entries = nil
		}
	}

	scorePreferred := selectedForLog(scores.NodeList)
	ordered := topotree.Reorder(entries, occupied)
	rebuilt := make([]*policy.NodeScore, 0, len(ordered))
	tiers := make([]string, 0, len(ordered))
	for _, entry := range ordered {
		rebuilt = append(rebuilt, byID[entry.NodeID])
		tiers = append(tiers, entry.NodeID+":"+tierName(topotree.Classify(entry, occupied)))
	}
	scores.NodeList = rebuilt

	outcome.SelectedNodeID = selectedForLog(rebuilt)
	outcome.Gated = len(dropped)
	if outcome.SelectedNodeID != "" {
		for _, entry := range ordered {
			if entry.NodeID != outcome.SelectedNodeID {
				continue
			}
			outcome.SuperPodID = entry.SuperPodID
			outcome.RackID = entry.RackID
			outcome.Tier = tierName(topotree.Classify(entry, occupied))
			break
		}
	}
	// The topology tier outranks the packing score by design. Recording the
	// candidate that would have won without it is what makes a surprising
	// placement answerable.
	outcome.ScorePreferred = scorePreferred
	outcome.OverrodeScore = scorePreferred != "" && outcome.SelectedNodeID != "" && scorePreferred != outcome.SelectedNodeID

	klog.V(2).InfoS("Applied superpod scheduling policy",
		"pod", klog.KObj(pod),
		"task", taskID,
		// The deprecated key keeps the fleet's existing log greps working for
		// one release; taskLabel says which spelling a workload still writes,
		// which is the evidence for dropping the legacy readers.
		"taskLabel", util.TaskIDLabelUsed(pod),
		"service", taskID,
		"rackTier", hasRack,
		"intentAnchor", outcome.IntentAnchor,
		"capacitySpill", outcome.CapacitySpill,
		"occupied", outcome.Domains,
		"multiDomain", outcome.MultiDomain,
		"escapedSiblings", outcome.EscapedSiblings,
		"order", tiers,
		"dropped", len(dropped),
		"selected", outcome.SelectedNodeID,
		"overrodeScore", outcome.OverrodeScore,
		"conflict", outcome.Conflict)

	if len(dropped) == 0 {
		return nil, outcome
	}
	failed := make(map[string]string, len(dropped))
	for _, entry := range dropped {
		failed[entry.NodeID] = fmt.Sprintf(
			"node %s is in superpod %d, outside the superpod domain this workload converges into (%s)",
			entry.NodeID, entry.SuperPodID, outcome.Domains)
	}
	return failed, outcome
}

// serverReport is one candidate's view of where its machine sits, for the
// cross-check that the annotations of the candidate set agree with each other.
type serverReport struct {
	nodeID     string
	superpodID int64
	serverID   int64
}

// contradictoryServers names candidate nodes that put the same server in more
// than one superpod. A machine lives in one superpod, so two nodes disagreeing
// about one server index mean at least one annotation is stale or was set by
// hand. The scheduler cannot tell which is right, and must not guess by
// silently picking one domain, but naming the pair turns "why is this task
// stuck in superpod 4" into a one-line answer.
//
// Nodes missing either level are skipped: an absent server id says nothing, and
// two unannotated nodes are not a contradiction.
func contradictoryServers(reports []serverReport) string {
	byServer := make(map[int64][]serverReport)
	for _, r := range reports {
		if r.serverID < 0 || r.superpodID < 0 {
			continue
		}
		byServer[r.serverID] = append(byServer[r.serverID], r)
	}
	var out []string
	for _, serverID := range slices.Sorted(maps.Keys(byServer)) {
		domains := make(map[int64][]string)
		for _, r := range byServer[serverID] {
			domains[r.superpodID] = append(domains[r.superpodID], r.nodeID)
		}
		if len(domains) < 2 {
			continue
		}
		parts := make([]string, 0, len(domains))
		for _, superpodID := range slices.Sorted(maps.Keys(domains)) {
			nodes := domains[superpodID]
			slices.Sort(nodes)
			parts = append(parts, fmt.Sprintf("node(s) %s say superpod %d",
				strings.Join(nodes, ","), superpodID))
		}
		out = append(out, fmt.Sprintf("serverid %d is %s", serverID, strings.Join(parts, " but ")))
	}
	return strings.Join(out, "; ")
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

// domainCount reads a domainsString list back into a number, for the sentence
// that has to say how many domains a split workload spans.
func domainCount(domains string) int {
	if domains == "" || domains == "none" {
		return 0
	}
	return strings.Count(domains, ",") + 1
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

// collectTaskOccupancy records which superpods and racks the given
// task already holds. The pod being scheduled is excluded so a
// re-queued pod is not pinned to the superpod it is leaving. Pods that have
// finished (Succeeded/Failed) are excluded too: their devices are released, so
// counting their domain would keep a ghost superpod occupied and could gate
// every live pod of the task into an empty domain.
//
// A sibling placed through the per-Pod escape valve is left out of the domain
// set and returned in `escaped` instead. The valve is a one-pod exemption, so
// letting its placement widen the domain would hand every later pod of the task
// a licence to cross the fabric - and a ratchet has no way back, since the
// widened set is recomputed from live placements every round. Leaving it out
// means the task keeps the domain the gate actually converged it into, while
// the pod that slipped out stays visible as a fact to report.
//
// The walk itself lives in eachTaskSibling, shared with the soft score, so the
// two halves of the feature cannot end up counting one task's siblings by
// different rules; each caller decides for itself what an escaped pod means.
func (s *Scheduler) collectTaskOccupancy(taskID string, selfUID k8stypes.UID) (topotree.Occupied, map[int64]int) {
	scheduled := make([]topotree.ScheduledPod, 0)
	escaped := make(map[int64]int)
	s.eachTaskSibling(taskID, selfUID, func(sibling *corev1.Pod, node *corev1.Node) {
		superpodID := util.GetNodeSuperPodID(node)
		if util.SuperpodEscapeValveUsed(sibling) {
			escaped[superpodID]++
			return
		}
		scheduled = append(scheduled, topotree.ScheduledPod{
			NodeID:     node.Name,
			SuperPodID: superpodID,
			RackID:     util.GetNodeRackID(node),
		})
	})
	return topotree.CollectOccupied(scheduled), escaped
}

// collectClusterOccupancy records which superpods the cluster already has
// devices placed in. Unlabeled compute pods use it to keep opening new
// domains, which is the only convergence a pod with no stated task can be
// held to.
func (s *Scheduler) collectClusterOccupancy(selfUID k8stypes.UID) topotree.Occupied {
	podsInfo := s.podManager.ListPodsInfo()
	scheduled := make([]topotree.ScheduledPod, 0, len(podsInfo))
	for _, podInfo := range podsInfo {
		if podInfo == nil || podInfo.Pod == nil || podInfo.UID == selfUID || podInfo.NodeID == "" {
			continue
		}
		if isPodFinished(podInfo.Pod) {
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

// topologyNode resolves a node's topology metadata.
//
// The informer is the primary source on purpose: the scheduler keeps a deep
// copy of the Node inside its device cache and only replaces it when the device
// list changes, so a node whose annotations arrived (or moved) after its
// devices were registered would stay invisible in that cache indefinitely. The
// informer store is updated by every watch event, so it always holds the
// current annotations. The cached copy is the fallback for callers that have no
// node informer wired, and an empty string is answered with nil so a pod that
// was never bound contributes no occupancy.
func (s *Scheduler) topologyNode(nodeID string) *corev1.Node {
	if nodeID == "" {
		return nil
	}
	if s.nodeLister != nil {
		if node, err := s.nodeLister.Get(nodeID); err == nil && node != nil {
			return node
		}
	}
	if info, err := s.GetNode(nodeID); err == nil && info != nil {
		return info.Node
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

// podSchedulingState is the coarse liveness view the occupancy collectors
// need: a pod that has already finished no longer holds devices, so its
// superpod must not count as occupied.
type podSchedulingState int

const (
	podActive podSchedulingState = iota
	podFinished
)

// isPodFinished reports whether the pod has reached a terminal phase and no
// longer holds devices.
func isPodFinished(pod *corev1.Pod) bool {
	if pod == nil {
		return true
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return true
	default:
		return false
	}
}
