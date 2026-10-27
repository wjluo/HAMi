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

	"gotest.tools/v3/assert"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/topology/plane"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

// computeLeafNode labels a node onto one compute fabric position the way the
// operator writes it by hand (decision D-3: one label per tier).
func computeLeafNode(node *corev1.Node, domain, leaf string) *corev1.Node {
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	if domain != "" {
		node.Labels["hami.io/topo.compute.domain"] = domain
	}
	if leaf != "" {
		node.Labels["hami.io/topo.compute.leaf"] = leaf
	}
	return node
}

// scoreOf reads one candidate's score back out of a list by node id, so an
// assertion says which candidate it is about instead of which index it sits at.
func scoreOf(scores *policy.NodeScoreList, nodeID string) float32 {
	for _, ns := range scores.NodeList {
		if ns.NodeID == nodeID {
			return ns.Score
		}
	}
	return -1
}

func withTopologyScoreWeight(t *testing.T, value float32) {
	t.Helper()
	previous := config.TopologyScoreWeight
	config.TopologyScoreWeight = value
	t.Cleanup(func() { config.TopologyScoreWeight = previous })
}

// D-4 pins the soft-score weight to the value the production fleet was tuned
// with. The 2.10 branch this layer is ported from ships 2.0 as its default, so
// the default itself is the thing under test: a silent re-base to 2.0 would
// quietly re-tune every placement the topology layer decides.
func TestTopologyScoreWeightIsPinnedToTheTunedValue(t *testing.T) {
	assert.Equal(t, config.TopologyScoreWeight, float32(3.0))
}

// The three states the production scheme logs and scores by: the node a sibling
// already sits on, a node on the same leaf, and a node on another leaf. Only
// the first two get a boost, and the gap between bands is what the
// calibration test below measures against the packing score.
func TestApplyLeafAdjacencyScoresTheThreeStates(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	home := computeLeafNode(topoNode("node-home", "1", "1", "1"), "PF-1", "LF-1")
	sameLeaf := computeLeafNode(topoNode("node-same-leaf", "1", "1", "2"), "PF-1", "LF-1")
	crossLeaf := computeLeafNode(topoNode("node-cross-leaf", "1", "2", "3"), "PF-1", "LF-2")
	for _, node := range []*corev1.Node{home, sameLeaf, crossLeaf} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(home, sameLeaf, crossLeaf)
	s.podManager.AddPod(taskPod("p-0", "svc-leaf"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(home, 10), nodeScore(sameLeaf, 10), nodeScore(crossLeaf, 10))
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-leaf"))

	// Raw bands 100/80/0 scaled by /10 and multiplied by the pinned weight.
	assert.Equal(t, scoreOf(scores, "node-home"), float32(10)+10.0*3.0)
	assert.Equal(t, scoreOf(scores, "node-same-leaf"), float32(10)+8.0*3.0)
	assert.Equal(t, scoreOf(scores, "node-cross-leaf"), float32(10))
}

// A leaf is only comparable inside one fabric domain: two nodes whose leaf
// switches carry the same name under different domains are not neighbours, and
// the three physical fabrics are isolated. The compute plane's tier order
// (domain then leaf) is what makes the cross-domain pair read as far apart.
func TestApplyLeafAdjacencyTreatsDifferentDomainsAsCrossLeaf(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	home := computeLeafNode(topoNode("node-home", "1", "1", "1"), "PF-1", "LF-9")
	otherFabric := computeLeafNode(topoNode("node-other-fabric", "2", "1", "2"), "PF-2", "LF-9")
	for _, node := range []*corev1.Node{home, otherFabric} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(home, otherFabric)
	s.podManager.AddPod(taskPod("p-0", "svc-domain"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(home, 10), nodeScore(otherFabric, 10))
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-domain"))

	assert.Equal(t, scoreOf(scores, "node-other-fabric"), float32(10),
		"the same leaf name on another fabric domain is not adjacency")
}

// The first pod of a task has no sibling to be adjacent to. Its domain is the
// intent anchor's decision, and a soft score would be a second, weaker vote on
// the same question.
func TestApplyLeafAdjacencyInertWithoutSiblings(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	first := computeLeafNode(topoNode("node-a", "1", "1", "1"), "PF-1", "LF-1")
	second := computeLeafNode(topoNode("node-b", "1", "1", "2"), "PF-1", "LF-1")
	registerTopoNode(t, s, first)
	registerTopoNode(t, s, second)
	s.nodeLister = nodeListerWith(first, second)

	scores := scoreList(nodeScore(first, 7), nodeScore(second, 3))
	s.applyLeafAdjacency(scores, taskPod("p-0", "svc-empty"))

	assert.Equal(t, scoreOf(scores, "node-a"), float32(7))
	assert.Equal(t, scoreOf(scores, "node-b"), float32(3))
}

// Nothing is labelled on the compute plane, so nothing is comparable, so no
// candidate moves. This is the promise that P0-3 changes no placement in a
// cluster that has not been labelled yet.
func TestApplyLeafAdjacencyInertWithoutComputePlane(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	home := topoNode("node-home", "1", "1", "1")
	other := topoNode("node-other", "1", "1", "2")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, other)
	s.nodeLister = nodeListerWith(home, other)
	s.podManager.AddPod(taskPod("p-0", "svc-unlabeled"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(home, 5), nodeScore(other, 99))
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-unlabeled"))

	assert.Equal(t, scoreOf(scores, "node-home"), float32(5), "an unlabelled plane must not score")
	assert.Equal(t, scoreOf(scores, "node-other"), float32(99))
}

// The weight is also the calibration lever on its own: at zero the soft score
// is mute while the gate still holds the task in its domain, which is how an
// operator separates "the distance is wrong" from "the domain is wrong" during
// a live calibration. Mute means quiet, not noisy: an off layer writes nothing.
func TestApplyLeafAdjacencyMutedByZeroWeight(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 0)

	home := computeLeafNode(topoNode("node-home", "1", "1", "1"), "PF-1", "LF-1")
	neighbour := computeLeafNode(topoNode("node-neighbour", "1", "1", "2"), "PF-1", "LF-1")
	for _, node := range []*corev1.Node{home, neighbour} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(home, neighbour)
	s.podManager.AddPod(taskPod("p-0", "svc-zero"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(home, 1), nodeScore(neighbour, 2))
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-zero"))

	assert.Equal(t, scoreOf(scores, "node-home"), float32(1), "weight 0 must leave the packing score alone")
	assert.Equal(t, scoreOf(scores, "node-neighbour"), float32(2))
}

// One switch closes the whole feature (P0-4's acceptance): a pod that turns
// convergence off must not still be pulled towards its siblings' leaf by a
// score, or `disable` would leave the topology layer half in place.
func TestApplyLeafAdjacencySilencedByTheSameSwitchAsTheGate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy string
		perPod string
	}{
		{"cluster default", util.SuperpodSchedulerPolicyDisable.String(), ""},
		{"per-pod valve", util.SuperpodSchedulerPolicyEnable.String(), util.SuperpodSchedulerPolicyDisable.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withSuperpodPolicy(t, tc.policy)

			s := NewScheduler()
			t.Cleanup(func() { close(s.stopCh) })
			withTopologyScoreWeight(t, 3.0)

			home := computeLeafNode(topoNode("node-home", "1", "1", "1"), "PF-1", "LF-1")
			far := computeLeafNode(topoNode("node-far", "1", "1", "2"), "PF-1", "LF-2")
			registerTopoNode(t, s, home)
			registerTopoNode(t, s, far)
			s.nodeLister = nodeListerWith(home, far)
			s.podManager.AddPod(taskPod("p-0", "svc-off"), "node-home", device.PodDevices{})

			var pending *corev1.Pod
			if tc.perPod != "" {
				pending = podWithSuperpodPolicy("p-1", "svc-off", tc.perPod)
			} else {
				pending = taskPod("p-1", "svc-off")
			}
			scores := scoreList(nodeScore(home, 5), nodeScore(far, 9))
			s.applyLeafAdjacency(scores, pending)

			assert.Equal(t, scoreOf(scores, "node-home"), float32(5), "disable must silence the soft score too")
			assert.Equal(t, scoreOf(scores, "node-far"), float32(9))
		})
	}
}

// B-1's rule reaches the score too: a sibling that left through the per-Pod
// valve exempted itself, and must not become the leaf that pulls the rest of
// the task towards it.
func TestApplyLeafAdjacencyIgnoresEscapedSiblings(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	escaped := computeLeafNode(topoNode("node-escaped", "2", "1", "2"), "PF-1", "LF-1")
	sameLeaf := computeLeafNode(topoNode("node-same-leaf", "1", "2", "3"), "PF-1", "LF-1")
	registerTopoNode(t, s, escaped)
	registerTopoNode(t, s, sameLeaf)
	s.nodeLister = nodeListerWith(escaped, sameLeaf)
	s.podManager.AddPod(
		podWithSuperpodPolicy("p-0", "svc-valve", util.SuperpodSchedulerPolicySpill.String()),
		"node-escaped", device.PodDevices{})

	scores := scoreList(nodeScore(escaped, 1), nodeScore(sameLeaf, 5))
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-valve"))

	assert.Equal(t, scoreOf(scores, "node-escaped"), float32(1), "an escaped sibling is not a same-node anchor")
	assert.Equal(t, scoreOf(scores, "node-same-leaf"), float32(5),
		"an escaped sibling must not anchor the leaf score")
}

// The layer is additive, never exclusionary: a cross-leaf candidate keeps its
// own score and stays in the list. Cutting candidates is the gate's job, and a
// node the device fit already accepted is not made unfit by a distance.
func TestApplyLeafAdjacencyKeepsEveryCandidate(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	home := computeLeafNode(topoNode("node-home", "1", "1", "1"), "PF-1", "LF-1")
	far := computeLeafNode(topoNode("node-far", "1", "2", "2"), "PF-1", "LF-9")
	plain := topoNode("node-plain", "", "", "")
	for _, node := range []*corev1.Node{home, far, plain} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(home, far, plain)
	s.podManager.AddPod(taskPod("p-0", "svc-keep"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(home, 1), nodeScore(far, 2), nodeScore(plain, 3))
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-keep"))

	assert.Equal(t, len(scores.NodeList), 3)
	assert.Equal(t, scoreOf(scores, "node-plain"), float32(3))
}

// The calibration question, stated as a test: the boost only outranks the
// packing score when the band gap times the weight exceeds the score gap. Below that
// boundary binpack still decides, so the weight is a real tuning knob with a
// measurable edge, not a blanket override of the packing policy.
func TestApplyLeafAdjacencyBoundaryAgainstThePackingScore(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	// The task sits on node-anchor, which is not a candidate this round, so the
	// only topology signal between the two candidates is the compute leaf.
	anchor := computeLeafNode(topoNode("node-anchor", "1", "1", "1"), "PF-1", "LF-1")
	adjacent := computeLeafNode(topoNode("node-adjacent", "1", "1", "2"), "PF-1", "LF-1")
	distant := computeLeafNode(topoNode("node-distant", "1", "2", "3"), "PF-1", "LF-2")
	for _, node := range []*corev1.Node{anchor, adjacent, distant} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(anchor, adjacent, distant)
	s.podManager.AddPod(taskPod("p-0", "svc-edge"), "node-anchor", device.PodDevices{})

	best := func(weight float32) string {
		t.Helper()
		withTopologyScoreWeight(t, weight)
		scores := scoreList(nodeScore(adjacent, 60), nodeScore(distant, 80))
		s.applyLeafAdjacency(scores, taskPod("p-1", "svc-edge"))
		sort.Sort(scores)
		return scores.NodeList[len(scores.NodeList)-1].NodeID
	}

	// The adjacent candidate trails by 20 packing points. Its 24-point boost
	// (8.0 raw x the pinned 3.0) overtakes the distant node; at weight 1.0 the
	// same gap is only 8 points and the packing score stands.
	assert.Equal(t, best(3.0), "node-adjacent")
	assert.Equal(t, best(1.0), "node-distant")
}

// The production fleet writes the vendor leaf path, not per-tier labels. The
// domain registry turns that string into a compute domain and its tail into the
// leaf, so the score reads the same topology the gate and the audit read.
func TestApplyLeafAdjacencyReadsTheVendorPathThroughTheRegistry(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	reg, err := plane.ParseRegistry("plane: compute\ndomains:\n  - name: PF-SZ01\n    match: DC1_FB13\n")
	assert.NilError(t, err)
	s.domainRegistry.Store(reg)

	path := func(name, superpod, rack, path string) *corev1.Node {
		node := topoNode(name, superpod, rack, name)
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Labels["topology.ascend.huawei.com/path"] = path
		return node
	}
	home := path("node-home", "1", "1", "leaf-SZPX06R1-DCN-DC1_FB13-LF46_A")
	neighbour := path("node-neighbour", "1", "1", "leaf-SZPX06R2-DCN-DC1_FB13-LF46_A")
	distant := path("node-distant", "1", "2", "leaf-SZPX06R3-DCN-DC2_FB99-LF01_A")
	for _, node := range []*corev1.Node{home, neighbour, distant} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(home, neighbour, distant)
	s.podManager.AddPod(taskPod("p-0", "svc-path"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(home, 1), nodeScore(neighbour, 1), nodeScore(distant, 1))
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-path"))

	// The leaf tier is the path tail, so ..._A and ..._A on the same mapped
	// domain are neighbours even on different spine rows, and the other domain
	// is not.
	assert.Equal(t, scoreOf(scores, "node-neighbour"), float32(1)+8.0*3.0)
	assert.Equal(t, scoreOf(scores, "node-distant"), float32(1))
}

// HAMi reads the winner off the last element of the score list, and under
// `spread` that element holds the lowest score. Without inverting the boost it
// would push the adjacent node to the back, so the topology layer would
// actively separate the task on a spread fleet - the same trap the
// policy-neutral device scorers avoid by negating their weight.
func TestApplyLeafAdjacencyInvertsTheBoostUnderSpreadPolicy(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	anchor := computeLeafNode(topoNode("node-anchor", "1", "1", "1"), "PF-1", "LF-1")
	adjacent := computeLeafNode(topoNode("node-adjacent", "1", "1", "2"), "PF-1", "LF-1")
	distant := computeLeafNode(topoNode("node-distant", "1", "2", "3"), "PF-1", "LF-2")
	for _, node := range []*corev1.Node{anchor, adjacent, distant} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(anchor, adjacent, distant)
	s.podManager.AddPod(taskPod("p-0", "svc-spread"), "node-anchor", device.PodDevices{})

	scores := &policy.NodeScoreList{
		Policy:   util.NodeSchedulerPolicySpread.String(),
		NodeList: []*policy.NodeScore{nodeScore(adjacent, 10), nodeScore(distant, 10)},
	}
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-spread"))
	sort.Sort(scores)

	assert.Equal(t, scoreOf(scores, "node-adjacent"), float32(10)-8.0*3.0,
		"under spread the leaf boost has to lower the score, because the winner is the lowest")
	assert.Equal(t, scores.NodeList[len(scores.NodeList)-1].NodeID, "node-adjacent")
}

// The two layers composed, which is what their division of labour claims: the
// soft score may reorder candidates the gate ranks equally, and must not rescue
// a candidate the gate cuts. One function calling the other in the Filter order
// is the only place this is observable at this layer.
func TestLeafAdjacencyOrdersWithinTheGateTier(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	withTopologyScoreWeight(t, 3.0)

	// The task sits on node-anchor, which is not a candidate this round. The
	// two candidates in its superpod are on different racks, so the gate ranks
	// them in one tier and only the leaf score can tell them apart; the third
	// candidate is in another superpod and is cut however it scores.
	anchor := computeLeafNode(topoNode("node-anchor", "1", "1", "1"), "PF-1", "LF-1")
	adjacent := computeLeafNode(topoNode("node-adjacent", "1", "2", "2"), "PF-1", "LF-1")
	distant := computeLeafNode(topoNode("node-distant", "1", "3", "3"), "PF-1", "LF-2")
	foreign := computeLeafNode(topoNode("node-foreign", "2", "4", "4"), "PF-1", "LF-1")
	for _, node := range []*corev1.Node{anchor, adjacent, distant, foreign} {
		registerTopoNode(t, s, node)
	}
	s.nodeLister = nodeListerWith(anchor, adjacent, distant, foreign)
	s.podManager.AddPod(taskPod("p-0", "svc-compose"), "node-anchor", device.PodDevices{})

	scores := scoreList(
		nodeScore(adjacent, 60),
		nodeScore(distant, 80),
		nodeScore(foreign, 90),
	)
	s.applyLeafAdjacency(scores, taskPod("p-1", "svc-compose"))
	sort.Sort(scores)
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("p-1", "svc-compose"), nil)

	assert.Assert(t, sameSet(droppedIDs(dropped), []string{"node-foreign"}),
		"the leaf boost must not keep a foreign superpod in: order=%v dropped=%v", orderOf(scores), droppedIDs(dropped))
	assert.Equal(t, scores.NodeList[len(scores.NodeList)-1].NodeID, "node-adjacent",
		"within one gate tier the leaf-adjacent node must win, order=%v", orderOf(scores))
}
