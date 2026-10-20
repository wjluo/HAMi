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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

func topoNode(name string, superpod, rack, server string) *corev1.Node {
	annos := map[string]string{}
	if superpod != "" {
		annos[util.TopoSuperPodIDAnnotationKey] = superpod
	}
	if rack != "" {
		annos[util.TopoRackIDAnnotationKey] = rack
	}
	if server != "" {
		annos[util.TopoServerIDAnnotationKey] = server
	}
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annos}}
}

// nodeManager.addNode ignores a NodeInfo without devices, so fixtures must
// register one the way a device plugin does.
func registerTopoNode(t *testing.T, s *Scheduler, node *corev1.Node) {
	t.Helper()
	s.addNode(node.Name, &device.NodeInfo{
		ID:   node.Name,
		Node: node,
		Devices: map[string][]device.DeviceInfo{
			"vendor": {{ID: node.Name + "-device"}},
		},
	})
}

func inferPod(name, serviceID string) *corev1.Pod {
	labels := map[string]string{}
	if serviceID != "" {
		labels[util.InferServiceIDLabel] = serviceID
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    labels,
			// Real pods always carry a UID and self-exclusion keys off it, so the
			// fixtures must too.
			UID: k8stypes.UID("uid-" + name),
		},
	}
}

func scoreList(entries ...*policy.NodeScore) *policy.NodeScoreList {
	return &policy.NodeScoreList{NodeList: entries, Policy: util.NodeSchedulerPolicyBinpack.String()}
}

func nodeScore(node *corev1.Node, score float32) *policy.NodeScore {
	return &policy.NodeScore{NodeID: node.Name, Node: node, Score: score}
}

func orderOf(scores *policy.NodeScoreList) []string {
	ids := make([]string, 0, len(scores.NodeList))
	for _, ns := range scores.NodeList {
		ids = append(ids, ns.NodeID)
	}
	return ids
}

// withSuperpodPolicy points the cluster-wide switch at value for one test and
// puts the default back afterwards.
func withSuperpodPolicy(t *testing.T, value string) {
	t.Helper()
	previous := config.SuperpodSchedulerPolicy
	config.SuperpodSchedulerPolicy = value
	t.Cleanup(func() { config.SuperpodSchedulerPolicy = previous })
}

func droppedIDs(dropped map[string]string) []string {
	ids := make([]string, 0, len(dropped))
	for nodeID := range dropped {
		ids = append(ids, nodeID)
	}
	return ids
}

// The scheduler picks NodeList[len-1], so "the winner ends up last" is the
// assertion that matters at this layer.
func TestApplySuperpodPolicySameRackWinsAndForeignIsGated(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	sameRack := topoNode("node-same-rack", "1", "1", "1")
	otherRack := topoNode("node-other-rack", "1", "2", "2")
	otherSP := topoNode("node-other-sp", "2", "5", "3")
	plain := topoNode("node-plain", "", "", "")

	for _, node := range []*corev1.Node{sameRack, otherRack, otherSP, plain} {
		registerTopoNode(t, s, node)
	}
	// The service already runs on node-same-rack, whose own score is the
	// worst of the four: topology has to override it.
	s.podManager.AddPod(inferPod("prefill-0", "svc-a"), "node-same-rack", device.PodDevices{})

	scores := scoreList(
		nodeScore(sameRack, 10),
		nodeScore(otherRack, 20),
		nodeScore(otherSP, 30),
		nodeScore(plain, 40),
	)
	dropped := s.applySuperpodPolicy(scores, inferPod("decode-0", "svc-a"))

	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-same-rack" {
		t.Fatalf("best node = %q, want node-same-rack (order=%v)", got, orderOf(scores))
	}
	// The foreign superpod is cut; the rack within the domain is only ordered,
	// and a node that reports no topology is exempt rather than foreign.
	if want := []string{"node-other-sp"}; !sameSet(droppedIDs(dropped), want) {
		t.Errorf("dropped = %v, want %v", droppedIDs(dropped), want)
	}
	if want := []string{"node-plain", "node-other-rack", "node-same-rack"}; !equal(orderOf(scores), want) {
		t.Errorf("order = %v, want %v", orderOf(scores), want)
	}
	if !strings.Contains(dropped["node-other-sp"], "superpod 2") {
		t.Errorf("reason does not name the offending domain: %q", dropped["node-other-sp"])
	}
}

func TestApplySuperpodPolicyTwoTierCluster(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	inSP := topoNode("node-in-sp", "1", "", "1")
	elsewhere := topoNode("node-elsewhere", "2", "", "2")
	registerTopoNode(t, s, inSP)
	registerTopoNode(t, s, elsewhere)
	s.podManager.AddPod(inferPod("p-0", "svc-b"), "node-in-sp", device.PodDevices{})

	scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
	dropped := s.applySuperpodPolicy(scores, inferPod("p-1", "svc-b"))

	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-in-sp" {
		t.Fatalf("best node = %q, want node-in-sp on a rack-less cluster (order=%v)", got, orderOf(scores))
	}
	if len(scores.NodeList) != 1 || !sameSet(droppedIDs(dropped), []string{"node-elsewhere"}) {
		t.Errorf("order = %v, dropped = %v; want only node-in-sp left", orderOf(scores), droppedIDs(dropped))
	}
}

// Strictness means what it says: when the domain the workload converges into
// holds no candidate, the pod gets no node instead of a foreign one.
func TestApplySuperpodPolicyForeignOnlyLeavesNoCandidate(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	host := topoNode("node-hosting-service", "1", "1", "1")
	foreignA := topoNode("node-fa", "2", "2", "2")
	foreignB := topoNode("node-fb", "3", "3", "3")
	registerTopoNode(t, s, host)
	s.podManager.AddPod(inferPod("prefill-0", "svc-f"), "node-hosting-service", device.PodDevices{})

	scores := scoreList(nodeScore(foreignA, 80), nodeScore(foreignB, 90))
	dropped := s.applySuperpodPolicy(scores, inferPod("decode-0", "svc-f"))

	if len(scores.NodeList) != 0 {
		t.Fatalf("order = %v, want an empty candidate list", orderOf(scores))
	}
	if !sameSet(droppedIDs(dropped), []string{"node-fa", "node-fb"}) {
		t.Errorf("dropped = %v, want both foreign nodes", droppedIDs(dropped))
	}
}

// A cluster-wide switch that is off must take the whole feature with it, not
// merely downgrade strictness.
func TestApplySuperpodPolicyDisabled(t *testing.T) {
	withSuperpodPolicy(t, util.SuperpodSchedulerPolicyDisable.String())

	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	inSP := topoNode("node-in-sp", "1", "1", "1")
	elsewhere := topoNode("node-elsewhere", "2", "2", "2")
	registerTopoNode(t, s, inSP)
	registerTopoNode(t, s, elsewhere)
	s.podManager.AddPod(inferPod("p-0", "svc-g"), "node-in-sp", device.PodDevices{})

	scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
	dropped := s.applySuperpodPolicy(scores, inferPod("p-1", "svc-g"))

	if dropped != nil {
		t.Errorf("dropped = %v, want none", droppedIDs(dropped))
	}
	if want := []string{"node-in-sp", "node-elsewhere"}; !equal(orderOf(scores), want) {
		t.Errorf("order = %v, want the incoming score order (binpack decides)", want)
	}
}

// --superpod-scheduler-policy is a free-form string, so an unrecognized value
// keeps the documented default instead of quietly disabling convergence.
func TestApplySuperpodPolicyUnrecognizedValueStaysEnabled(t *testing.T) {
	withSuperpodPolicy(t, "bananas")

	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	inSP := topoNode("node-in-sp", "1", "", "1")
	elsewhere := topoNode("node-elsewhere", "2", "", "2")
	registerTopoNode(t, s, inSP)
	registerTopoNode(t, s, elsewhere)
	s.podManager.AddPod(inferPod("p-0", "svc-h"), "node-in-sp", device.PodDevices{})

	scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
	dropped := s.applySuperpodPolicy(scores, inferPod("p-1", "svc-h"))

	if !sameSet(droppedIDs(dropped), []string{"node-elsewhere"}) {
		t.Errorf("dropped = %v, want the foreign superpod gated out", droppedIDs(dropped))
	}
}

// A pod that names no service has no statement of who must share its domain,
// so it is ordered towards the superpods already in use instead of being
// restricted to them.
func TestApplySuperpodPolicyUnlabeledPodPrefersUsedDomain(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	usedSP := topoNode("node-used-sp", "1", "1", "1")
	freshSP := topoNode("node-fresh-sp", "2", "2", "2")
	registerTopoNode(t, s, usedSP)
	registerTopoNode(t, s, freshSP)
	// Occupied by some other workload entirely.
	s.podManager.AddPod(inferPod("other-0", "svc-i"), "node-used-sp", device.PodDevices{})

	scores := scoreList(nodeScore(usedSP, 15), nodeScore(freshSP, 90))
	dropped := s.applySuperpodPolicy(scores, inferPod("anonymous-pod", ""))

	if dropped != nil {
		t.Errorf("dropped = %v, want an unlabeled pod never gated", droppedIDs(dropped))
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-used-sp" {
		t.Fatalf("best node = %q, want the already-used superpod even with the worse score (order=%v)",
			got, orderOf(scores))
	}
}

// Nothing is placed yet, so there is no domain to hold the pod to.
func TestApplySuperpodPolicyIdleClusterIsInert(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	sp1 := topoNode("node-sp1", "1", "1", "1")
	sp2 := topoNode("node-sp2", "2", "2", "2")
	scores := scoreList(nodeScore(sp1, 10), nodeScore(sp2, 20))

	dropped := s.applySuperpodPolicy(scores, inferPod("first-pod", "svc-j"))

	if dropped != nil {
		t.Errorf("dropped = %v, want none on an empty cluster", droppedIDs(dropped))
	}
	if want := []string{"node-sp1", "node-sp2"}; !equal(orderOf(scores), want) {
		t.Errorf("order = %v, want it left alone", want)
	}
}

// The feature has to stay invisible where nobody turned it on: no annotations,
// or annotations a vendor plugin wrote in a form the parser cannot read.
func TestApplySuperpodPolicyNoOps(t *testing.T) {
	tests := []struct {
		name  string
		pod   *corev1.Pod
		nodes []*corev1.Node
	}{
		{
			"no node carries topology",
			inferPod("svc-pod", "svc-c"),
			[]*corev1.Node{topoNode("n1", "", "", ""), topoNode("n2", "", "", "")},
		},
		{
			"topology annotation is unparseable",
			inferPod("svc-pod", "svc-d"),
			[]*corev1.Node{topoNode("n1", "sp-one", "r-one", ""), topoNode("n2", "sp-two", "r-two", "")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScheduler()
			t.Cleanup(func() { close(s.stopCh) })
			scores := scoreList()
			for i, node := range tc.nodes {
				scores.NodeList = append(scores.NodeList, nodeScore(node, float32(i)))
			}
			before := orderOf(scores)

			dropped := s.applySuperpodPolicy(scores, tc.pod)

			if dropped != nil {
				t.Errorf("dropped = %v, want none", droppedIDs(dropped))
			}
			if after := orderOf(scores); !equal(before, after) {
				t.Errorf("order changed: before=%v after=%v", before, after)
			}
		})
	}
}

// The occupancy of the service must not include the pod being scheduled,
// otherwise a re-queued pod would pin itself to the superpod it is leaving.
func TestApplySuperpodPolicyExcludesSelf(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	sp1 := topoNode("node-sp1", "1", "1", "1")
	sp2 := topoNode("node-sp2", "2", "2", "2")
	registerTopoNode(t, s, sp1)
	registerTopoNode(t, s, sp2)

	self := inferPod("victim-pod", "svc-e")
	s.podManager.AddPod(self, "node-sp1", device.PodDevices{})

	scores := scoreList(nodeScore(sp1, 10), nodeScore(sp2, 20))
	dropped := s.applySuperpodPolicy(scores, self)

	if dropped != nil {
		t.Errorf("dropped = %v, want the departing pod not to gate the cluster out", droppedIDs(dropped))
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-sp2" {
		t.Fatalf("best node = %q, want node-sp2 so the pod can leave its old superpod (order=%v)", got, orderOf(scores))
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, id := range got {
		if seen[id] {
			return false
		}
		seen[id] = true
	}
	for _, id := range want {
		if !seen[id] {
			return false
		}
	}
	return true
}
