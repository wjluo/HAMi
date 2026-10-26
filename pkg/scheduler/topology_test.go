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
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"gotest.tools/v3/assert"

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

// nodeListerWith returns a node lister backed by an in-memory store holding
// exactly these nodes, standing in for the informer cache.
func nodeListerWith(nodes ...*corev1.Node) listerscorev1.NodeLister {
	store := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		_ = store.Add(node)
	}
	return listerscorev1.NewNodeLister(store)
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

func taskPod(name, taskID string) *corev1.Pod {
	labels := map[string]string{}
	if taskID != "" {
		labels[util.TaskIDLabel] = taskID
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
	s.podManager.AddPod(taskPod("prefill-0", "svc-a"), "node-same-rack", device.PodDevices{})

	scores := scoreList(
		nodeScore(sameRack, 10),
		nodeScore(otherRack, 20),
		nodeScore(otherSP, 30),
		nodeScore(plain, 40),
	)
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("decode-0", "svc-a"))

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
	s.podManager.AddPod(taskPod("p-0", "svc-b"), "node-in-sp", device.PodDevices{})

	scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("p-1", "svc-b"))

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
	s.podManager.AddPod(taskPod("prefill-0", "svc-f"), "node-hosting-service", device.PodDevices{})

	scores := scoreList(nodeScore(foreignA, 80), nodeScore(foreignB, 90))
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("decode-0", "svc-f"))

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
	s.podManager.AddPod(taskPod("p-0", "svc-g"), "node-in-sp", device.PodDevices{})

	scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
	dropped, outcome := s.applySuperpodPolicy(scores, taskPod("p-1", "svc-g"))

	if dropped != nil {
		t.Errorf("dropped = %v, want none", droppedIDs(dropped))
	}
	if want := []string{"node-in-sp", "node-elsewhere"}; !equal(orderOf(scores), want) {
		t.Errorf("order = %v, want the incoming score order (binpack decides)", want)
	}
	// A user who turned the feature off must not keep reading its events. An
	// empty outcome is also what the caller tests, so anything left in it - a
	// policy name, a task id - would invite a second, half-on code path.
	if outcome != (superpodOutcome{}) {
		t.Errorf("outcome = %+v, want the zero value with nothing to report", outcome)
	}
	if msg := outcome.EventSuffix(); msg != "" {
		t.Errorf("event suffix = %q, want no event text under a disabled policy", msg)
	}
}

// The per-Pod escape valve is the same switch, one pod at a time: it has to
// short-circuit everything the policy does, not merely relax the gate, or a
// workload that opted out still gets convergence reported at it.
func TestApplySuperpodPolicyDisabledPerPodLeavesNoTrace(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	inSP := topoNode("node-in-sp", "1", "1", "1")
	elsewhere := topoNode("node-elsewhere", "2", "2", "2")
	registerTopoNode(t, s, inSP)
	registerTopoNode(t, s, elsewhere)
	s.podManager.AddPod(taskPod("p-0", "svc-escape"), "node-in-sp", device.PodDevices{})

	scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
	dropped, outcome := s.applySuperpodPolicy(scores, podWithSuperpodPolicy("p-1", "svc-escape", util.SuperpodSchedulerPolicyDisable.String()))

	assert.Equal(t, len(dropped), 0)
	assert.Equal(t, outcome, superpodOutcome{}, "an opted-out pod must produce nothing to report")
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
	s.podManager.AddPod(taskPod("p-0", "svc-h"), "node-in-sp", device.PodDevices{})

	scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("p-1", "svc-h"))

	if !sameSet(droppedIDs(dropped), []string{"node-elsewhere"}) {
		t.Errorf("dropped = %v, want the foreign superpod gated out", droppedIDs(dropped))
	}
}

// A mistaken node annotation plus a strict cluster default can leave every pod
// of one service pending, and the only way out would be to switch the feature
// off for the whole cluster. The pod annotation is the per-workload escape
// valve, and it works in both directions so it can also be used to trial the
// policy on one service.
func TestApplySuperpodPolicyPodAnnotationOverridesClusterDefault(t *testing.T) {
	tests := []struct {
		name         string
		cluster      string
		pod          string
		wantDropped  []string
		wantBestNode string
	}{
		{
			name:         "off for one pod",
			cluster:      util.SuperpodSchedulerPolicyEnable.String(),
			pod:          util.SuperpodSchedulerPolicyDisable.String(),
			wantDropped:  nil,
			wantBestNode: "node-elsewhere",
		},
		{
			name:         "on for one pod",
			cluster:      util.SuperpodSchedulerPolicyDisable.String(),
			pod:          util.SuperpodSchedulerPolicyEnable.String(),
			wantDropped:  []string{"node-elsewhere"},
			wantBestNode: "node-in-sp",
		},
		{
			name:         "cluster default holds when the pod says nothing",
			cluster:      util.SuperpodSchedulerPolicyEnable.String(),
			pod:          "",
			wantDropped:  []string{"node-elsewhere"},
			wantBestNode: "node-in-sp",
		},
		{
			name:         "a typo on the pod falls back to the cluster default",
			cluster:      util.SuperpodSchedulerPolicyEnable.String(),
			pod:          "false",
			wantDropped:  []string{"node-elsewhere"},
			wantBestNode: "node-in-sp",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withSuperpodPolicy(t, tc.cluster)
			s := NewScheduler()
			t.Cleanup(func() { close(s.stopCh) })

			inSP := topoNode("node-in-sp", "1", "1", "1")
			elsewhere := topoNode("node-elsewhere", "2", "2", "2")
			registerTopoNode(t, s, inSP)
			registerTopoNode(t, s, elsewhere)
			s.podManager.AddPod(taskPod("p-0", "svc-j"), "node-in-sp", device.PodDevices{})

			fresh := taskPod("p-1", "svc-j")
			if tc.pod != "" {
				fresh.Annotations = map[string]string{util.SuperpodSchedulerPolicyAnnotationKey: tc.pod}
			}
			scores := scoreList(nodeScore(inSP, 5), nodeScore(elsewhere, 99))
			dropped, _ := s.applySuperpodPolicy(scores, fresh)

			if !sameSet(droppedIDs(dropped), tc.wantDropped) {
				t.Errorf("dropped = %v, want %v", droppedIDs(dropped), tc.wantDropped)
			}
			if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != tc.wantBestNode {
				t.Errorf("best node = %q, want %q (order=%v)", got, tc.wantBestNode, orderOf(scores))
			}
		})
	}
}

// A Pending or misplaced inference service has to be explainable from
// kubectl's own output, so the policy reports what it decided, not merely what
// it removed: the domain it converged into and the tier the winner sits at.
func TestApplySuperpodPolicyOutcomeDescribesThePlacement(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	home := topoNode("node-home", "1", "1", "1")
	otherRack := topoNode("node-other-rack", "1", "2", "2")
	foreign := topoNode("node-foreign", "2", "3", "3")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, otherRack)
	registerTopoNode(t, s, foreign)
	s.podManager.AddPod(taskPod("prefill-0", "svc-k"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(otherRack, 10), nodeScore(home, 20), nodeScore(foreign, 30))
	dropped, outcome := s.applySuperpodPolicy(scores, taskPod("decode-0", "svc-k"))

	if !sameSet(droppedIDs(dropped), []string{"node-foreign"}) {
		t.Fatalf("dropped = %v, want node-foreign", droppedIDs(dropped))
	}
	if outcome.SelectedNodeID != "node-home" {
		t.Errorf("selected = %q, want node-home (order=%v)", outcome.SelectedNodeID, orderOf(scores))
	}
	if outcome.Tier != "same-rack" {
		t.Errorf("tier = %q, want same-rack", outcome.Tier)
	}
	if outcome.SuperPodID != 1 || outcome.RackID != 1 {
		t.Errorf("domain = superpod %d rack %d, want 1/1", outcome.SuperPodID, outcome.RackID)
	}
	if outcome.Domains != "1" {
		t.Errorf("domains = %q, want the single domain this service converges into", outcome.Domains)
	}
	if outcome.Gated != 1 {
		t.Errorf("gated = %d, want 1", outcome.Gated)
	}
	msg := outcome.EventSuffix()
	for _, want := range []string{"superpod", "1", "same-rack", "svc-k"} {
		if !strings.Contains(msg, want) {
			t.Errorf("event suffix = %q, want it to mention %q", msg, want)
		}
	}
}

// An unlabeled pod, and a peer-of-a-service case, still have to name the
// absence of a domain rather than pretend an empty set is one. A *labeled*
// first pod no longer has an empty domain: the intent anchor has given it a
// deterministic superpod to open, and the clause has to say so.
func TestApplySuperpodOutcomeNamesAnEmptyDomain(t *testing.T) {
	tests := []struct {
		name     string
		task     string
		peerTask string
		want     string
		deny     string
	}{{
		name: "labeled pod with no sibling yet opens by deterministic intent",
		task: "svc-m",
		want: "by deterministic intent, no sibling bound yet",
		deny: "converged into",
	}, {
		name:     "labeled pod once a peer of the same task holds a superpod",
		task:     "svc-m",
		peerTask: "svc-m",
		want:     "task=svc-m converged into superpod(s) [1]",
		deny:     "no placement in any superpod yet",
	}, {
		name: "unlabeled pod in an empty cluster",
		want: "no superpod is occupied yet",
		deny: "preferring occupied",
	}, {
		name:     "unlabeled pod with a superpod in use",
		peerTask: "svc-peer",
		want:     "preferring occupied superpod(s) [1]",
		deny:     "no superpod is occupied yet",
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScheduler()
			t.Cleanup(func() { close(s.stopCh) })

			home := topoNode("node-home", "1", "1", "1")
			other := topoNode("node-other", "2", "2", "2")
			registerTopoNode(t, s, home)
			registerTopoNode(t, s, other)
			if tc.peerTask != "" {
				s.podManager.AddPod(taskPod("peer-0", tc.peerTask), "node-home", device.PodDevices{})
			}

			scores := scoreList(nodeScore(other, 10), nodeScore(home, 20))
			_, outcome := s.applySuperpodPolicy(scores, taskPod("new-0", tc.task))

			msg := outcome.EventSuffix()
			if !strings.Contains(msg, tc.want) {
				t.Errorf("event suffix = %q, want it to contain %q", msg, tc.want)
			}
			if strings.Contains(msg, tc.deny) {
				t.Errorf("event suffix = %q, want it not to say %q", msg, tc.deny)
			}
		})
	}
}

// A legacy label is a workload that has not been re-written yet, not a
// workload that opted out: it has to converge exactly like a taskid Pod, and
// the decision it produces has to read under the new key, because the platform
// team greps one namespace of logs, not two.
func TestApplySuperpodPolicyConvergesLegacyTaskLabels(t *testing.T) {
	for _, key := range []string{util.TaskIDLabel, util.TaskIDLabelLegacy, util.TaskIDLabelLegacyBare} {
		t.Run(key, func(t *testing.T) {
			s := NewScheduler()
			t.Cleanup(func() { close(s.stopCh) })

			home := topoNode("node-home", "1", "1", "1")
			foreign := topoNode("node-foreign", "2", "2", "2")
			registerTopoNode(t, s, home)
			registerTopoNode(t, s, foreign)

			peer := taskPod("peer-0", "task-legacy")
			peer.Labels = map[string]string{key: "task-legacy"}
			s.podManager.AddPod(peer, "node-home", device.PodDevices{})

			pod := taskPod("new-0", "task-legacy")
			pod.Labels = map[string]string{key: "task-legacy"}
			dropped, outcome := s.applySuperpodPolicy(scoreList(nodeScore(foreign, 30), nodeScore(home, 10)), pod)

			assert.Equal(t, len(dropped), 1, "the foreign superpod has to be gated out for a legacy label too")
			if !strings.Contains(outcome.EventSuffix(), "task=task-legacy") {
				t.Errorf("event suffix = %q, want the new log key regardless of the spelling read", outcome.EventSuffix())
			}
		})
	}
}

// The topology tier outranks the packing score by design, which means a user
// who asked for spread can be placed on a busier node without any explanation.
// The outcome has to say so, because that is the one question an operator asks
// after a surprising placement.
func TestApplySuperpodPolicyOutcomeRecordsTheScoreItOverrode(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	usedSP := topoNode("node-used-sp", "1", "1", "1")
	freshSP := topoNode("node-fresh-sp", "2", "2", "2")
	registerTopoNode(t, s, usedSP)
	registerTopoNode(t, s, freshSP)
	s.podManager.AddPod(taskPod("other-0", "svc-l"), "node-used-sp", device.PodDevices{})

	// Binpack puts the emptier node last, which is where the winner is read.
	scores := scoreList(nodeScore(usedSP, 90), nodeScore(freshSP, 10))
	scores.Policy = util.NodeSchedulerPolicyBinpack.String()
	dropped, outcome := s.applySuperpodPolicy(scores, taskPod("unlabeled-0", ""))

	if dropped != nil {
		t.Fatalf("dropped = %v, want an unlabeled pod never gated", droppedIDs(dropped))
	}
	if outcome.SelectedNodeID != "node-used-sp" {
		t.Fatalf("selected = %q, want the node in the superpod already in use", outcome.SelectedNodeID)
	}
	if outcome.ScorePreferred != "node-fresh-sp" || !outcome.OverrodeScore {
		t.Errorf("score preferred %q (overrode=%v), want node-fresh-sp (overrode=true)",
			outcome.ScorePreferred, outcome.OverrodeScore)
	}
	if !strings.Contains(outcome.EventSuffix(), "binpack") {
		t.Errorf("event suffix = %q, want it to name the packing policy it overrode", outcome.EventSuffix())
	}
}

// Two nodes claiming the same server index in different superpods cannot both
// be right, and a value that contradicts its peers is the failure a mistaken or
// stale annotation produces. Naming it turns "why is this service stuck in
// superpod 4" into a one-line answer.
func TestApplySuperpodPolicyOutcomeReportsContradictoryTopology(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	first := topoNode("node-a", "1", "1", "7")
	second := topoNode("node-b", "2", "2", "7")
	registerTopoNode(t, s, first)
	registerTopoNode(t, s, second)

	scores := scoreList(nodeScore(first, 10), nodeScore(second, 20))
	_, outcome := s.applySuperpodPolicy(scores, taskPod("plain-0", ""))

	if !strings.Contains(outcome.Conflict, "node-a") || !strings.Contains(outcome.Conflict, "node-b") {
		t.Errorf("conflict = %q, want both nodes sharing serverid 7 named", outcome.Conflict)
	}
	if !strings.Contains(outcome.EventSuffix(), "conflicting") {
		t.Errorf("event suffix = %q, want the contradiction surfaced to the user", outcome.EventSuffix())
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
	s.podManager.AddPod(taskPod("other-0", "svc-i"), "node-used-sp", device.PodDevices{})

	scores := scoreList(nodeScore(usedSP, 15), nodeScore(freshSP, 90))
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("anonymous-pod", ""))

	if dropped != nil {
		t.Errorf("dropped = %v, want an unlabeled pod never gated", droppedIDs(dropped))
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-used-sp" {
		t.Fatalf("best node = %q, want the already-used superpod even with the worse score (order=%v)",
			got, orderOf(scores))
	}
}

// A labeled first pod on a cluster nothing has placed yet is not left to free
// choice: the intent anchor names a deterministic superpod to open and gates the
// foreign one out, which is what stops two concurrent first pods of one service
// from opening two domains. Which superpod wins is a property of the service id,
// so the assertion is structural - exactly one node survives, in one superpod.
func TestApplySuperpodPolicyLabeledFirstPodAnchorsOnEmptyCluster(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	sp1 := topoNode("node-sp1", "1", "1", "1")
	sp2 := topoNode("node-sp2", "2", "2", "2")
	scores := scoreList(nodeScore(sp1, 10), nodeScore(sp2, 20))

	dropped, outcome := s.applySuperpodPolicy(scores, taskPod("first-pod", "svc-j"))

	if !outcome.IntentAnchor {
		t.Fatalf("IntentAnchor = false, want a labeled first pod on an empty cluster to open by intent")
	}
	if len(scores.NodeList) != 1 {
		t.Fatalf("kept = %v, want the foreign superpod gated out so one node survives", orderOf(scores))
	}
	if got := droppedIDs(dropped); len(got) != 1 {
		t.Fatalf("dropped = %v, want exactly the foreign node gated out", got)
	}
	if outcome.Domains == "" || outcome.Domains == "none" {
		t.Errorf("Domains = %q, want the anchored superpod named", outcome.Domains)
	}
	if !strings.Contains(outcome.EventSuffix(), "by deterministic intent") {
		t.Errorf("event suffix = %q, want it to name the deterministic intent", outcome.EventSuffix())
	}
}

// podWithSuperpodPolicy returns a labelled infer-service pod carrying the
// per-Pod superpod policy annotation, so a test can select enable/spill/disable
// without touching the cluster default.
func podWithSuperpodPolicy(name, serviceID, policy string) *corev1.Pod {
	pod := taskPod(name, serviceID)
	pod.Annotations = map[string]string{util.SuperpodSchedulerPolicyAnnotationKey: policy}
	return pod
}

// The spill policy is the whole-service capacity signal this layer can honestly
// observe: the service's occupied superpod offered no candidate that fits this
// pod this round (the device filter already dropped every node in it), so the
// pod would otherwise sit Pending with no self-heal. Spill trades topology for
// delivery, places the pod in a foreign superpod, and reports the domain it
// spilled out of.
func TestApplySuperpodPolicySpillPlacesForeignWhenDomainFull(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	full := topoNode("node-full", "1", "1", "1")
	foreign := topoNode("node-foreign", "2", "2", "2")
	registerTopoNode(t, s, full)
	registerTopoNode(t, s, foreign)
	// The service is anchored to superpod 1 by this bound sibling.
	s.podManager.AddPod(taskPod("svc-holder", "svc-f"), "node-full", device.PodDevices{})

	// The only candidate that reaches the filter is the foreign node: node-full
	// has already been dropped upstream for lack of capacity.
	scores := scoreList(nodeScore(foreign, 10))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithSuperpodPolicy("svc-new", "svc-f", util.SuperpodSchedulerPolicySpill.String()))

	if len(dropped) != 0 {
		t.Errorf("dropped = %v, want spill to keep the foreign node rather than reject it", droppedIDs(dropped))
	}
	if outcome.CapacitySpill != "1" {
		t.Errorf("CapacitySpill = %q, want the full superpod 1 named", outcome.CapacitySpill)
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-foreign" {
		t.Fatalf("best node = %q, want the foreign node placed rather than left pending (order=%v)", got, orderOf(scores))
	}
	if outcome.Tier != "other-superpod" {
		t.Errorf("Tier = %q, want the spill recorded as a foreign placement", outcome.Tier)
	}
	if !strings.Contains(outcome.EventSuffix(), "spilled to superpod=2") {
		t.Errorf("event suffix = %q, want it to name the spill", outcome.EventSuffix())
	}
}

// Spill only relaxes the gate under exhaustion. While a candidate inside the
// occupied domain still fits, the workload stays there and nothing is spilled,
// so the fallback cannot quietly become the default and break convergence.
func TestApplySuperpodPolicySpillStaysInDomainWhenItFits(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	home := topoNode("node-home", "1", "1", "1")
	foreign := topoNode("node-foreign", "2", "2", "2")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, foreign)
	s.podManager.AddPod(taskPod("svc-holder", "svc-f"), "node-home", device.PodDevices{})

	// node-home (in the occupied superpod) fits and is present, so the foreign
	// node is gated out exactly as under the strict policy.
	scores := scoreList(nodeScore(foreign, 20), nodeScore(home, 10))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithSuperpodPolicy("svc-new", "svc-f", util.SuperpodSchedulerPolicySpill.String()))

	if outcome.CapacitySpill != "" {
		t.Errorf("CapacitySpill = %q, want no spill when the domain still fits", outcome.CapacitySpill)
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-home" {
		t.Fatalf("best node = %q, want the in-domain node (order=%v)", got, orderOf(scores))
	}
	if !sameSet(droppedIDs(dropped), []string{"node-foreign"}) {
		t.Errorf("dropped = %v, want the foreign node gated out as usual", droppedIDs(dropped))
	}
}

// The default stays strict: without the spill annotation, a labelled pod whose
// occupied domain is full is reported as gated-out-with-no-candidate, exactly as
// before, so enabling this feature cannot strand a workload that was relying on
// strict convergence to keep it pending until capacity frees in-domain.
func TestApplySuperpodPolicyStrictDefaultStillStrandsWhenDomainFull(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	full := topoNode("node-full", "1", "1", "1")
	foreign := topoNode("node-foreign", "2", "2", "2")
	registerTopoNode(t, s, full)
	registerTopoNode(t, s, foreign)
	s.podManager.AddPod(taskPod("svc-holder", "svc-f"), "node-full", device.PodDevices{})

	scores := scoreList(nodeScore(foreign, 10))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithSuperpodPolicy("svc-new", "svc-f", util.SuperpodSchedulerPolicyEnable.String()))

	if outcome.CapacitySpill != "" {
		t.Errorf("CapacitySpill = %q, want the strict default never to spill", outcome.CapacitySpill)
	}
	if len(scores.NodeList) != 0 {
		t.Errorf("kept = %v, want the strict default to drop the foreign candidate and leave no node", orderOf(scores))
	}
	if !sameSet(droppedIDs(dropped), []string{"node-foreign"}) {
		t.Errorf("dropped = %v, want node-foreign gated out as foreign", droppedIDs(dropped))
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
			taskPod("svc-pod", "svc-c"),
			[]*corev1.Node{topoNode("n1", "", "", ""), topoNode("n2", "", "", "")},
		},
		{
			"topology annotation is unparseable",
			taskPod("svc-pod", "svc-d"),
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

			dropped, _ := s.applySuperpodPolicy(scores, tc.pod)

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
// A device plugin writes the topology annotations after it registers its
// devices, and the scheduler node cache only refreshes the Node object when the
// device list changes. Reading topology from that snapshot would keep a node
// "unknown" forever, so the scheduler must read the live node from its informer
// and fall back to the snapshot only when the informer cannot answer.
func TestApplySuperpodPolicyReadsTopologyFromListerWhenCacheIsStale(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	// The cache snapshot of node-sp1 predates the annotation write.
	stale := topoNode("node-sp1", "", "", "")
	plain := topoNode("node-plain", "", "", "")
	anchor := topoNode("node-anchor", "2", "", "")
	registerTopoNode(t, s, stale)
	registerTopoNode(t, s, plain)
	registerTopoNode(t, s, anchor)

	// The API object has since been patched with the topology, which is what
	// the informer sees.
	fresh := topoNode("node-sp1", "2", "", "")
	s.nodeLister = nodeListerWith(fresh, plain, anchor)

	s.podManager.AddPod(taskPod("prefill-0", "svc-live"), "node-anchor", device.PodDevices{})

	scores := scoreList(nodeScore(stale, 10), nodeScore(plain, 20))
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("decode-0", "svc-live"))

	if dropped != nil {
		t.Errorf("dropped = %v, want no node gated out (both candidates are usable)", droppedIDs(dropped))
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-sp1" {
		t.Fatalf("best node = %q, want node-sp1: its superpod annotation is only visible through the node informer (order=%v)",
			got, orderOf(scores))
	}
}

// A caller with no node informer wired must still see the topology carried by
// the score snapshot, otherwise the policy would silently switch itself off for
// them rather than gating on the domains it was handed.
func TestApplySuperpodPolicyFallsBackToSnapshotWithoutLister(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	if s.nodeLister != nil {
		t.Fatalf("this test needs a scheduler without a node informer")
	}

	foreign := topoNode("node-sp1", "1", "", "")
	home := topoNode("node-sp2", "2", "", "")
	registerTopoNode(t, s, foreign)
	registerTopoNode(t, s, home)
	s.podManager.AddPod(taskPod("prefill-0", "svc-live"), "node-sp2", device.PodDevices{})

	scores := scoreList(nodeScore(foreign, 30), nodeScore(home, 10))
	dropped, _ := s.applySuperpodPolicy(scores, taskPod("decode-0", "svc-live"))

	if !sameSet(droppedIDs(dropped), []string{"node-sp1"}) {
		t.Fatalf("dropped = %v, want node-sp1 gated from the snapshot annotations", droppedIDs(dropped))
	}
}

// The occupancy of the service must not include the pod being scheduled.
// A live sibling pins the service to node-sp2, which keeps the intent anchor
// from firing, so this exercises self-exclusion alone. If self counted toward
// its own service, node-sp1 would stay in play, the two superpods would both
// look occupied, and the higher-scoring sp1 could keep the pod in the superpod
// it is leaving.
func TestApplySuperpodPolicyExcludesSelf(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	sp1 := topoNode("node-sp1", "1", "1", "1")
	sp2 := topoNode("node-sp2", "2", "2", "2")
	registerTopoNode(t, s, sp1)
	registerTopoNode(t, s, sp2)

	self := taskPod("victim-pod", "svc-e")
	peer := taskPod("peer-pod", "svc-e")
	s.podManager.AddPod(self, "node-sp1", device.PodDevices{})
	s.podManager.AddPod(peer, "node-sp2", device.PodDevices{})

	scores := scoreList(nodeScore(sp2, 10), nodeScore(sp1, 20))
	dropped, _ := s.applySuperpodPolicy(scores, self)

	if !sameSet(droppedIDs(dropped), []string{"node-sp1"}) {
		t.Fatalf("dropped = %v, want self's own superpod gated out because self is excluded from occupancy", droppedIDs(dropped))
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-sp2" {
		t.Fatalf("best node = %q, want node-sp2, the peer's superpod, so the pod can leave its old superpod (order=%v)", got, orderOf(scores))
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

// B-1: the escape valve must save the pod that asked for it and nobody else.
// The live scene is this exact shape (cicpoc, svc-d4): two siblings anchored in
// superpod 1, one pod pushed into superpod 2 by a per-Pod spill annotation, and
// a fourth pod of the same task that the operator did not opt out. Counting the
// spilled placement into the domain turned [1] into [1,2] and left the strict
// pod free to follow it across the fabric - permanently, since a ratchet has no
// way back. Measured on superpod.6: "converged into superpod(s) [10,20]".
func TestApplySuperpodPolicyEscapeValvePlacementDoesNotWidenTheDomain(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	home := topoNode("node-home", "1", "1", "1")
	foreign := topoNode("node-foreign", "2", "2", "2")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, foreign)
	s.podManager.AddPod(taskPod("p-0", "svc-ratchet"), "node-home", device.PodDevices{})
	s.podManager.AddPod(
		podWithSuperpodPolicy("p-1", "svc-ratchet", util.SuperpodSchedulerPolicySpill.String()),
		"node-foreign", device.PodDevices{})

	// The foreign node scores best, so without the gate this pod lands there.
	scores := scoreList(nodeScore(home, 5), nodeScore(foreign, 99))
	dropped, outcome := s.applySuperpodPolicy(scores, taskPod("p-2", "svc-ratchet"))

	if got := outcome.Domains; got != "1" {
		t.Errorf("occupied = %q, want \"1\": an escape-valve placement must not widen the task's domain", got)
	}
	if want := []string{"node-foreign"}; !sameSet(droppedIDs(dropped), want) {
		t.Errorf("dropped = %v, want %v gated out as foreign", droppedIDs(dropped), want)
	}
	if got := scores.NodeList[len(scores.NodeList)-1].NodeID; got != "node-home" {
		t.Errorf("best node = %q, want node-home (order=%v)", got, orderOf(scores))
	}
}

// The same rule for the disable valve, which is the one that leaves no trace of
// its own: a pod placed with convergence switched off for it must still not
// become the domain the rest of the task is held to.
func TestApplySuperpodPolicyDisabledPlacementDoesNotWidenTheDomain(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	home := topoNode("node-home", "1", "1", "1")
	foreign := topoNode("node-foreign", "2", "2", "2")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, foreign)
	s.podManager.AddPod(taskPod("p-0", "svc-ratchet"), "node-home", device.PodDevices{})
	s.podManager.AddPod(
		podWithSuperpodPolicy("p-1", "svc-ratchet", util.SuperpodSchedulerPolicyDisable.String()),
		"node-foreign", device.PodDevices{})

	scores := scoreList(nodeScore(home, 5), nodeScore(foreign, 99))
	_, outcome := s.applySuperpodPolicy(scores, taskPod("p-2", "svc-ratchet"))

	assert.Equal(t, outcome.Domains, "1")
}

// B-1's cost, stated as a test: the pod that was pushed out of the domain is
// now alone there, and nothing in the domain set says so. The only honest
// counterweight to a tighter gate is to name the orphan.
func TestApplySuperpodPolicyNamesEscapedSiblingsOutsideTheDomain(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	home := topoNode("node-home", "1", "1", "1")
	foreign := topoNode("node-foreign", "2", "2", "2")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, foreign)
	s.podManager.AddPod(taskPod("p-0", "svc-orphan"), "node-home", device.PodDevices{})
	s.podManager.AddPod(
		podWithSuperpodPolicy("p-1", "svc-orphan", util.SuperpodSchedulerPolicySpill.String()),
		"node-foreign", device.PodDevices{})

	scores := scoreList(nodeScore(home, 5), nodeScore(foreign, 99))
	_, outcome := s.applySuperpodPolicy(scores, taskPod("p-2", "svc-orphan"))

	msg := outcome.EventSuffix()
	if !strings.Contains(msg, "outside") || !strings.Contains(msg, "superpod 2") {
		t.Errorf("event suffix = %q, want it to warn that a task pod sits outside the domain in superpod 2", msg)
	}
}

// B-2: a task whose domain really is several superpods is a cross-domain
// service, which under "performance first" is an incident, not a low score.
// This state is reachable from placements predating the rule above, or from a
// cluster-wide default that opted every pod out at once.
func TestApplySuperpodOutcomeWarnsOnMultiDomainService(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	home := topoNode("node-home", "1", "1", "1")
	other := topoNode("node-other", "2", "2", "2")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, other)
	s.podManager.AddPod(taskPod("p-0", "svc-split"), "node-home", device.PodDevices{})
	s.podManager.AddPod(taskPod("p-1", "svc-split"), "node-other", device.PodDevices{})

	scores := scoreList(nodeScore(home, 5), nodeScore(other, 99))
	_, outcome := s.applySuperpodPolicy(scores, taskPod("p-2", "svc-split"))

	assert.Equal(t, outcome.Domains, "1,2")
	if msg := outcome.EventSuffix(); !strings.Contains(msg, "WARNING") || !strings.Contains(msg, "2 superpod domains") {
		t.Errorf("event suffix = %q, want a warning that the task spans two domains", msg)
	}
}

// The exclusion reads the Pod's own annotation, never the cluster default: a
// fleet configured with --superpod-scheduler-policy=spill is the operator's
// decision to trade topology for delivery everywhere, and must not end up with
// an empty occupancy (which would leave every task ungated).
func TestApplySuperpodPolicyClusterWideSpillStillOccupies(t *testing.T) {
	withSuperpodPolicy(t, util.SuperpodSchedulerPolicySpill.String())

	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	foreign := topoNode("node-foreign", "2", "2", "2")
	elsewhere := topoNode("node-elsewhere", "3", "3", "3")
	registerTopoNode(t, s, foreign)
	registerTopoNode(t, s, elsewhere)
	s.podManager.AddPod(taskPod("p-0", "svc-default-spill"), "node-foreign", device.PodDevices{})

	scores := scoreList(nodeScore(foreign, 5), nodeScore(elsewhere, 99))
	dropped, outcome := s.applySuperpodPolicy(scores, taskPod("p-1", "svc-default-spill"))

	assert.Equal(t, outcome.Domains, "2", "a sibling placed under the cluster default stays part of the domain")
	assert.Equal(t, len(droppedIDs(dropped)), 1, "the third superpod is still foreign to this task")
}
