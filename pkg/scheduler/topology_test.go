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
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("decode-0", "svc-a"))

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
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("p-1", "svc-b"))

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
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("decode-0", "svc-f"))

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
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("p-1", "svc-g"))

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
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("p-1", "svc-h"))

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
			s.podManager.AddPod(inferPod("p-0", "svc-j"), "node-in-sp", device.PodDevices{})

			fresh := inferPod("p-1", "svc-j")
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
	s.podManager.AddPod(inferPod("prefill-0", "svc-k"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(otherRack, 10), nodeScore(home, 20), nodeScore(foreign, 30))
	dropped, outcome := s.applySuperpodPolicy(scores, inferPod("decode-0", "svc-k"))

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

// The first pod of a service, and any pod in a cluster nothing has placed yet,
// has no domain to converge into. Saying "converged into superpod(s) [none]"
// would read as a bug to the operator, so the clause has to name the absence
// instead of pretending an empty set is a domain.
func TestApplySuperpodOutcomeNamesAnEmptyDomain(t *testing.T) {
	tests := []struct {
		name        string
		service     string
		peerService string
		want        string
		deny        string
	}{{
		name:    "labeled pod with no placement yet",
		service: "svc-m",
		want:    "service=svc-m has no placement in any superpod yet",
		deny:    "converged",
	}, {
		name:        "labeled pod once a peer of the same service holds a superpod",
		service:     "svc-m",
		peerService: "svc-m",
		want:        "service=svc-m converged into superpod(s) [1]",
		deny:        "no placement in any superpod yet",
	}, {
		name: "unlabeled pod in an empty cluster",
		want: "no superpod is occupied yet",
		deny: "preferring occupied",
	}, {
		name:        "unlabeled pod with a superpod in use",
		peerService: "svc-peer",
		want:        "preferring occupied superpod(s) [1]",
		deny:        "no superpod is occupied yet",
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScheduler()
			t.Cleanup(func() { close(s.stopCh) })

			home := topoNode("node-home", "1", "1", "1")
			other := topoNode("node-other", "2", "2", "2")
			registerTopoNode(t, s, home)
			registerTopoNode(t, s, other)
			if tc.peerService != "" {
				s.podManager.AddPod(inferPod("peer-0", tc.peerService), "node-home", device.PodDevices{})
			}

			scores := scoreList(nodeScore(other, 10), nodeScore(home, 20))
			_, outcome := s.applySuperpodPolicy(scores, inferPod("new-0", tc.service))

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
	s.podManager.AddPod(inferPod("other-0", "svc-l"), "node-used-sp", device.PodDevices{})

	// Binpack puts the emptier node last, which is where the winner is read.
	scores := scoreList(nodeScore(usedSP, 90), nodeScore(freshSP, 10))
	scores.Policy = util.NodeSchedulerPolicyBinpack.String()
	dropped, outcome := s.applySuperpodPolicy(scores, inferPod("unlabeled-0", ""))

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
	_, outcome := s.applySuperpodPolicy(scores, inferPod("plain-0", ""))

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
	s.podManager.AddPod(inferPod("other-0", "svc-i"), "node-used-sp", device.PodDevices{})

	scores := scoreList(nodeScore(usedSP, 15), nodeScore(freshSP, 90))
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("anonymous-pod", ""))

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

	dropped, _ := s.applySuperpodPolicy(scores, inferPod("first-pod", "svc-j"))

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

	s.podManager.AddPod(inferPod("prefill-0", "svc-live"), "node-anchor", device.PodDevices{})

	scores := scoreList(nodeScore(stale, 10), nodeScore(plain, 20))
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("decode-0", "svc-live"))

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
	s.podManager.AddPod(inferPod("prefill-0", "svc-live"), "node-sp2", device.PodDevices{})

	scores := scoreList(nodeScore(foreign, 30), nodeScore(home, 10))
	dropped, _ := s.applySuperpodPolicy(scores, inferPod("decode-0", "svc-live"))

	if !sameSet(droppedIDs(dropped), []string{"node-sp1"}) {
		t.Fatalf("dropped = %v, want node-sp1 gated from the snapshot annotations", droppedIDs(dropped))
	}
}

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
	dropped, _ := s.applySuperpodPolicy(scores, self)

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
