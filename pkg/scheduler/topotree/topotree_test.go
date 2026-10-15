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

package topotree

import (
	"reflect"
	"testing"
)

func entry(nodeID string, sp, rack int64, score float64) Entry {
	return Entry{NodeID: nodeID, SuperPodID: sp, RackID: rack, Score: score}
}

// HAMi picks the best node as the LAST element of the score list
// (pkg/scheduler/scheduler.go sorts then reads NodeList[len-1]), so every
// expectation below is written with the winner at the end.
func TestReorderThreeTierPrefersSameRack(t *testing.T) {
	// Candidate set of an A5-style cluster: one node already in the occupied
	// rack, one in another rack of the same superpod, one in a foreign
	// superpod, one plain node.
	candidates := []Entry{
		entry("plain-1", TopoIDInvalid, TopoIDInvalid, 90),
		entry("sp2-r5", 2, 5, 80),
		entry("sp1-r2", 1, 2, 70),
		entry("sp1-r1", 1, 1, 60),
	}
	// The service already runs on sp1/rack1. Note its score is the lowest of
	// all candidates: topology must be able to override the plain score.
	occupied := CollectOccupied([]ScheduledPod{
		{NodeID: "node-x", SuperPodID: 1, RackID: 1},
	})

	got := Reorder(candidates, occupied)
	want := []string{"plain-1", "sp2-r5", "sp1-r2", "sp1-r1"}
	if ids := nodeIDs(got); !reflect.DeepEqual(ids, want) {
		t.Fatalf("Reorder() order = %v, want %v", ids, want)
	}
	if got[len(got)-1].NodeID != "sp1-r1" {
		t.Fatalf("best node must be last, got %q", got[len(got)-1].NodeID)
	}
}

func TestReorderTwoTierHasNoRackStep(t *testing.T) {
	candidates := []Entry{
		entry("sp2", 2, TopoIDInvalid, 90),
		entry("sp1", 1, TopoIDInvalid, 60),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1, RackID: TopoIDInvalid}})

	got := Reorder(candidates, occupied)
	if got[len(got)-1].NodeID != "sp1" {
		t.Fatalf("same-superpod node should win on a two-tier cluster, order=%v", nodeIDs(got))
	}
	if len(got) != 2 {
		t.Fatalf("Reorder must keep every candidate, got %v", nodeIDs(got))
	}
}

func TestReorderKeepsScoreOrderWithinTier(t *testing.T) {
	// Two nodes in the same rack, scored by the existing logic. The incoming
	// slice is already sorted best-last, so relative order must survive.
	candidates := []Entry{
		entry("same-rack-low", 1, 1, 55),
		entry("same-rack-high", 1, 1, 65),
		entry("foreign-sp", 7, 3, 99),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1, RackID: 1}})

	got := Reorder(candidates, occupied)
	want := []string{"foreign-sp", "same-rack-low", "same-rack-high"}
	if ids := nodeIDs(got); !reflect.DeepEqual(ids, want) {
		t.Fatalf("within-tier order = %v, want %v", ids, want)
	}
}

func TestReorderFirstPodOfServiceLeavesOrderUntouched(t *testing.T) {
	candidates := []Entry{
		entry("sp1", 1, 1, 60),
		entry("sp2", 2, 2, 70),
		entry("plain", TopoIDInvalid, TopoIDInvalid, 99),
	}
	got := Reorder(candidates, CollectOccupied(nil))
	// Nothing is occupied yet: only the plain node is demoted, the superpod
	// candidates keep their score order.
	want := []string{"plain", "sp1", "sp2"}
	if ids := nodeIDs(got); !reflect.DeepEqual(ids, want) {
		t.Fatalf("empty occupancy = %v, want %v", ids, want)
	}
}

func TestReorderIsIdempotent(t *testing.T) {
	candidates := []Entry{
		entry("plain", TopoIDInvalid, TopoIDInvalid, 90),
		entry("sp1-r2", 1, 2, 70),
		entry("sp1-r1", 1, 1, 60),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1, RackID: 1}})

	once := Reorder(candidates, occupied)
	twice := Reorder(once, occupied)
	if !reflect.DeepEqual(nodeIDs(once), nodeIDs(twice)) {
		t.Fatalf("Reorder not idempotent: %v then %v", nodeIDs(once), nodeIDs(twice))
	}
}

func TestHasRackTierIsAutoDetected(t *testing.T) {
	tests := []struct {
		name       string
		candidates []Entry
		want       bool
	}{
		{"any valid rack enables the rack step", []Entry{
			entry("a", 1, TopoIDInvalid, 1),
			entry("b", 1, 4, 2),
		}, true},
		{"all invalid means two-tier", []Entry{
			entry("a", 1, TopoIDInvalid, 1),
			entry("b", 2, TopoIDInvalid, 2),
		}, false},
		{"no candidates", nil, false},
		{"rack valid but superpod invalid is not a rack tier", []Entry{
			entry("a", TopoIDInvalid, 4, 1),
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasRackTier(tc.candidates); got != tc.want {
				t.Errorf("HasRackTier() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1, RackID: 1}})
	tests := []struct {
		name     string
		cand     Entry
		hasRack  bool
		wantTier Tier
	}{
		{"same rack", entry("a", 1, 1, 0), true, TierSameRack},
		{"same superpod other rack", entry("b", 1, 2, 0), true, TierSameSuperPod},
		{"other superpod", entry("c", 2, 1, 0), true, TierOtherSuperPod},
		{"no topology at all", entry("d", TopoIDInvalid, TopoIDInvalid, 0), true, TierNoTopology},
		{
			// A node whose rack parsed but whose superpod did not cannot be
			// trusted, otherwise a broken annotation could win the reorder.
			"rack occupied but superpod unknown",
			entry("e", TopoIDInvalid, 1, 0),
			true,
			TierNoTopology,
		},
		{"two-tier ignores the rack id", entry("f", 1, 1, 0), false, TierSameSuperPod},
		{"first pod: nothing occupied", entry("g", 5, 5, 0), true, TierOtherSuperPod},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.cand, occupied, tc.hasRack); got != tc.wantTier {
				t.Errorf("Classify(%s) = %v, want %v", tc.cand.NodeID, got, tc.wantTier)
			}
		})
	}
}

func TestCollectOccupied(t *testing.T) {
	occ := CollectOccupied([]ScheduledPod{
		{NodeID: "n1", SuperPodID: 1, RackID: 2},
		{NodeID: "n2", SuperPodID: 1, RackID: TopoIDInvalid},
		{NodeID: "n3", SuperPodID: TopoIDInvalid, RackID: TopoIDInvalid},
	})
	if !occ.SuperPodIDs[1] {
		t.Errorf("superpod 1 should be occupied: %v", occ.SuperPodIDs)
	}
	if occ.SuperPodIDs[TopoIDInvalid] {
		t.Errorf("invalid superpod ids must not enter the set: %v", occ.SuperPodIDs)
	}
	if !occ.RackKeys[RackKey(1, 2)] {
		t.Errorf("rack 1:2 should be occupied: %v", occ.RackKeys)
	}
	if len(occ.RackKeys) != 1 {
		t.Errorf("only one valid rack expected, got %v", occ.RackKeys)
	}
}

func TestReorderNeverDropsOrDuplicatesCandidates(t *testing.T) {
	candidates := []Entry{
		entry("a", 1, 1, 10),
		entry("b", 1, 1, 20),
		entry("c", 2, 3, 30),
		entry("d", TopoIDInvalid, TopoIDInvalid, 40),
		entry("e", 3, TopoIDInvalid, 50),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 3, RackID: 9}})

	got := Reorder(candidates, occupied)
	if len(got) != len(candidates) {
		t.Fatalf("len = %d, want %d", len(got), len(candidates))
	}
	seen := map[string]int{}
	for _, e := range got {
		seen[e.NodeID]++
	}
	for _, c := range candidates {
		if seen[c.NodeID] != 1 {
			t.Errorf("node %q appears %d times in %v", c.NodeID, seen[c.NodeID], nodeIDs(got))
		}
	}
}

func TestGateDropsForeignSuperpods(t *testing.T) {
	candidates := []Entry{
		entry("sp1-a", 1, TopoIDInvalid, 40),
		entry("sp2-b", 2, TopoIDInvalid, 80),
		entry("sp3-c", 3, TopoIDInvalid, 90),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1, RackID: TopoIDInvalid}})

	kept, dropped := Gate(candidates, occupied)
	if ids := nodeIDs(kept); !reflect.DeepEqual(ids, []string{"sp1-a"}) {
		t.Fatalf("kept = %v, want [sp1-a]", ids)
	}
	if ids := nodeIDs(dropped); !reflect.DeepEqual(ids, []string{"sp2-b", "sp3-c"}) {
		t.Fatalf("dropped = %v, want [sp2-b sp3-c] in incoming order", ids)
	}
}

// A node whose plugin reports no topology is not evidence that it sits in
// another superpod. Dropping those would lock out every node on a mixed or
// partially upgraded cluster, so they stay candidates and only lose the
// ordering contest.
func TestGateExemptsNodesWithoutTopology(t *testing.T) {
	candidates := []Entry{
		entry("plain", TopoIDInvalid, TopoIDInvalid, 95),
		entry("sp1", 1, TopoIDInvalid, 40),
		entry("sp2", 2, TopoIDInvalid, 80),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1}})

	kept, dropped := Gate(candidates, occupied)
	if ids := nodeIDs(kept); !reflect.DeepEqual(ids, []string{"plain", "sp1"}) {
		t.Fatalf("kept = %v, want [plain sp1]", ids)
	}
	if ids := nodeIDs(dropped); !reflect.DeepEqual(ids, []string{"sp2"}) {
		t.Fatalf("dropped = %v, want [sp2]", ids)
	}
}

// The first pod of a service, and any pod in a cluster where nothing is
// placed yet, has no domain to converge into. Gating then would reject the
// whole cluster, so the gate is inert while occupancy is empty.
func TestGateIdleClusterKeepsEveryCandidate(t *testing.T) {
	candidates := []Entry{
		entry("sp1", 1, TopoIDInvalid, 40),
		entry("sp2", 2, TopoIDInvalid, 80),
		entry("plain", TopoIDInvalid, TopoIDInvalid, 10),
	}

	kept, dropped := Gate(candidates, Occupied{})
	if len(kept) != len(candidates) {
		t.Fatalf("kept = %v, want every candidate", nodeIDs(kept))
	}
	if len(dropped) != 0 {
		t.Fatalf("dropped = %v, want none", nodeIDs(dropped))
	}
}

func TestGateKeepsEveryDomainTheServiceAlreadyHolds(t *testing.T) {
	candidates := []Entry{
		entry("sp1", 1, TopoIDInvalid, 40),
		entry("sp2", 2, TopoIDInvalid, 80),
		entry("sp3", 3, TopoIDInvalid, 90),
	}
	occupied := CollectOccupied([]ScheduledPod{
		{NodeID: "a", SuperPodID: 1},
		{NodeID: "b", SuperPodID: 2},
	})

	kept, dropped := Gate(candidates, occupied)
	if ids := nodeIDs(kept); !reflect.DeepEqual(ids, []string{"sp1", "sp2"}) {
		t.Fatalf("kept = %v, want [sp1 sp2]", ids)
	}
	if ids := nodeIDs(dropped); !reflect.DeepEqual(ids, []string{"sp3"}) {
		t.Fatalf("dropped = %v, want [sp3]", ids)
	}
}

// The rack narrows ordering, never the candidate set: rejecting a node for
// sitting in another rack of the superpod the service already occupies would
// turn a bandwidth preference into a capacity constraint.
func TestGateDoesNotNarrowToTheOccupiedRack(t *testing.T) {
	candidates := []Entry{
		entry("sp1-r1", 1, 1, 40),
		entry("sp1-r2", 1, 2, 60),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1, RackID: 1}})

	kept, dropped := Gate(candidates, occupied)
	if ids := nodeIDs(kept); !reflect.DeepEqual(ids, []string{"sp1-r1", "sp1-r2"}) {
		t.Fatalf("kept = %v, want both racks of the occupied superpod", ids)
	}
	if len(dropped) != 0 {
		t.Fatalf("dropped = %v, want none", nodeIDs(dropped))
	}
}

func TestGateKeepsIncomingOrderWithinTheDomain(t *testing.T) {
	// The caller hands in an already score-sorted list and reads the last
	// element, so Gate must not disturb binpack/spread ordering.
	candidates := []Entry{
		entry("low", 1, TopoIDInvalid, 20),
		entry("mid", 1, TopoIDInvalid, 50),
		entry("high", 1, TopoIDInvalid, 70),
	}
	occupied := CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1}})

	kept, _ := Gate(candidates, occupied)
	if ids := nodeIDs(kept); !reflect.DeepEqual(ids, []string{"low", "mid", "high"}) {
		t.Fatalf("kept = %v, want the incoming score order preserved", ids)
	}
}

func TestGateEmptyCandidates(t *testing.T) {
	kept, dropped := Gate(nil, CollectOccupied([]ScheduledPod{{NodeID: "n", SuperPodID: 1}}))
	if len(kept) != 0 || len(dropped) != 0 {
		t.Fatalf("kept=%v dropped=%v, want both empty", nodeIDs(kept), nodeIDs(dropped))
	}
}

func nodeIDs(entries []Entry) []string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.NodeID)
	}
	return ids
}
