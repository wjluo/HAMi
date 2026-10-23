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
	"strconv"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func intentCandidates() []Entry {
	return []Entry{
		entry("plain", TopoIDInvalid, TopoIDInvalid, 90),
		entry("sp1-r1", 1, 1, 70),
		entry("sp1-r2", 1, 2, 60),
		entry("sp2-r5", 2, 5, 80),
	}
}

// The whole point of the intent anchor: a service with no bound sibling yet
// still gets one deterministic superpod, so two first pods of the same service
// cannot open in two superpods at once.
func TestSyntheticFirstPodOccupiedIsDeterministic(t *testing.T) {
	a := SyntheticFirstPodOccupied("svc-a", intentCandidates())
	b := SyntheticFirstPodOccupied("svc-a", intentCandidates())
	assert.DeepEqual(t, a.SuperPodIDs, b.SuperPodIDs)
	assert.DeepEqual(t, a.RackKeys, b.RackKeys)
}

// The target must not depend on the order the candidates arrive in, because two
// pods of one service are handed the same nodes in whatever order the score
// list produced.
func TestSyntheticFirstPodOccupiedIgnoresCandidateOrder(t *testing.T) {
	cands := intentCandidates()
	rev := make([]Entry, 0, len(cands))
	for i := len(cands) - 1; i >= 0; i-- {
		rev = append(rev, cands[i])
	}
	forward := SyntheticFirstPodOccupied("svc-order", cands)
	backward := SyntheticFirstPodOccupied("svc-order", rev)
	assert.DeepEqual(t, forward.SuperPodIDs, backward.SuperPodIDs)
	assert.DeepEqual(t, forward.RackKeys, backward.RackKeys)
}

// The chosen superpod is exactly the crc32 pick over the sorted candidate set,
// and is always one of the candidates, so the gate can never strand a first pod
// on a superpod that has no node in this round.
func TestSyntheticFirstPodOccupiedTargetsACandidateSuperpod(t *testing.T) {
	sps := sortedSuperPodIDs(intentCandidates())
	assert.DeepEqual(t, sps, []int64{1, 2})
	want := sps[int(crc32String("svc-a"))%len(sps)]
	got := SyntheticFirstPodOccupied("svc-a", intentCandidates())
	assert.Assert(t, got.SuperPodIDs[want], "target superpod should be the crc32 pick")
	assert.Equal(t, len(got.SuperPodIDs), 1)
}

func TestSyntheticFirstPodOccupiedNoServiceIDIsInert(t *testing.T) {
	got := SyntheticFirstPodOccupied("", intentCandidates())
	assert.Equal(t, len(got.SuperPodIDs), 0)
	assert.Equal(t, len(got.RackKeys), 0)
	assert.Equal(t, got.HasRackTier(), false)
}

// A topology-less candidate set has nothing to anchor on, so the caller keeps
// its inert-gate behaviour instead of inventing a domain.
func TestSyntheticFirstPodOccupiedNoTopologyIsInert(t *testing.T) {
	plainOnly := []Entry{
		entry("p1", TopoIDInvalid, TopoIDInvalid, 50),
		entry("p2", TopoIDInvalid, TopoIDInvalid, 60),
	}
	got := SyntheticFirstPodOccupied("svc-plain", plainOnly)
	assert.Equal(t, len(got.SuperPodIDs), 0)
}

// On a two-tier chip no candidate reports a rack, so the intent must not fabricate
// a rack tier; otherwise a three-tier cluster would be implied where the
// hardware only has superpod -> server.
func TestSyntheticFirstPodOccupiedTwoTierHasNoRack(t *testing.T) {
	twoTier := []Entry{
		entry("sp1", 1, TopoIDInvalid, 40),
		entry("sp2", 2, TopoIDInvalid, 60),
	}
	got := SyntheticFirstPodOccupied("svc-2tier", twoTier)
	assert.Equal(t, got.HasRackTier(), false)
}

func TestSyntheticFirstPodOccupiedThreeTierPicksRackWithinTarget(t *testing.T) {
	got := SyntheticFirstPodOccupied("svc-3tier", intentCandidates())
	assert.Assert(t, got.HasRackTier(), "a three-tier candidate set should anchor a rack")
	// The rack belongs to the chosen superpod only.
	var target int64
	for sp := range got.SuperPodIDs {
		target = sp
	}
	// RackKey is "<superpod>:<rack>", so every key must open with the target's
	// own id followed by the separator - that is what "belongs to the target
	// superpod" means here.
	prefix := strconv.FormatInt(target, 10) + ":"
	for key := range got.RackKeys {
		assert.Assert(t, strings.HasPrefix(key, prefix),
			"rack key %q must belong to target superpod %d", key, target)
	}
}

// End to end through the existing algorithm: the first pod, guided only by the
// intent, lands inside the intended superpod, and an unknown-topology node is
// still kept (never gated for lacking an annotation).
func TestSyntheticFirstPodOccupiedDrivesReorderAndGate(t *testing.T) {
	occ := SyntheticFirstPodOccupied("svc-e2e", intentCandidates())
	var target int64
	for sp := range occ.SuperPodIDs {
		target = sp
	}

	kept, dropped := Gate(intentCandidates(), occ)
	// No node is dropped for lacking topology.
	for _, e := range intentCandidates() {
		if e.SuperPodID < 0 {
			assert.Assert(t, containsNode(kept, e.NodeID), "unknown-topology node must stay kept")
		}
	}
	// Foreign superpods are gated out.
	for _, e := range dropped {
		assert.Assert(t, e.SuperPodID >= 0 && e.SuperPodID != target, "only foreign superpods may be dropped")
	}

	ordered := Reorder(kept, occ)
	winner := ordered[len(ordered)-1]
	assert.Equal(t, winner.SuperPodID, target, "winner must sit in the intended superpod")
}

func containsNode(entries []Entry, nodeID string) bool {
	for _, e := range entries {
		if e.NodeID == nodeID {
			return true
		}
	}
	return false
}
