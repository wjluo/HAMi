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

// Package topotree orders scheduling candidates by superpod topology, so that
// the Pods of one inference service converge into the same superpod - and, when
// the chip reports a rack tier, into the same rack.
//
// It is deliberately free of device and node-cache dependencies: callers pass
// plain entries, which keeps the algorithm testable and reusable by every
// vendor's topology reporting scheme.
package topotree

import (
	"sort"
	"strconv"

	"github.com/Project-HAMi/HAMi/pkg/util"
)

// TopoIDInvalid marks a missing or unusable topology id. Aliased from pkg/util
// so the scheduler, the algorithm and the annotation parser agree on one value.
const TopoIDInvalid = util.TopoIDInvalid

// Entry is one scheduling candidate together with its topology.
type Entry struct {
	NodeID     string
	SuperPodID int64
	RackID     int64
	Score      float64
}

// ScheduledPod is a pod of the same inference service that already occupies a
// node. Callers resolve the topology ids; this package only groups them.
type ScheduledPod struct {
	NodeID     string
	SuperPodID int64
	RackID     int64
}

// Occupied records the topology domains already taken by one inference service.
type Occupied struct {
	SuperPodIDs map[int64]bool
	RackKeys    map[string]bool
}

// Tier ranks a candidate. Higher is preferred; TierNoTopology nodes only win if
// nothing else is available.
type Tier int

const (
	TierNoTopology Tier = iota
	TierOtherSuperPod
	TierSameSuperPod
	TierSameRack
)

// RackKey scopes a rack to its superpod, because rack ids are only unique
// inside one superpod.
func RackKey(superPodID, rackID int64) string {
	return strconv.FormatInt(superPodID, 10) + ":" + strconv.FormatInt(rackID, 10)
}

// CollectOccupied builds the occupancy sets of one inference service. Pods on
// nodes without usable topology are ignored, so a broken annotation never
// constrains the schedule.
func CollectOccupied(pods []ScheduledPod) Occupied {
	occ := Occupied{
		SuperPodIDs: map[int64]bool{},
		RackKeys:    map[string]bool{},
	}
	for _, pod := range pods {
		if pod.SuperPodID < 0 {
			continue
		}
		occ.SuperPodIDs[pod.SuperPodID] = true
		if pod.RackID >= 0 {
			occ.RackKeys[RackKey(pod.SuperPodID, pod.RackID)] = true
		}
	}
	return occ
}

// HasRackTier reports whether the domains this workload converges into have a
// rack between the superpod and the server. It is the single source of that
// decision: an ordering and a diagnostic computed from two different views can
// disagree about which tier a node is in, and a candidate set that happens to
// hold no rack node for one round would otherwise downgrade a three-tier
// cluster to a two-tier one.
//
// RackKeys can only be filled by a pod on a node with a usable superpod id, so
// a rack id alone - the driver sentinel for "unknown" leaking through - never
// creates a tier.
func (o Occupied) HasRackTier() bool {
	return len(o.RackKeys) > 0
}

// Classify places one candidate into a tier. On a two-tier chip the rack
// dimension is skipped entirely, leaving same-superpod as the best outcome. The
// rack tier comes from the occupied domains, so a caller cannot hand Classify a
// view that disagrees with Reorder.
func Classify(cand Entry, occ Occupied) Tier {
	if cand.SuperPodID < 0 {
		return TierNoTopology
	}
	hasRack := occ.HasRackTier()
	if hasRack && cand.RackID >= 0 && occ.RackKeys[RackKey(cand.SuperPodID, cand.RackID)] {
		return TierSameRack
	}
	if occ.SuperPodIDs[cand.SuperPodID] {
		return TierSameSuperPod
	}
	return TierOtherSuperPod
}

// Reorder returns the candidates ordered least- to most-preferred. The caller
// picks the last element, matching HAMi's convention that the best node sits at
// the end of a sorted score list (see scheduler.go). Within a tier the incoming
// order is preserved, so the scores computed by the existing logic still decide.
func Reorder(candidates []Entry, occ Occupied) []Entry {
	ordered := make([]Entry, len(candidates))
	copy(ordered, candidates)
	sort.SliceStable(ordered, func(i, j int) bool {
		return Classify(ordered[i], occ) < Classify(ordered[j], occ)
	})
	return ordered
}

// Gate separates the candidates that lie in the occupied domain from those in a
// foreign superpod, keeping the incoming order in both halves.
//
// Two groups are never dropped. Nodes reporting no topology stay candidates,
// because an absent annotation means the plugin did not say where the node is,
// not that the node is elsewhere - dropping them would lock out a mixed or
// partially upgraded cluster. And while nothing is occupied there is no domain
// to hold a workload to, so the gate is inert.
//
// The rack tier is deliberately not part of this: a rack narrows preference,
// and refusing a node for sitting in another rack of the same superpod would
// turn a bandwidth wish into a capacity wall.
func Gate(candidates []Entry, occ Occupied) (kept, dropped []Entry) {
	if len(occ.SuperPodIDs) == 0 {
		return candidates, nil
	}
	kept = make([]Entry, 0, len(candidates))
	for _, cand := range candidates {
		if cand.SuperPodID < 0 || occ.SuperPodIDs[cand.SuperPodID] {
			kept = append(kept, cand)
			continue
		}
		dropped = append(dropped, cand)
	}
	return kept, dropped
}
