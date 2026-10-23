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
	"hash/crc32"
	"sort"
)

// SyntheticFirstPodOccupied gives the first pod of an inference service a
// deterministic home before any sibling has bound.
//
// A reactive anchor - "the superpod where the first pod happened to bind" -
// cannot hold concurrent first pods of one service: two pods evaluated in the
// same round, with nothing occupied yet, each keep their own best node and the
// service opens in two superpods at once. This makes the choice a pure function
// of the service id and the candidate superpods, so every pod of the service
// derives the same target with no shared state and no coordination.
//
// The result is fed straight back through Gate/Classify/Reorder as if that
// domain were already occupied, so the intent rides the existing tier logic
// instead of introducing a separate scoring path. Nodes that report no
// topology are untouched: this only names a superpod to open and to hold, it
// never drops a node for lacking an annotation.
//
// It returns an empty Occupied - so the caller keeps its current inert-gate
// behaviour - when there is nothing to anchor on: no service id, or no
// candidate node carrying a usable superpod id.
func SyntheticFirstPodOccupied(serviceID string, candidates []Entry) Occupied {
	empty := Occupied{SuperPodIDs: map[int64]bool{}, RackKeys: map[string]bool{}}
	if serviceID == "" {
		return empty
	}
	// The target is drawn from the superpods actually present among the
	// candidates, so the chosen domain always has at least one node that fits
	// this pod; the gate can therefore never strand a first pod on an empty
	// superpod.
	sps := sortedSuperPodIDs(candidates)
	if len(sps) == 0 {
		return empty
	}
	target := sps[int(crc32String(serviceID))%len(sps)]
	occ := Occupied{SuperPodIDs: map[int64]bool{target: true}, RackKeys: map[string]bool{}}
	// Pick a rack the same way only when this superpod's candidates carry one,
	// so a two-tier chip (no rack reported) stays two-tier and never invents a
	// rack the hardware does not have.
	if racks := sortedRackIDsIn(candidates, target); len(racks) > 0 {
		targetRack := racks[int(crc32String(serviceID+"#rack"))%len(racks)]
		occ.RackKeys[RackKey(target, targetRack)] = true
	}
	return occ
}

// sortedSuperPodIDs returns the distinct usable superpod ids in the candidate
// set, ascending. Sorting first is what makes the modulo choice stable: the
// same cluster read from two pods yields the same ordered list.
func sortedSuperPodIDs(candidates []Entry) []int64 {
	seen := make(map[int64]bool, len(candidates))
	out := make([]int64, 0, len(candidates))
	for _, e := range candidates {
		if e.SuperPodID < 0 || seen[e.SuperPodID] {
			continue
		}
		seen[e.SuperPodID] = true
		out = append(out, e.SuperPodID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// sortedRackIDsIn returns the distinct usable rack ids within one superpod,
// ascending. Rack ids are only unique inside a superpod, so the caller scopes
// the lookup to the chosen one.
func sortedRackIDsIn(candidates []Entry, superPodID int64) []int64 {
	seen := make(map[int64]bool, len(candidates))
	out := make([]int64, 0, len(candidates))
	for _, e := range candidates {
		if e.SuperPodID != superPodID || e.RackID < 0 || seen[e.RackID] {
			continue
		}
		seen[e.RackID] = true
		out = append(out, e.RackID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func crc32String(s string) uint32 { return crc32.ChecksumIEEE([]byte(s)) }
