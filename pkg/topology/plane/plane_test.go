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

package plane

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"gotest.tools/v3/assert"
)

func nodeWith(labels map[string]string, annos map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annos},
	}
}

// registry maps the production leaf-path string onto a short compute domain
// name, in exactly the spelling examples/superpod/11-configmap-domain-registry.yaml
// tells operators to write.
func testRegistry(t *testing.T) *Registry {
	t.Helper()
	reg, err := ParseRegistry(`
plane: compute
domains:
  # comment lines must not break the load
  - name: PF-SZ01
    match: "-DCN-DC1_FB13-LF46"
  - name: SO-SET1
    match: "R1F10U39"
`)
	assert.NilError(t, err)
	return reg
}

func TestResolveScaleUpFromLegacyIntAnnotations(t *testing.T) {
	n := nodeWith(nil, map[string]string{
		"hami.io/superpodid": "10",
		"hami.io/rackid":     "2",
		"hami.io/serverid":   "3",
	})
	got := Resolve(n, testRegistry(t))

	d := got[ScaleUp]
	assert.Equal(t, d.State, Labeled)
	assert.Equal(t, len(d.Tiers), 3)
	// Shallow -> deep, so Tiers[0] is the capability key (the superpod itself).
	assert.DeepEqual(t, d.Tiers, []int64{10, 2, 3})
}

func TestResolveScaleUpFromCombinedAnnotation(t *testing.T) {
	n := nodeWith(nil, map[string]string{
		"hami.io/topo.scaleup": "domain=10/rack=2/server=3",
	})
	got := Resolve(n, testRegistry(t))

	assert.DeepEqual(t, got[ScaleUp].Tiers, []int64{10, 2, 3})
	assert.Equal(t, got[ScaleUp].State, Labeled)
}

func TestResolveScaleUpReadsLegacyMockSupernodeIDAlias(t *testing.T) {
	// The mock device plugin writes hami.io/topotree.supernodeid; it must land on
	// the same ScaleUp plane as hami.io/superpodid.
	n := nodeWith(nil, map[string]string{
		"hami.io/topotree.supernodeid": "7",
		"hami.io/topotree.rackid":      "4",
	})
	got := Resolve(n, testRegistry(t))

	assert.Equal(t, got[ScaleUp].State, Labeled)
	assert.DeepEqual(t, got[ScaleUp].Tiers, []int64{7, 4})
}

func TestResolveComputeFromVendorPathLabelAndRegistry(t *testing.T) {
	n := nodeWith(map[string]string{
		"topology.ascend.huawei.com/path": "leaf-SZPX06R1D10U39-DCN-DC1_FB13-LF46_A",
	}, nil)
	got := Resolve(n, testRegistry(t))

	d := got[Compute]
	assert.Equal(t, d.State, Labeled)
	assert.Equal(t, len(d.Tiers), 2, "domain + leaf")
	assert.Equal(t, d.Raw[0], "PF-SZ01", "short domain name comes from the registry")

	// A node on the physically isolated scale-out fabric is a different domain,
	// so the gate must reject it - not score it lower.
	other := Resolve(nodeWith(map[string]string{
		"topology.ascend.huawei.com/path": "leaf-SZPX06R1F10U39-DCN-R1F10U39-B",
	}, nil), testRegistry(t))
	assert.Equal(t, other[Compute].Raw[0], "SO-SET1")
	assert.Assert(t, !d.SameDomain(other[Compute]), "different short names are different domains")
	assert.Equal(t, LCP(d, other[Compute]), 0)
}

func TestResolveComputeDomainMismatchDegradesToUnlabeled(t *testing.T) {
	// Ops labelled SO-SET1, but the vendor path only matches PF-SZ01 -> conflict.
	n := nodeWith(map[string]string{
		"topology.ascend.huawei.com/path": "leaf-SZPX06R1D10U39-DCN-DC1_FB13-LF46_A",
		"hami.io/topo.compute.domain":     "SO-SET1",
		"hami.io/topo.compute.leaf":       "LF46_A",
	}, nil)
	got := Resolve(n, testRegistry(t))

	d := got[Compute]
	assert.Equal(t, d.State, Conflict, "must not silently trust one of the two sources")
	assert.Equal(t, d.Reason, ReasonDomainMismatch)
	assert.Equal(t, len(d.Tiers), 0, "a conflicted plane contributes no domain to the gate")
}

func TestResolveComputeFromLabelsOnly(t *testing.T) {
	n := nodeWith(map[string]string{
		"hami.io/topo.compute.domain": "SO-SET1",
		"hami.io/topo.compute.leaf":   "LF49_A",
	}, nil)
	got := Resolve(n, testRegistry(t))

	d := got[Compute]
	assert.Equal(t, d.State, Labeled)
	assert.Equal(t, len(d.Tiers), 2)
}

func TestResolveExplicitNoneIsNotTheSameAsUnlabeled(t *testing.T) {
	none := Resolve(nodeWith(map[string]string{"hami.io/topo.compute.domain": "none"}, nil), testRegistry(t))
	assert.Equal(t, none[Compute].State, Absent, "probed-and-absent is a hard fact the gate may reject on")

	unknown := Resolve(nodeWith(nil, nil), testRegistry(t))
	assert.Equal(t, unknown[Compute].State, Unlabeled, "not yet labelled stays exempt")

	assert.Assert(t, none[Compute].State != unknown[Compute].State)
}

func TestResolveSentinelNegativeIDsMeanAbsent(t *testing.T) {
	// Vendors report -1 (no such tier) and -2 (driver unsupported); neither is a domain.
	n := nodeWith(nil, map[string]string{
		"hami.io/superpodid": "-1",
		"hami.io/rackid":     "-2",
	})
	got := Resolve(n, testRegistry(t))

	assert.Equal(t, got[ScaleUp].State, Absent)
}

func TestResolveLabelAndAnnotationSourceConflict(t *testing.T) {
	n := nodeWith(map[string]string{
		"hami.io/topo.compute.domain": "SO-SET1",
	}, map[string]string{
		"hami.io/topo.compute": "domain=SO-SET2/leaf=LF52_A",
	})
	got := Resolve(n, testRegistry(t))

	d := got[Compute]
	assert.Equal(t, d.State, Conflict)
	assert.Equal(t, d.Reason, ReasonSourceConflict)
}

func TestResolveServicePlaneIsFilledByDefault(t *testing.T) {
	// 25GE interconnects every node, so an unlabelled Service plane must not read
	// as "unknown" - that would make requirement=service reject healthy nodes.
	cfg := Config{ServiceDefault: "cluster"}
	got := ResolveWith(nodeWith(nil, nil), testRegistry(t), cfg)

	d := got[Service]
	assert.Equal(t, d.State, Labeled)
	assert.Equal(t, len(d.Tiers), 1)

	other := ResolveWith(nodeWith(nil, nil), testRegistry(t), cfg)
	assert.Assert(t, d.SameDomain(other[Service]))
}

func TestResolveServicePlaneStaysUnlabeledWithoutDefault(t *testing.T) {
	got := Resolve(nodeWith(nil, nil), testRegistry(t))
	assert.Equal(t, got[Service].State, Unlabeled)
}

func TestLCPPrefersTheDeepestCommonTier(t *testing.T) {
	sameRack := Domain{State: Labeled, Tiers: []int64{10, 2, 3}}
	sameRackOtherServer := Domain{State: Labeled, Tiers: []int64{10, 2, 9}}
	otherRack := Domain{State: Labeled, Tiers: []int64{10, 5, 3}}
	otherSuperpod := Domain{State: Labeled, Tiers: []int64{11, 2, 3}}

	assert.Equal(t, LCP(sameRack, sameRackOtherServer), 2)
	assert.Equal(t, LCP(sameRack, otherRack), 1)
	assert.Equal(t, LCP(sameRack, otherSuperpod), 0)
	assert.Equal(t, LCP(sameRack, sameRack), 3)
}

func TestLCPAndSameDomainIgnoreUnknownStates(t *testing.T) {
	known := Domain{State: Labeled, Tiers: []int64{10}}
	for _, other := range []Domain{
		{State: Unlabeled},
		{State: Absent},
		{State: Conflict, Reason: ReasonDomainMismatch},
	} {
		assert.Equal(t, LCP(known, other), 0)
		assert.Assert(t, !known.SameDomain(other))
	}
}

func TestCanonicalIDKeepsIntegersAndHashesNames(t *testing.T) {
	assert.Equal(t, canonicalID("10"), int64(10))
	assert.Equal(t, canonicalID(" 10 "), int64(10))
	// Names only need equality, never ordering.
	assert.Equal(t, canonicalID("SO-SET1"), canonicalID("SO-SET1"))
	assert.Assert(t, canonicalID("SO-SET1") != canonicalID("SO-SET2"))
	assert.Assert(t, canonicalID("SO-SET1") > InvalidID, "hashes stay non-negative")
}

func TestCanonicalIDReservesSentinels(t *testing.T) {
	for _, bad := range []string{"", "none", "None", "-1", "-2", "island"} {
		assert.Equal(t, canonicalID(bad), InvalidID)
	}
}

func TestDetectDomainSources(t *testing.T) {
	integer := Domain{State: Labeled, Tiers: []int64{3}, Raw: []string{"3"}}
	named := Domain{State: Labeled, Tiers: []int64{canonicalID("PF-SZ01")}, Raw: []string{"PF-SZ01"}}

	assert.Equal(t, SourceOf([]Domain{integer, integer}), SourceInteger)
	assert.Equal(t, SourceOf([]Domain{named}), SourceNamed)
	assert.Equal(t, SourceOf([]Domain{integer, named}), SourceMixed,
		"one plane must not mix hardware ids and switch names")
	assert.Equal(t, SourceOf(nil), SourceUnknown)
}

func TestParseRegistryRejectsIncompleteRules(t *testing.T) {
	_, err := ParseRegistry("plane: compute\ndomains:\n  - name: PF-SZ01\n")
	assert.Assert(t, err != nil, "a rule needs both a short name and a match")

	_, err = ParseRegistry("plane: compute\ndomains:\n  - match: X\n")
	assert.Assert(t, err != nil, "a rule with no name cannot be a domain")

	_, err = ParseRegistry("plane: compute\ndomains:\n")
	assert.Assert(t, err != nil, "an empty registry would silently unmap every node")
}

func TestParseRegistryRejectsAmbiguousAndMisspelledFiles(t *testing.T) {
	_, err := ParseRegistry("plane: compute\ndomains:\n  - name: A\n    match: X\n  - name: B\n    match: X\n")
	assert.Assert(t, err != nil, "one substring mapping to two domains makes every match ambiguous")

	_, err = ParseRegistry("plane: compute\ndomains:\n  - name: A\n    math: X\n")
	assert.Assert(t, err != nil, "a misspelled key must fail the load, not be ignored")

	_, err = ParseRegistry("plane: mgmt\ndomains:\n  - name: A\n    match: X\n")
	assert.Assert(t, err != nil, "only the three known planes exist")
}

func TestRegistryOnlyFeedsItsOwnPlane(t *testing.T) {
	reg, err := ParseRegistry("plane: scaleup\ndomains:\n  - name: A\n    match: X\n")
	assert.NilError(t, err)
	n := nodeWith(map[string]string{"topology.ascend.huawei.com/path": "leaf-SZPX06R1D10U39-DCN-DC1_FB13-LF46_A"}, nil)

	assert.Equal(t, Resolve(n, reg)[Compute].State, Unlabeled,
		"a scaleup registry must not name compute domains")
}

func TestResolveScaleUpFromOperatorTierLabels(t *testing.T) {
	// A5 superpods without a topology API are hand-labelled, one label per tier.
	n := nodeWith(map[string]string{
		"hami.io/topo.scaleup.domain": "10",
		"hami.io/topo.scaleup.rack":   "2",
	}, nil)
	got := Resolve(n, testRegistry(t))

	assert.DeepEqual(t, got[ScaleUp].Tiers, []int64{10, 2})
	assert.Equal(t, SourceOf([]Domain{got[ScaleUp]}), SourceInteger)
}

func TestIdenticalLabelAndAnnotationIsNotAConflict(t *testing.T) {
	// Both writers may describe the same truth; only disagreement degrades the
	// plane, otherwise an idempotent relabel would switch the gate off.
	n := nodeWith(map[string]string{
		"hami.io/topo.compute.domain": "SO-SET1",
		"hami.io/topo.compute.leaf":   "LF49_A",
	}, map[string]string{
		"hami.io/topo.compute": "domain=SO-SET1/leaf=LF49_A",
	})
	got := Resolve(n, testRegistry(t))

	assert.Equal(t, got[Compute].State, Labeled)
	assert.Equal(t, len(got[Compute].Tiers), 2)
}

func TestVendorPathOutsideTheRegistryStaysUnlabeled(t *testing.T) {
	n := nodeWith(map[string]string{
		"topology.ascend.huawei.com/path": "leaf-SZPX06R1D10U39-DCN-DC4_OTHER-LF99_A",
	}, nil)
	got := Resolve(n, testRegistry(t))

	assert.Equal(t, got[Compute].State, Unlabeled, "an unmapped path is not evidence of a domain")
	assert.Equal(t, got[Compute].Reason, ReasonPathUnmapped, "but it must stay visible in the audit")
}

func TestAuditCountsPlaneStatesBeforeTheGateOpens(t *testing.T) {
	nodes := []*corev1.Node{
		nodeNamedWith("node-a", map[string]string{"hami.io/topo.compute.domain": "SO-SET1", "hami.io/topo.compute.leaf": "LF49_A"}),
		nodeNamedWith("node-b", map[string]string{"hami.io/topo.compute.domain": "none"}),
		nodeNamedWith("node-c", map[string]string{"hami.io/topo.compute.domain": "SO-SET1"},
			map[string]string{"hami.io/topo.compute": "domain=SO-SET2/leaf=LF52_A"}),
		nodeNamedWith("node-d", nil),
		nodeNamedWith("node-e", map[string]string{"topology.ascend.huawei.com/path": "leaf-UNKNOWN"}),
	}
	sum := Audit(nodes, testRegistry(t), Config{})

	assert.Equal(t, sum.Nodes, 5)
	assert.Equal(t, sum.ByState(Compute, Labeled), 1)
	assert.Equal(t, sum.ByState(Compute, Absent), 1)
	assert.Equal(t, sum.ByState(Compute, Conflict), 1)
	assert.Equal(t, sum.ByState(Compute, Unlabeled), 2)
	assert.Equal(t, sum.ReasonCount(ReasonSourceConflict), 1)

	// Only the actionable findings are listed, so an operator gets a work list
	// rather than a dump of every unlabelled node.
	suspects := map[string]string{}
	for _, s := range sum.Suspects {
		suspects[s.Node] = s.Reason
	}
	assert.Equal(t, len(suspects), 2)
	assert.Equal(t, suspects["node-c"], ReasonSourceConflict)
	assert.Equal(t, suspects["node-e"], ReasonPathUnmapped)
}

func nodeNamedWith(name string, labels map[string]string, annos ...map[string]string) *corev1.Node {
	var merged map[string]string
	if len(annos) > 0 {
		merged = annos[0]
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: merged},
	}
}

func TestRegistryUnmappedPathIsNotADomain(t *testing.T) {
	reg := testRegistry(t)
	name, ok := reg.DomainFor("leaf-totally-unknown")
	assert.Assert(t, !ok)
	assert.Equal(t, name, "")
}

func TestPlanesCoverTheThreeFabrics(t *testing.T) {
	assert.Equal(t, string(ScaleUp), "scaleup")
	assert.Equal(t, string(Compute), "compute")
	assert.Equal(t, string(Service), "service")
	assert.DeepEqual(t, All(), []Plane{ScaleUp, Compute, Service})
}

func TestResolveNilNodeIsSafe(t *testing.T) {
	got := Resolve(nil, testRegistry(t))
	for _, p := range All() {
		assert.Equal(t, got[p].State, Unlabeled)
	}
}

// cicpoc-gpu2 as observed on the shared cluster on 2026-10-26. Two machine
// writers describe the same box and disagree on both the domain and the rack:
// hami.io/* (the vendor-reported ids) against the hami.io/topotree.* mirror.
// First-wins here is not a harmless choice — the domain tier is the capability
// key, so reading 20 and ignoring 10 places workloads against a fabric the
// other writer says is a different superpod, and the audit then reports a
// labelled fleet with nothing wrong in it.
func TestResolveReportsDisagreeingMachineWriters(t *testing.T) {
	got := ResolveWith(gpu2Node(), nil, Config{})

	assert.Equal(t, got[ScaleUp].State, Conflict)
	assert.Equal(t, got[ScaleUp].Reason, ReasonSourceConflict)
	assert.Equal(t, len(got[ScaleUp].Tiers), 0, "a plane that contradicts itself must not contribute a domain")
}

// The mirror is idempotent on cicpoc-gpu1: hami.io/* and hami.io/topotree.*
// carry the same numbers. Equal values from two writers are a relabel, not a
// contradiction, and must stay a readable topology.
func TestResolveAgreeingMachineWritersStayLabeled(t *testing.T) {
	got := ResolveWith(gpu1Node(), nil, Config{ServiceDefault: "cluster"})

	assert.Equal(t, got[ScaleUp].State, Labeled)
	assert.DeepEqual(t, got[ScaleUp].Tiers, []int64{10, 1, 1})
}

// The audit is the instrument an operator uses to decide whether labelling is
// good enough to turn the policy on, so a self-contradicting node has to show
// up in it.
func TestAuditCountsTheLiveFleetContradiction(t *testing.T) {
	nodes := []*corev1.Node{
		gpu1Node(), gpu2Node(),
		nodeWith(map[string]string{"topology.ascend.huawei.com/path": "leaf-1"}, nil),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "cicpoc-mst1"}},
	}
	sum := Audit(nodes, nil, Config{ServiceDefault: "cluster"})

	assert.Equal(t, sum.Nodes, 4)
	assert.Equal(t, sum.ByState(ScaleUp, Labeled), 1)
	assert.Equal(t, sum.ByState(ScaleUp, Conflict), 1)
	assert.Equal(t, sum.ReasonCount(ReasonSourceConflict), 1)
}

func gpu1Node() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "cicpoc-gpu1",
		Labels: map[string]string{
			"topology.ascend.huawei.com/path": "leaf-1",
		},
		Annotations: map[string]string{
			"hami.io/rackid":               "1",
			"hami.io/serverid":             "1",
			"hami.io/superpodid":           "10",
			"hami.io/topotree.rackid":      "1",
			"hami.io/topotree.serverid":    "1",
			"hami.io/topotree.supernodeid": "10",
		},
	}}
}

func gpu2Node() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "cicpoc-gpu2",
		Annotations: map[string]string{
			"hami.io/rackid":               "2",
			"hami.io/serverid":             "2",
			"hami.io/superpodid":           "20",
			"hami.io/topotree.rackid":      "1",
			"hami.io/topotree.serverid":    "2",
			"hami.io/topotree.supernodeid": "10",
		},
	}}
}
