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

// Package plane normalises every topology writer in the cluster - vendor path
// labels, operator-set tier labels, machine-written annotations and the legacy
// integer annotations - into one read-side model: for each of the three network
// planes, a node either is labelled, is explicitly off that plane, is not yet
// described, or carries contradictory evidence.
//
// The three planes are the only vocabulary callers may use:
//
//	service  the 25GE north-south fabric that carries business traffic
//	compute  the spine+leaf RDMA fabric (parameter-plane and scale-out fabrics
//	         are separate domains of this one plane, physically isolated)
//	scaleup  the superpod-exclusive interconnect
//
// Tiers are ordered shallow to deep, so Tiers[0] is always the capability key:
// two nodes whose Tiers[0] differ cannot talk on that plane at all, which is a
// hard reject rather than a lower score. This package is read-only and free of
// device-cache dependencies so that both the scheduler and the startup audit
// can share one implementation of "closer" and "reachable".
package plane

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/yaml"

	"github.com/Project-HAMi/HAMi/pkg/util"
)

// Plane identifies one of the three network fabrics.
type Plane string

const (
	ScaleUp Plane = "scaleup"
	Compute Plane = "compute"
	Service Plane = "service"
)

// All returns the planes in shallow-to-deep capability order.
func All() []Plane {
	return []Plane{ScaleUp, Compute, Service}
}

// Depth is how many tiers a plane reports, which is what makes an LCP count
// readable: "one tier in common" means something different on a two-tier fabric
// than on a four-tier one. Callers compare an LCP against it to ask whether two
// nodes sit at the same place on the fabric.
func Depth(p Plane) int {
	return len(specs[p].tiers)
}

// State tells the caller how much it can trust a node's topology on one plane.
type State string

const (
	// Labeled means every tier below was resolved without contradiction.
	Labeled State = "labeled"
	// Absent means the node was probed and genuinely does not belong to this
	// plane. The gate may reject on it.
	Absent State = "absent"
	// Unlabeled means nobody has described this plane yet. The gate must stay
	// silent, otherwise unlabelled clusters lose every candidate.
	Unlabeled State = "unlabeled"
	// Conflict means two writers disagree; the plane contributes nothing.
	Conflict State = "conflict"
)

// Reasons recorded on Conflict and unusable Unlabeled domains.
const (
	ReasonDomainMismatch = "domain-mismatch"
	ReasonSourceConflict = "plane-source-conflict"
	ReasonPathUnmapped   = "path-unmapped"
	ReasonMissingDomain  = "missing-domain"
)

// InvalidID aliases the scheduler-wide sentinel so readers cannot invent a
// second meaning for -1.
const InvalidID int64 = util.TopoIDInvalid

// Where the domain registry lives, named here so the loader, the ops script and
// the delivered example cannot drift apart.
const (
	DomainRegistryConfigMapName = "hami-topo-domain-registry"
	DomainRegistryConfigMapKey  = "domains.yaml"
)

// reserved reads as "no value" wherever a tier value appears.
var reserved = map[string]bool{
	"":       true,
	"none":   true,
	"null":   true,
	"island": true,
}

// Domain is one node's topology on one plane.
type Domain struct {
	State  State
	Tiers  []int64
	Raw    []string
	Reason string
}

// SameDomain reports whether two nodes share the capability key - the only
// question the hard gate asks.
func (d Domain) SameDomain(o Domain) bool {
	if d.State != Labeled || o.State != Labeled {
		return false
	}
	if len(d.Tiers) == 0 || len(o.Tiers) == 0 {
		return false
	}
	return d.Tiers[0] == o.Tiers[0]
}

// Config tunes resolution for clusters that cannot label every node.
type Config struct {
	// ServiceDefault fills the service plane for unlabelled nodes. The 25GE
	// fabric interconnects every node, so an unlabelled service plane must not
	// read as unknown, or requirement=service would reject healthy nodes.
	ServiceDefault string
}

type tier struct {
	name  string
	annos []string
	label string
}

type planeSpec struct {
	combined  string
	pathLabel string
	tiers     []tier
}

const (
	tierDomain = "domain"
	tierRack   = "rack"
	tierLeaf   = "leaf"
	tierServer = "server"
)

var specs = map[Plane]planeSpec{
	ScaleUp: {
		combined: "hami.io/topo.scaleup",
		tiers: []tier{
			{name: tierDomain, annos: []string{util.TopoSuperPodIDAnnotationKey, "hami.io/topotree.supernodeid"}, label: "hami.io/topo.scaleup.domain"},
			{name: tierRack, annos: []string{util.TopoRackIDAnnotationKey, "hami.io/topotree.rackid"}, label: "hami.io/topo.scaleup.rack"},
			{name: tierServer, annos: []string{util.TopoServerIDAnnotationKey, "hami.io/topotree.serverid"}, label: "hami.io/topo.scaleup.server"},
		},
	},
	Compute: {
		combined:  "hami.io/topo.compute",
		pathLabel: "topology.ascend.huawei.com/path",
		tiers: []tier{
			{name: tierDomain, label: "hami.io/topo.compute.domain"},
			{name: tierLeaf, label: "hami.io/topo.compute.leaf"},
		},
	},
	Service: {
		combined: "hami.io/topo.service",
		tiers: []tier{
			{name: tierDomain, label: "hami.io/topo.service.domain"},
		},
	},
}

// Resolve reports every plane of one node. A nil registry means no vendor path
// can be mapped to a compute domain.
func Resolve(n *corev1.Node, reg *Registry) map[Plane]Domain {
	return ResolveWith(n, reg, Config{})
}

func ResolveWith(n *corev1.Node, reg *Registry, cfg Config) map[Plane]Domain {
	out := make(map[Plane]Domain, len(specs))
	for _, p := range All() {
		out[p] = resolve(n, p, reg, cfg)
	}
	return out
}

func resolve(n *corev1.Node, p Plane, reg *Registry, cfg Config) Domain {
	if n == nil {
		return Domain{State: Unlabeled}
	}
	spec := specs[p]
	combinedValues := map[string]string{}
	if combined, ok := n.Annotations[spec.combined]; ok {
		combinedValues = parseCombined(combined)
	}

	// Machine writers own the annotation family: the legacy per-id annotations,
	// the topotree mirror, and the combined per-plane spelling all describe the
	// same hardware truth, so where they name the same tier differently there is
	// no correct pick — the node's topology is not knowable this round.
	anno := map[string]string{}
	selfContradicted := false
	for _, t := range spec.tiers {
		ids := map[int64]bool{}
		for _, key := range t.annos {
			if v, ok := n.Annotations[key]; ok {
				ids[canonicalID(strings.TrimSpace(v))] = true
			}
		}
		if v, ok := combinedValues[t.name]; ok {
			ids[canonicalID(v)] = true
		}
		if len(ids) > 1 {
			selfContradicted = true
		}
		// The writers agree, so any of their spellings describes the tier.
		for _, key := range t.annos {
			if v, ok := n.Annotations[key]; ok {
				anno[t.name] = strings.TrimSpace(v)
				break
			}
		}
		if _, ok := anno[t.name]; !ok {
			if v, ok := combinedValues[t.name]; ok {
				anno[t.name] = v
			}
		}
	}
	if selfContradicted {
		return Domain{State: Conflict, Reason: ReasonSourceConflict}
	}

	// Operators own the label family, one label per tier, plus the vendor path
	// label that the registry turns into a domain name.
	label := map[string]string{}
	for _, t := range spec.tiers {
		if t.label == "" {
			continue
		}
		if v, ok := n.Labels[t.label]; ok {
			label[t.name] = strings.TrimSpace(v)
		}
	}
	reason := ""
	derived := map[string]string{}
	if spec.pathLabel != "" {
		if path, ok := n.Labels[spec.pathLabel]; ok {
			name, found := "", false
			if reg.PlaneFor() == p {
				name, found = reg.DomainFor(path)
			}
			if found {
				derived[tierDomain] = name
				if leaf := leafFromPath(path); leaf != "" {
					derived[tierLeaf] = leaf
				}
			} else {
				reason = ReasonPathUnmapped
			}
		}
	}

	if d, ok := label[tierDomain]; ok {
		if a, ok := anno[tierDomain]; ok && canonicalID(d) != canonicalID(a) {
			return Domain{State: Conflict, Reason: ReasonSourceConflict}
		}
	}
	if d, ok := derived[tierDomain]; ok {
		if l, ok := label[tierDomain]; ok && canonicalID(d) != canonicalID(l) {
			return Domain{State: Conflict, Reason: ReasonDomainMismatch}
		}
	}

	merged := map[string]string{}
	for name, v := range anno {
		merged[name] = v
	}
	for name, v := range label {
		merged[name] = v
	}
	for name, v := range derived {
		if _, ok := merged[name]; !ok {
			merged[name] = v
		}
	}

	if len(merged) == 0 {
		if p == Service && cfg.ServiceDefault != "" {
			return Domain{
				State: Labeled,
				Tiers: []int64{canonicalID(cfg.ServiceDefault)},
				Raw:   []string{cfg.ServiceDefault},
			}
		}
		return Domain{State: Unlabeled, Reason: reason}
	}

	var tiers []int64
	var raw []string
	for _, t := range spec.tiers {
		v, ok := merged[t.name]
		if !ok {
			break
		}
		id := canonicalID(v)
		if id == InvalidID {
			break
		}
		tiers = append(tiers, id)
		raw = append(raw, v)
	}
	if len(tiers) == 0 {
		if _, ok := merged[tierDomain]; ok {
			return Domain{State: Absent}
		}
		return Domain{State: Unlabeled, Reason: ReasonMissingDomain}
	}
	return Domain{State: Labeled, Tiers: tiers, Raw: raw, Reason: reason}
}

// LCP is the single "who is closer" function: the depth of the longest common
// prefix, counted in tiers from the shallowest one. Nodes we know nothing about
// are never closer than nodes we do.
func LCP(a, b Domain) int {
	if a.State != Labeled || b.State != Labeled {
		return 0
	}
	n := 0
	for n < len(a.Tiers) && n < len(b.Tiers) && a.Tiers[n] == b.Tiers[n] {
		n++
	}
	return n
}

// Source classifies how a plane's domains were spelled, so the audit can flag
// a plane that mixes hardware ids with switch names.
type Source string

const (
	SourceInteger Source = "integer"
	SourceNamed   Source = "named"
	SourceMixed   Source = "mixed"
	SourceUnknown Source = "unknown"
)

func SourceOf(domains []Domain) Source {
	integer, named := false, false
	for _, d := range domains {
		if d.State != Labeled || len(d.Raw) == 0 {
			continue
		}
		if _, err := strconv.ParseInt(d.Raw[0], 10, 64); err == nil {
			integer = true
		} else {
			named = true
		}
	}
	switch {
	case integer && named:
		return SourceMixed
	case named:
		return SourceNamed
	case integer:
		return SourceInteger
	default:
		return SourceUnknown
	}
}

// canonicalID maps a tier value onto the single integer space the tiers and the
// gate work in. Integers pass through; names are hashed because domains only
// need equality, never ordering.
func canonicalID(raw string) int64 {
	v := strings.TrimSpace(raw)
	if reserved[strings.ToLower(v)] {
		return InvalidID
	}
	if id, err := strconv.ParseInt(v, 10, 64); err == nil {
		if id < 0 {
			return InvalidID
		}
		return id
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToLower(v)))
	return int64(h.Sum64() >> 1)
}

// parseCombined reads the machine-written "domain=10/rack=2/server=3" spelling.
func parseCombined(value string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(value, "/") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}
		out[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return out
}

// leafFromPath takes the last segment of a vendor path such as
// leaf-SITE01R1D10U39-DCN-DC1_FB01-LF01_A, whose tail is the leaf identifier.
func leafFromPath(path string) string {
	seg := strings.Split(strings.Trim(path, "-"), "-")
	if len(seg) < 2 {
		return ""
	}
	return seg[len(seg)-1]
}

// Registry maps the unstructured vendor path strings onto short domain names,
// which is the only spelling operators and the platform both agree to read.
type Registry struct {
	plane Plane
	rules []registryRule
}

type registryRule struct {
	name  string
	match string
}

type registryFile struct {
	Plane   string `json:"plane"`
	Domains []struct {
		Name  string `json:"name"`
		Match string `json:"match"`
	} `json:"domains"`
}

// ParseRegistry reads the operator-facing registry document:
//
//	plane: compute
//	domains:
//	  - name: PF-SZ01
//	    match: "-DCN-DC1_FB01-LF01"
//
// Rule order decides which domain wins when a path matches more than one
// substring, so a duplicated name or match is refused at load time rather than
// resolved silently at schedule time. Unknown keys are refused too, because a
// misspelled "math:" would otherwise unmap the whole fabric.
func ParseRegistry(text string) (*Registry, error) {
	var file registryFile
	if err := yaml.UnmarshalStrict([]byte(text), &file); err != nil {
		return nil, fmt.Errorf("domain registry: %w", err)
	}
	p := Plane(strings.TrimSpace(file.Plane))
	if p == "" {
		p = Compute
	}
	known := false
	for _, candidate := range All() {
		if candidate == p {
			known = true
			break
		}
	}
	if !known {
		return nil, fmt.Errorf("domain registry: unknown plane %q", file.Plane)
	}
	if len(file.Domains) == 0 {
		return nil, fmt.Errorf("domain registry: plane %q has no domains", p)
	}
	reg := &Registry{plane: p}
	names := map[string]bool{}
	matches := map[string]bool{}
	for i, rule := range file.Domains {
		name, match := strings.TrimSpace(rule.Name), strings.TrimSpace(rule.Match)
		if name == "" || match == "" {
			return nil, fmt.Errorf("domain registry: rule %d needs both a name and a match", i+1)
		}
		if names[name] {
			return nil, fmt.Errorf("domain registry: duplicate domain name %q", name)
		}
		if matches[match] {
			return nil, fmt.Errorf("domain registry: %q already names a different domain", match)
		}
		names[name], matches[match] = true, true
		reg.rules = append(reg.rules, registryRule{name: name, match: match})
	}
	return reg, nil
}

// PlaneFor reports which plane this registry describes.
func (r *Registry) PlaneFor() Plane {
	if r == nil {
		return ""
	}
	return r.plane
}

// DomainFor returns the short domain name a vendor path belongs to.
func (r *Registry) DomainFor(path string) (string, bool) {
	if r == nil {
		return "", false
	}
	for _, rule := range r.rules {
		if strings.Contains(path, rule.match) {
			return rule.name, true
		}
	}
	return "", false
}

// AuditSummary is what the scheduler logs once at startup, before it dares to
// gate on topology.
type AuditSummary struct {
	Nodes   int
	States  map[Plane]map[State]int
	Reasons map[string]int
	// Suspects lists the findings an operator has to fix, in node order. Quiet
	// nodes - unlabelled but consistent - are left out, so the list stays a work
	// list instead of a dump of the fleet.
	Suspects []Suspect
}

// Suspect is one node whose topology contradicts itself or cannot be read.
type Suspect struct {
	Node   string
	Plane  Plane
	State  State
	Reason string
}

func (s AuditSummary) ByState(p Plane, st State) int {
	return s.States[p][st]
}

func (s AuditSummary) ReasonCount(reason string) int {
	return s.Reasons[reason]
}

// Audit tallies how much of the fleet is actually describable. Nodes that are
// Unlabeled are counted too: an empty fleet is the most likely reason a gate
// would reject everything.
func Audit(nodes []*corev1.Node, reg *Registry, cfg Config) AuditSummary {
	sum := AuditSummary{
		States:  map[Plane]map[State]int{},
		Reasons: map[string]int{},
	}
	for _, p := range All() {
		sum.States[p] = map[State]int{}
	}
	for _, n := range nodes {
		sum.Nodes++
		for p, d := range ResolveWith(n, reg, cfg) {
			sum.States[p][d.State]++
			if d.Reason == "" {
				continue
			}
			sum.Reasons[d.Reason]++
			if d.State == Conflict || d.State == Unlabeled {
				sum.Suspects = append(sum.Suspects, Suspect{Node: n.Name, Plane: p, State: d.State, Reason: d.Reason})
			}
		}
	}
	return sum
}
