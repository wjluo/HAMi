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

package util

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func nodeWithAnnotations(annos map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", Annotations: annos}}
}

func podWithLabels(labels map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", Labels: labels}}
}

func TestGetNodeSuperPodID(t *testing.T) {
	if got := GetNodeSuperPodID(nil); got != TopoIDInvalid {
		t.Errorf("nil node: GetNodeSuperPodID() = %d, want %d", got, TopoIDInvalid)
	}
	tests := []struct {
		name  string
		value string
		want  int64
	}{
		{"missing key", "", TopoIDInvalid},
		{"zero is valid", "0", 0},
		{"plain integer", "7", 7},
		{"surrounding spaces tolerated", " 12 ", 12},
		{"non numeric", "supernode-1", TopoIDInvalid},
		{"float form", "3.0", TopoIDInvalid},
		{"negative normalized to invalid", "-5", TopoIDInvalid},
		{"sentinel -1", "-1", TopoIDInvalid},
		{"sentinel -2 normalized", "-2", TopoIDInvalid},
		{"overflow normalized to invalid", "99999999999999999999999", TopoIDInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			annos := map[string]string{}
			if tc.value != "" {
				annos[TopoSuperPodIDAnnotationKey] = tc.value
			}
			if got := GetNodeSuperPodID(nodeWithAnnotations(annos)); got != tc.want {
				t.Errorf("GetNodeSuperPodID() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetNodeRackID(t *testing.T) {
	tests := []struct {
		name     string
		annos    map[string]string
		wantRack int64
	}{
		{
			"all three levels present",
			map[string]string{
				TopoSuperPodIDAnnotationKey: "3",
				TopoRackIDAnnotationKey:     "2",
				TopoServerIDAnnotationKey:   "11",
			},
			2,
		},
		{
			"rack level absent for A3-like node",
			map[string]string{
				TopoSuperPodIDAnnotationKey: "3",
				TopoServerIDAnnotationKey:   "11",
			},
			TopoIDInvalid,
		},
		{
			"nil node yields invalid",
			nil,
			TopoIDInvalid,
		},
		{
			"garbage rack falls back to invalid",
			map[string]string{
				TopoRackIDAnnotationKey:   "R02",
				TopoServerIDAnnotationKey: "4",
			},
			TopoIDInvalid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := nodeWithAnnotations(tc.annos)
			if tc.annos == nil {
				node = nil
			}
			if got := GetNodeRackID(node); got != tc.wantRack {
				t.Errorf("GetNodeRackID() = %d, want %d", got, tc.wantRack)
			}
		})
	}
}

// The server level is only read to cross-check one node against another, so a
// missing or unusable value has to read as "says nothing" rather than as a
// server every unannotated node shares.
func TestGetNodeServerID(t *testing.T) {
	tests := []struct {
		name       string
		annos      map[string]string
		wantServer int64
	}{
		{"reported server index", map[string]string{
			TopoSuperPodIDAnnotationKey: "3",
			TopoServerIDAnnotationKey:   "11",
		}, 11},
		{"absent level on a chip that stops above the server", map[string]string{
			TopoSuperPodIDAnnotationKey: "3",
		}, TopoIDInvalid},
		{"nil node yields invalid", nil, TopoIDInvalid},
		{"negative driver sentinel yields invalid", map[string]string{
			TopoServerIDAnnotationKey: "-2",
		}, TopoIDInvalid},
		{"zero is a real index, not a missing one", map[string]string{
			TopoServerIDAnnotationKey: "0",
		}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := nodeWithAnnotations(tc.annos)
			if tc.annos == nil {
				node = nil
			}
			if got := GetNodeServerID(node); got != tc.wantServer {
				t.Errorf("GetNodeServerID() = %d, want %d", got, tc.wantServer)
			}
		})
	}
}

func TestGetInferServiceID(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{"standard prefixed label wins", map[string]string{
			InferServiceIDLabel:       "svc-a",
			InferServiceIDLabelLegacy: "svc-legacy",
		}, "svc-a"},
		{"legacy label honoured alone", map[string]string{
			InferServiceIDLabelLegacy: "svc-legacy",
		}, "svc-legacy"},
		{"prefixed empty falls through to legacy", map[string]string{
			InferServiceIDLabel:       "",
			InferServiceIDLabelLegacy: "svc-legacy",
		}, "svc-legacy"},
		{"no labels means not an inference service pod", nil, ""},
		{"unrelated labels", map[string]string{"app": "mindie"}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := GetInferServiceID(podWithLabels(tc.labels)); got != tc.want {
				t.Errorf("GetInferServiceID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGetInferServiceIDNilPod(t *testing.T) {
	if got := GetInferServiceID(nil); got != "" {
		t.Errorf("GetInferServiceID(nil) = %q, want empty", got)
	}
}

// Device plugins declare these keys independently, so the literals are the
// cross-repo contract: a rename here must be mirrored in the plugin, and the
// two pinning tests are what catch a drift.
func TestTopologyKeysAreTheCrossRepoContract(t *testing.T) {
	want := map[string]string{
		TopoSuperPodIDAnnotationKey: "hami.io/superpodid",
		TopoRackIDAnnotationKey:     "hami.io/rackid",
		TopoServerIDAnnotationKey:   "hami.io/serverid",
		InferServiceIDLabel:         "hami.io/inferserviceid",
		InferServiceIDLabelLegacy:   "inferserviceid",
	}
	for got, expected := range want {
		if got != expected {
			t.Errorf("key %q drifted from the contract value %q", got, expected)
		}
	}
}
