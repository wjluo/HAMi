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

func TestGetNodeHyperNode(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "nil labels returns empty",
			labels: nil,
			want:   "",
		},
		{
			name:   "no hypernode labels returns empty",
			labels: map[string]string{"kubernetes.io/hostname": "node1"},
			want:   "",
		},
		{
			name:   "hami label wins",
			labels: map[string]string{HyperNodeLabelKey: "hn-a", VolcanoHyperNodeLabelKey: "hn-b"},
			want:   "hn-a",
		},
		{
			name:   "volcano label as fallback",
			labels: map[string]string{VolcanoHyperNodeLabelKey: "hn-b"},
			want:   "hn-b",
		},
		{
			name:   "empty label values are ignored",
			labels: map[string]string{HyperNodeLabelKey: "  ", VolcanoHyperNodeLabelKey: "hn-b"},
			want:   "hn-b",
		},
		{
			name:   "value is trimmed",
			labels: map[string]string{HyperNodeLabelKey: " hn-a "},
			want:   "hn-a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: tt.labels}}
			if got := GetNodeHyperNode(node); got != tt.want {
				t.Errorf("GetNodeHyperNode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetNodeHyperNodeNilNode(t *testing.T) {
	if got := GetNodeHyperNode(nil); got != "" {
		t.Errorf("GetNodeHyperNode(nil) = %q, want empty", got)
	}
}

func TestGetHyperNodeAffinityByPod(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
	}{
		{name: "no annotations", annotations: nil, want: ""},
		{name: "no affinity annotation", annotations: map[string]string{"other": "x"}, want: ""},
		{name: "affinity set", annotations: map[string]string{HyperNodeAffinityAnnotationKey: "hn-a"}, want: "hn-a"},
		{name: "affinity trimmed", annotations: map[string]string{HyperNodeAffinityAnnotationKey: " hn-a "}, want: "hn-a"},
		{name: "blank affinity ignored", annotations: map[string]string{HyperNodeAffinityAnnotationKey: "   "}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", Annotations: tt.annotations}}
			if got := GetHyperNodeAffinityByPod(pod); got != tt.want {
				t.Errorf("GetHyperNodeAffinityByPod() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetHyperNodeAffinityNilPod(t *testing.T) {
	if got := GetHyperNodeAffinityByPod(nil); got != "" {
		t.Errorf("GetHyperNodeAffinityByPod(nil) = %q, want empty", got)
	}
}

func TestGetHyperNodeGroupByPod(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
	}{
		{name: "no annotations", annotations: nil, want: ""},
		{name: "group set", annotations: map[string]string{HyperNodeGroupAnnotationKey: "job-1"}, want: "job-1"},
		{name: "group trimmed", annotations: map[string]string{HyperNodeGroupAnnotationKey: " job-1 "}, want: "job-1"},
		{name: "blank group ignored", annotations: map[string]string{HyperNodeGroupAnnotationKey: ""}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", Annotations: tt.annotations}}
			if got := GetHyperNodeGroupByPod(pod); got != tt.want {
				t.Errorf("GetHyperNodeGroupByPod() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetHyperNodeGroupNilPod(t *testing.T) {
	if got := GetHyperNodeGroupByPod(nil); got != "" {
		t.Errorf("GetHyperNodeGroupByPod(nil) = %q, want empty", got)
	}
}
