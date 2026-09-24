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
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	// HyperNodeLabelKey marks the hypernode (SuperPoD performance domain) a
	// node belongs to, e.g. one Atlas 900 A3 SuperPoD or CloudMatrix pod in a
	// multi-supernode cluster.
	HyperNodeLabelKey = "hami.io/hypernode"
	// VolcanoHyperNodeLabelKey is accepted as a fallback so clusters already
	// labelled for Volcano's network-topology-aware scheduling work with HAMi
	// without relabelling.
	VolcanoHyperNodeLabelKey = "volcano.sh/hypernode"
	// HyperNodeAffinityAnnotationKey pins a pod to the hypernode named in the
	// annotation value. Nodes outside that hypernode are filtered out.
	HyperNodeAffinityAnnotationKey = "hami.io/hypernode-affinity"
	// HyperNodeGroupAnnotationKey groups the pods of one distributed job; pods
	// sharing a value are packed into the same hypernode when capacity allows.
	HyperNodeGroupAnnotationKey = "hami.io/hypernode-group"
)

// GetNodeHyperNode returns the hypernode a node belongs to, preferring the
// HAMi label over the Volcano-compatible one. Blank label values are ignored
// and an empty string means the node carries no hypernode information.
func GetNodeHyperNode(node *corev1.Node) string {
	if node == nil {
		return ""
	}
	for _, key := range []string{HyperNodeLabelKey, VolcanoHyperNodeLabelKey} {
		if value, ok := node.Labels[key]; ok {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

// GetHyperNodeAffinityByPod returns the hypernode the pod is pinned to, or an
// empty string when the pod carries no (or a blank) affinity annotation.
func GetHyperNodeAffinityByPod(pod *corev1.Pod) string {
	return podAnnotation(pod, HyperNodeAffinityAnnotationKey)
}

// GetHyperNodeGroupByPod returns the hypernode group of the pod, or an empty
// string when the pod carries no (or a blank) group annotation.
func GetHyperNodeGroupByPod(pod *corev1.Pod) string {
	return podAnnotation(pod, HyperNodeGroupAnnotationKey)
}

func podAnnotation(pod *corev1.Pod, key string) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	return strings.TrimSpace(pod.Annotations[key])
}
