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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// TopoIDInvalid is returned for any node that does not carry a usable topology
// id. Negative values reported by vendors - including the MindCluster -1
// (no such tier) and -2 (driver unsupported) sentinels - are normalised to it,
// because none of them identifies a real superpod.
const TopoIDInvalid int64 = -1

// GetNodeSuperPodID returns the superpod the node belongs to, or TopoIDInvalid
// when the node is not part of a superpod cluster.
func GetNodeSuperPodID(node *corev1.Node) int64 {
	return topoID(node, TopoSuperPodIDAnnotationKey)
}

// GetNodeRackID returns the rack inside the node's superpod, or TopoIDInvalid
// on chips that have no rack tier.
func GetNodeRackID(node *corev1.Node) int64 {
	return topoID(node, TopoRackIDAnnotationKey)
}

// GetNodeServerID returns the server index inside the node's superpod, or
// TopoIDInvalid when the node does not report one. The scheduler never places a
// pod by it; it is the level a cross-check compares other nodes against,
// because one server cannot belong to two superpods.
func GetNodeServerID(node *corev1.Node) int64 {
	return topoID(node, TopoServerIDAnnotationKey)
}

// GetTaskID returns the task the pod belongs to. An empty result means the pod
// takes no part in topology convergence and is scheduled exactly as before.
func GetTaskID(pod *corev1.Pod) string {
	id, _ := taskID(pod)
	return id
}

// TaskIDLabelUsed names the label that supplied the task id, so the
// deprecation window can be closed on evidence rather than on a guess about
// what the cluster still writes. Empty means the pod names no task.
func TaskIDLabelUsed(pod *corev1.Pod) string {
	_, key := taskID(pod)
	return key
}

func taskID(pod *corev1.Pod) (string, string) {
	if pod == nil {
		return "", ""
	}
	for _, key := range []string{TaskIDLabel, TaskIDLabelLegacy, TaskIDLabelLegacyBare} {
		if id := pod.Labels[key]; id != "" {
			return id, key
		}
	}
	return "", ""
}

// SuperpodEscapeValveUsed reports whether the pod asked, on its own object, to
// be exempt from superpod domain convergence - the per-Pod disable or spill
// annotation. It deliberately ignores config.SuperpodSchedulerPolicy: a
// cluster-wide default is the operator opting the whole fleet out, which must
// not empty every task's occupancy and leave all workloads ungated. Used to
// decide whether a placement counted as topology-converged or slipped out
// through the valve, so a valve that saved one pod does not move the rest of
// its task.
func SuperpodEscapeValveUsed(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	value, ok := pod.GetAnnotations()[SuperpodSchedulerPolicyAnnotationKey]
	if !ok {
		return false
	}
	switch SchedulerPolicyName(strings.TrimSpace(value)) {
	case SuperpodSchedulerPolicyDisable, SuperpodSchedulerPolicySpill:
		return true
	}
	return false
}

func topoID(node *corev1.Node, key string) int64 {
	if node == nil {
		return TopoIDInvalid
	}
	raw, ok := node.Annotations[key]
	if !ok {
		return TopoIDInvalid
	}
	id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || id < 0 {
		return TopoIDInvalid
	}
	return id
}
