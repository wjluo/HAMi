/*
Copyright 2024 The HAMi Authors.

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

const (
	AssignedTimeAnnotations = "hami.io/vgpu-time"
	AssignedNodeAnnotations = "hami.io/vgpu-node"
	BindTimeAnnotations     = "hami.io/bind-time"
	DeviceBindPhase         = "hami.io/bind-phase"

	DeviceBindAllocating = "allocating"
	DeviceBindFailed     = "failed"
	DeviceBindSuccess    = "success"

	DeviceLimit = 100
	//TimeLayout = "ANSIC"
	//DefaultTimeout = time.Second * 60.

	BestEffort string = "best-effort"
	Restricted string = "restricted"
	Guaranteed string = "guaranteed"

	// NodeNameEnvName define env var name for use get node name.
	NodeNameEnvName = "NODE_NAME"
	TaskPriority    = "CUDA_TASK_PRIORITY"
	CoreLimitSwitch = "GPU_CORE_UTILIZATION_POLICY"

	// LeaderRoleLabel is the label key used to identify the leader pod.
	HAMiRoleLabel = "hami.io/scheduler-role"
	// LeaderRoleLabelValueLeader is the label value used to identify the leader pod.
	HAMiRoleLabelValueLeader = "leader"
	// LeaderRoleLabelValueFollower is the label value used to identify the follower pod.
	HAMiRoleLabelValueFollower = "follower"

	// HAMiSchedulerLabelValue is the label key to identify the component of HAMi in Kubernetes.
	HAMiComponentLabel = "app.kubernetes.io/component"
	// HAMiComponentScheduler the label value for hami-scheduler.
	HAMiComponentScheduler = "hami-scheduler"

	// PodGroupLabel is the label used by scheduler-plugins Coscheduling to mark
	// a pod as a member of a PodGroup. See
	// https://github.com/kubernetes-sigs/scheduler-plugins/blob/master/apis/scheduling/v1alpha1/types.go
	PodGroupLabel = "scheduling.x-k8s.io/pod-group"
)

var (
	DebugMode         bool
	NodeName          string
	RuntimeSocketFlag string
)

type SchedulerPolicyName string

const (
	// NodeSchedulerPolicyBinpack is node use binpack scheduler policy.
	NodeSchedulerPolicyBinpack SchedulerPolicyName = "binpack"
	// NodeSchedulerPolicySpread is node use spread scheduler policy.
	NodeSchedulerPolicySpread SchedulerPolicyName = "spread"
	// GPUSchedulerPolicyBinpack is GPU use binpack scheduler.
	GPUSchedulerPolicyBinpack SchedulerPolicyName = "binpack"
	// GPUSchedulerPolicySpread is GPU use spread scheduler.
	GPUSchedulerPolicySpread SchedulerPolicyName = "spread"
	// GPUSchedulerPolicyTopology is GPU use topology scheduler.
	GPUSchedulerPolicyTopology SchedulerPolicyName = "topology-aware"
	// GPUSchedulerPolicyMutex is GPU use mutex scheduler, only idle GPUs are allocated.
	GPUSchedulerPolicyMutex SchedulerPolicyName = "mutex"
	// GPUSchedulerPolicyNuma is GPU use numa scheduler, chained as a sort key alongside binpack/spread.
	GPUSchedulerPolicyNuma SchedulerPolicyName = "numa"
	// SuperpodSchedulerPolicyEnable keeps a converged workload inside the
	// superpod domain it already occupies.
	SuperpodSchedulerPolicyEnable SchedulerPolicyName = "enable"
	// SuperpodSchedulerPolicySpill keeps the workload inside its occupied
	// superpod as long as any candidate node in that superpod fits the pod,
	// and falls back to foreign superpods the moment the domain has no fitting
	// candidate this round. The fallback is placed and reported, not silently
	// opened: the placement event and the decision log name the domain that
	// spilled so an operator can see the topology/capacity trade without
	// reading scheduler internals. It exists because pre-sizing a domain from
	// a replica count is not visible at the extender /filter layer, but "the
	// occupied domain has no fitting candidate for this pod" is.
	SuperpodSchedulerPolicySpill SchedulerPolicyName = "spill"
	// SuperpodSchedulerPolicyDisable leaves node choice to binpack/spread alone.
	SuperpodSchedulerPolicyDisable SchedulerPolicyName = "disable"
)

const (
	// NodeSchedulerPolicyAnnotationKey is user set Pod annotation to change this default node policy.
	NodeSchedulerPolicyAnnotationKey = "hami.io/node-scheduler-policy"
	// GPUSchedulerPolicyAnnotationKey is user set Pod annotation to change this default GPU policy.
	GPUSchedulerPolicyAnnotationKey = "hami.io/gpu-scheduler-policy"
	// SuperpodSchedulerPolicyAnnotationKey is user set Pod annotation to change
	// the cluster's superpod domain policy for this Pod alone. See
	// SuperpodSchedulerPolicyEnable, SuperpodSchedulerPolicySpill and
	// SuperpodSchedulerPolicyDisable.
	SuperpodSchedulerPolicyAnnotationKey = "hami.io/superpod-scheduler-policy"
	// DeviceScoringWeightsAnnotationKey configures per-Pod slot, core, and memory scoring weights.
	DeviceScoringWeightsAnnotationKey = "hami.io/device-scoring-weights"
	// TaskReplicaCountAnnotationKey is how a workload states the size of the
	// task its Pod belongs to, so the scheduler can open the first replica in a
	// superpod that will still hold the rest. A task that is scaled out or
	// creates its Pods lazily knows its own size before the cluster sees every
	// Pod; without this annotation the scheduler counts the Pods already created
	// for the task. See hami.io/taskid.
	TaskReplicaCountAnnotationKey = "hami.io/task-replica-count"
	// NumaAlignmentAnnotationKey is a per-Pod annotation selecting how HAMi
	// reconciles the scheduler-selected GPU with the NUMA-aligned replica
	// preferred by kubelet's Topology Manager. See NumaAlignmentMode.
	NumaAlignmentAnnotationKey = "hami.io/numa-alignment"
)

// Node-level superpod topology annotations, reported by each vendor's device
// plugin (or set by an operator for chips without a topology API). They are
// read-only for HAMi: the scheduler never writes or garbage-collects them.
const (
	// TopoSuperPodIDAnnotationKey is the outermost convergence domain. Required
	// on every node of a superpod cluster.
	TopoSuperPodIDAnnotationKey = "hami.io/superpodid"
	// TopoRackIDAnnotationKey is the intermediate tier inside a superpod.
	// Absent on chips without a rack tier.
	TopoRackIDAnnotationKey = "hami.io/rackid"
	// TopoServerIDAnnotationKey is the server index inside a superpod, used for
	// observability rather than for scheduling decisions.
	TopoServerIDAnnotationKey = "hami.io/serverid"
)

// Pod label identifying one scheduling task: a training job, or one inference
// service including both its prefill and decode halves. Every Pod sharing a
// value is converged into the same topology domain. The two inferserviceid
// spellings are read only for workloads already deployed - the bare one for
// jobs labelled the way Huawei's volcano plugin labels them - and are dropped
// once nothing in the fleet hits them any more.
const (
	TaskIDLabel           = "hami.io/taskid"
	TaskIDLabelLegacy     = "hami.io/inferserviceid"
	TaskIDLabelLegacyBare = "inferserviceid"
)

func (s SchedulerPolicyName) String() string {
	return string(s)
}

const (
	Weight int = 10
)
