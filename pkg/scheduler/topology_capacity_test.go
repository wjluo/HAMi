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

package scheduler

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"

	"gotest.tools/v3/assert"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/device/nvidia"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/policy"
	"github.com/Project-HAMi/HAMi/pkg/util"
	"github.com/Project-HAMi/HAMi/pkg/util/client"
)

// The pod shape these cases size against: one whole slot, 1000MB, 10 cores, one
// device vendor. sizedUsage hands a node exactly enough room for N more copies
// of it, so a test can say "this superpod holds 3 pods of an 8-pod task"
// without repeating the arithmetic that turns free capacity into a pod count.
const (
	sizeSlots = int64(1)
	sizeMem   = int64(1000)
	sizeCores = int64(10)
)

func sizingRequests() device.PodDeviceRequests {
	return device.PodDeviceRequests{
		device.ContainerDeviceRequests{
			nvidia.NvidiaGPUDevice: device.ContainerDeviceRequest{
				Nums:     sizeSlots,
				Type:     nvidia.NvidiaGPUDevice,
				Memreq:   sizeMem,
				Coresreq: sizeCores,
			},
		},
	}
}

// sizedUsage builds the candidate node's usage view the way getNodesUsage and
// the scoring leave it: one device per entry, with the room that is still free
// on the node *after* the pod being placed has booked its own share. So
// sizedUsage(node, 7) is a node that can take 7 more replicas of this shape,
// which is 8 for a task that opens with the pod on the list.
func sizedUsage(nodeID string, pods int64) *NodeUsage {
	return &NodeUsage{
		Devices: policy.DeviceUsageList{
			DeviceLists: []*policy.DeviceListsScore{{
				Device: &device.DeviceUsage{
					ID:           nodeID + "-gpu",
					Count:        pods * sizeSlots,
					Totalmem:     pods * sizeMem,
					Totalcore:    pods * sizeCores,
					Type:         "NVIDIA-A100-SXM4-40GB",
					DeviceVendor: nvidia.NvidiaGPUDevice,
					Health:       true,
				},
			}},
		},
	}
}

func sizedUsageMap(entries map[string]int64) map[string]*NodeUsage {
	out := make(map[string]*NodeUsage, len(entries))
	for nodeID, pods := range entries {
		out[nodeID] = sizedUsage(nodeID, pods)
	}
	return out
}

func sizingFor(entries map[string]int64) *serviceSizing {
	return newServiceSizing(sizedUsageMap(entries), sizingRequests())
}

// podWithReplicas is a task pod whose service size is declared by the business
// side, which is the authoritative source for the pre-placement sizing.
func podWithReplicas(name, taskID string, replicas int) *corev1.Pod {
	pod := taskPod(name, taskID)
	pod.Annotations = map[string]string{util.TaskReplicaCountAnnotationKey: strconv.Itoa(replicas)}
	return pod
}

// The demand a pod carries is per container and per vendor, but the room a node
// has is per device. Flattening a pod's containers into one per-vendor demand is
// what lets a node's free slots, memory, and cores be compared with it.
func TestPodDeviceDemandSumsContainersPerVendor(t *testing.T) {
	reqs := device.PodDeviceRequests{
		device.ContainerDeviceRequests{
			nvidia.NvidiaGPUDevice: device.ContainerDeviceRequest{Nums: 2, Type: nvidia.NvidiaGPUDevice, Memreq: 1000, Coresreq: 20},
		},
		device.ContainerDeviceRequests{
			nvidia.NvidiaGPUDevice: device.ContainerDeviceRequest{Nums: 1, Type: nvidia.NvidiaGPUDevice, Memreq: 500, Coresreq: 10},
		},
	}
	demand := podDeviceDemand(reqs)
	assert.Equal(t, 1, len(demand))
	assert.Equal(t, int64(3), demand[nvidia.NvidiaGPUDevice].slots)
	assert.Equal(t, int64(1500), demand[nvidia.NvidiaGPUDevice].mem)
	assert.Equal(t, int64(30), demand[nvidia.NvidiaGPUDevice].cores)
}

// A container that asks for nothing must not contribute a zero-demand entry:
// dividing by a zero request would otherwise report infinite room.
func TestPodDeviceDemandSkipsContainersThatAskForNothing(t *testing.T) {
	demand := podDeviceDemand(device.PodDeviceRequests{
		device.ContainerDeviceRequests{nvidia.NvidiaGPUDevice: device.ContainerDeviceRequest{Nums: 0, Type: nvidia.NvidiaGPUDevice}},
		device.ContainerDeviceRequests{},
	})
	assert.Equal(t, 0, len(demand))
}

// Room is the tightest of the requested dimensions, not the loosest: a node
// with 8 free slots but memory for only 3 pods of this shape holds 3 replicas.
func TestNodePodCapacityIsTheTightestDimension(t *testing.T) {
	cases := []struct {
		name           string
		slots, mem, co int64
		want           int64
	}{
		{"slots bind", 3, 10 * sizeMem, 10 * sizeCores, 3},
		{"memory binds", 10, 4 * sizeMem, 10 * sizeCores, 4},
		{"cores bind", 10, 10 * sizeMem, 2 * sizeCores, 2},
		{"exact fit", 5, 5 * sizeMem, 5 * sizeCores, 5},
		{"one dimension short of a single pod", 5, sizeMem - 1, 5 * sizeCores, 0},
		{"nothing free", 0, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			usage := &NodeUsage{Devices: policy.DeviceUsageList{DeviceLists: []*policy.DeviceListsScore{{
				Device: &device.DeviceUsage{
					ID: "gpu", Count: tc.slots, Totalmem: tc.mem, Totalcore: tc.co,
					DeviceVendor: nvidia.NvidiaGPUDevice,
				},
			}}}}
			assert.Equal(t, tc.want, nodePodCapacity(usage, podDeviceDemand(sizingRequests())))
		})
	}
}

// A percentage-memory request reserves a share of each device rather than an
// absolute number of megabytes, and the room has to be priced the same way Fit
// books it. 101 is the "not actually a percentage" sentinel the backends use.
func TestNodePodCapacityConvertsMemoryPercentage(t *testing.T) {
	reqs := device.PodDeviceRequests{
		device.ContainerDeviceRequests{
			nvidia.NvidiaGPUDevice: device.ContainerDeviceRequest{
				Nums: 1, Type: nvidia.NvidiaGPUDevice, MemPercentagereq: 25,
			},
		},
	}
	usage := &NodeUsage{Devices: policy.DeviceUsageList{DeviceLists: []*policy.DeviceListsScore{{
		Device: &device.DeviceUsage{ID: "gpu", Count: 10, Totalmem: 8000, Totalcore: 100, DeviceVendor: nvidia.NvidiaGPUDevice},
	}}}}
	// 25% of 8000MB is 2000MB, so 4 pods fit the memory even though 10 slots are free.
	assert.Equal(t, int64(4), nodePodCapacity(usage, podDeviceDemand(reqs)))

	usage.Devices.DeviceLists[0].Device.Totalmem = 8000
	sentinel := device.PodDeviceRequests{
		device.ContainerDeviceRequests{
			nvidia.NvidiaGPUDevice: device.ContainerDeviceRequest{
				Nums: 1, Type: nvidia.NvidiaGPUDevice, MemPercentagereq: 101,
			},
		},
	}
	assert.Equal(t, int64(10), nodePodCapacity(usage, podDeviceDemand(sentinel)))
}

// Backends that store cores as a bit mask cannot be compared with an additive
// core demand; for those the core dimension must drop out instead of reporting
// a node as full.
func TestNodePodCapacitySkipsMaskAccountedCores(t *testing.T) {
	usage := &NodeUsage{Devices: policy.DeviceUsageList{DeviceLists: []*policy.DeviceListsScore{{
		Device: &device.DeviceUsage{
			ID: "node-gpu", Count: 4, Totalmem: 4 * sizeMem, Totalcore: 2 * sizeCores,
			DeviceVendor: nvidia.NvidiaGPUDevice,
		},
	}}}}
	demand := podDeviceDemand(sizingRequests())
	assert.Equal(t, int64(2), nodePodCapacity(usage, demand), "cores are the tight dimension when they are additive")

	restore := coreMaskAccounted
	coreMaskAccounted = func(vendor string) bool { return vendor == nvidia.NvidiaGPUDevice }
	t.Cleanup(func() { coreMaskAccounted = restore })

	// The same node read through a mask-accounting backend has no comparable
	// core figure, so the pods the slots and memory allow is the answer.
	assert.Equal(t, int64(4), nodePodCapacity(usage, demand))
}

// A node with no entry for a requested vendor cannot host the pod at all.
func TestNodePodCapacityIsZeroWithoutTheRequestedVendor(t *testing.T) {
	usage := &NodeUsage{Devices: policy.DeviceUsageList{DeviceLists: []*policy.DeviceListsScore{{
		Device: &device.DeviceUsage{ID: "mlu", Count: 10, Totalmem: 100000, Totalcore: 100, DeviceVendor: "cambricon"},
	}}}}
	assert.Equal(t, int64(0), nodePodCapacity(usage, podDeviceDemand(sizingRequests())))
}

// Used capacity is subtracted, so a node the sizing sees as half-taken reports
// the room that is left rather than its raw geometry.
func TestNodePodCapacitySubtractsWhatIsAlreadyUsed(t *testing.T) {
	usage := sizedUsage("node", 6)
	device0 := usage.Devices.DeviceLists[0].Device
	device0.Used = 2
	device0.Usedmem = 2 * sizeMem
	device0.Usedcores = 2 * sizeCores
	assert.Equal(t, int64(4), nodePodCapacity(usage, podDeviceDemand(sizingRequests())))
}

// The declared size of a task: the annotation the business side writes wins,
// because a scaled-out service knows its own replica count before the cluster
// has seen all of its pods.
func TestTaskReplicasPrefersTheDeclaredCount(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	client.KubeClient = fake.NewClientset()
	s.kubeClient = client.KubeClient
	factory := informers.NewSharedInformerFactoryWithOptions(client.KubeClient, time.Hour)
	s.podLister = factory.Core().V1().Pods().Lister()

	for _, name := range []string{"a", "b", "c"} {
		pod := taskPod(name, "svc-size")
		pod.Annotations = map[string]string{util.TaskReplicaCountAnnotationKey: "8"}
		assert.NilError(t, factory.Core().V1().Pods().Informer().GetIndexer().Add(pod))
	}
	assert.Equal(t, int64(8), s.taskReplicas(podWithReplicas("d", "svc-size", 8), "svc-size"))
}

// Without a declaration, the pods already created for the task are the count:
// a Deployment or StatefulSet builds all of its pods up front, so the siblings
// in the informer are the service. Terminated pods are not part of it.
func TestTaskReplicasCountsTheTaskPodsInTheCluster(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	client.KubeClient = fake.NewClientset()
	s.kubeClient = client.KubeClient
	factory := informers.NewSharedInformerFactoryWithOptions(client.KubeClient, time.Hour)
	s.podLister = factory.Core().V1().Pods().Lister()

	for _, name := range []string{"p-0", "p-1", "p-2"} {
		pod := taskPod(name, "svc-count")
		pod.UID = k8stypes.UID(name)
		assert.NilError(t, factory.Core().V1().Pods().Informer().GetIndexer().Add(pod))
	}
	done := taskPod("p-3", "svc-count")
	done.UID = k8stypes.UID("p-3")
	done.Status.Phase = corev1.PodSucceeded
	assert.NilError(t, factory.Core().V1().Pods().Informer().GetIndexer().Add(done))
	foreign := taskPod("other-0", "svc-elsewhere")
	foreign.UID = k8stypes.UID("other-0")
	assert.NilError(t, factory.Core().V1().Pods().Informer().GetIndexer().Add(foreign))

	// The pod being scheduled has not reached the informer yet, so it counts as
	// one more: the figure is the whole service, not its siblings.
	assert.Equal(t, int64(4), s.taskReplicas(taskPod("p-4", "svc-count"), "svc-count"))
}

// A service whose size cannot be established must not be sized at all: falling
// back to one replica makes the layer inert rather than inventing a constraint.
// A nonsense annotation counts the same way as a missing one.
func TestTaskReplicasFallsBackToNoConstraint(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	assert.Equal(t, int64(1), s.taskReplicas(taskPod("p-0", "svc-unknown"), "svc-unknown"))
	for _, bad := range []string{"", "many", "0", "-3"} {
		pod := taskPod("p-0", "svc-bad")
		pod.Annotations = map[string]string{util.TaskReplicaCountAnnotationKey: bad}
		assert.Equal(t, int64(1), s.taskReplicas(pod, "svc-bad"), "annotation %q", bad)
	}
}

// The whole point of pre-sizing: the first pod opens in the domain that can
// hold the service, not in the one the packing score or the intent hash liked.
// Superpod 1 has room for 2 more replicas of an 8-replica service and is held
// back, which is what keeps the rest of the service from arriving to a full
// domain one replica at a time.
func TestFirstPodOpensTheDomainThatHoldsTheService(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	small := topoNode("node-small-sp", "1", "1", "1")
	big := topoNode("node-big-sp", "2", "2", "2")
	registerTopoNode(t, s, small)
	registerTopoNode(t, s, big)

	scores := scoreList(nodeScore(small, 90), nodeScore(big, 10))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithReplicas("svc-0", "svc-size", 8),
		sizingFor(map[string]int64{"node-small-sp": 2, "node-big-sp": 8}))

	assert.Equal(t, "node-big-sp", scores.NodeList[len(scores.NodeList)-1].NodeID, "order=%v", orderOf(scores))
	assert.Equal(t, int64(8), outcome.Replicas)
	assert.Assert(t, strings.Contains(outcome.CapacityHold, "superpod 1"), "CapacityHold=%q", outcome.CapacityHold)
	assert.Assert(t, dropped["node-small-sp"] != "", "the held-back domain has to be reported, got %v", dropped)
	assert.Assert(t, strings.Contains(dropped["node-small-sp"], "3 of 8"), "reason=%q", dropped["node-small-sp"])
	assert.Assert(t, strings.Contains(outcome.EventSuffix(), "8 replica"), "event=%q", outcome.EventSuffix())
}

// Delivery outranks a preference: when no candidate domain can hold the whole
// service, nothing may be removed for it. The pod still has to land, and the
// sizing is an estimate - so it reports the shortfall instead of stranding the
// workload on a number the extender cannot guarantee.
func TestSizingKeepsEveryCandidateWhenNoDomainIsBigEnough(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	a := topoNode("node-a", "1", "1", "1")
	b := topoNode("node-b", "2", "2", "2")
	registerTopoNode(t, s, a)
	registerTopoNode(t, s, b)

	scores := scoreList(nodeScore(a, 3), nodeScore(b, 9))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithReplicas("svc-0", "svc-short", 8),
		sizingFor(map[string]int64{"node-a": 3, "node-b": 5}))

	// The deterministic intent still converges the task into one of the two
	// domains, so one node is gated as foreign exactly as it was before this
	// layer existed. What the sizing must not add is a capacity removal.
	assert.Equal(t, 1, len(dropped), "dropped=%v", dropped)
	for _, reason := range dropped {
		assert.Assert(t, !strings.Contains(reason, "held back"), "reason=%q", reason)
	}
	assert.Equal(t, 1, len(scores.NodeList), "order=%v", orderOf(scores))
	assert.Assert(t, strings.Contains(outcome.CapacityShort, "8"), "CapacityShort=%q", outcome.CapacityShort)
	assert.Assert(t, strings.Contains(outcome.CapacityShort, "6"), "CapacityShort=%q", outcome.CapacityShort)
	assert.Assert(t, strings.Contains(outcome.EventSuffix(), "no candidate superpod"), "event=%q", outcome.EventSuffix())
}

// The sizing is a first-pod decision. Once the task sits somewhere, its domain
// is already chosen and a sibling that re-measured it would reopen a settled
// question every time a node's free capacity ticks.
func TestSizingIsSkippedForABoundTasksSibling(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	home := topoNode("node-home", "1", "1", "1")
	rich := topoNode("node-rich", "2", "2", "2")
	registerTopoNode(t, s, home)
	registerTopoNode(t, s, rich)
	s.podManager.AddPod(taskPod("svc-holder", "svc-bound"), "node-home", device.PodDevices{})

	scores := scoreList(nodeScore(home, 5), nodeScore(rich, 50))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithReplicas("svc-1", "svc-bound", 8),
		sizingFor(map[string]int64{"node-home": 1, "node-rich": 40}))

	// The rich foreign domain is gated out as foreign, exactly as before this
	// layer existed, and no capacity reasoning is attached to the decision.
	assert.Equal(t, int64(0), outcome.Replicas, "sizing must not run for a sibling")
	assert.Assert(t, outcome.CapacityHold == "" && outcome.CapacityShort == "")
	assert.DeepEqual(t, []string{"node-rich"}, droppedIDs(dropped))
	assert.Equal(t, "node-home", scores.NodeList[len(scores.NodeList)-1].NodeID)
}

// A node that reports no topology is not in any domain, so no domain can be
// shown to be too small for it. The gate exempts such candidates and so must
// the sizing, or one missing annotation silently shrinks the service's options.
func TestSizingLeavesUnannotatedCandidatesAlone(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	plain := topoNode("node-plain", "", "", "")
	big := topoNode("node-big", "2", "2", "2")
	registerTopoNode(t, s, plain)
	registerTopoNode(t, s, big)

	scores := scoreList(nodeScore(plain, 1), nodeScore(big, 2))
	_, outcome := s.applySuperpodPolicy(scores,
		podWithReplicas("svc-0", "svc-mixed", 8),
		sizingFor(map[string]int64{"node-plain": 0, "node-big": 8}))

	assert.Equal(t, 2, len(scores.NodeList), "order=%v", orderOf(scores))
	assert.Assert(t, outcome.CapacityHold == "", "CapacityHold=%q", outcome.CapacityHold)
	assert.Equal(t, "node-big", scores.NodeList[len(scores.NodeList)-1].NodeID)
}

// No sizing input means no sizing: the candidate list, the anchor, and the
// reports have to be exactly what they were before this layer existed.
func TestSizingAbsentLeavesThePolicyUntouched(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	small := topoNode("node-small-sp", "1", "1", "1")
	big := topoNode("node-big-sp", "2", "2", "2")
	registerTopoNode(t, s, small)
	registerTopoNode(t, s, big)

	scores := scoreList(nodeScore(small, 90), nodeScore(big, 10))
	_, outcome := s.applySuperpodPolicy(scores, podWithReplicas("svc-0", "svc-size", 8), nil)

	assert.Equal(t, int64(0), outcome.Replicas)
	assert.Assert(t, outcome.CapacityHold == "" && outcome.CapacityShort == "")
}

// The usage the scoring hands over already has this pod booked into it, so the
// pod being placed must be counted on the side of the domain it opens: a
// superpod with room for 7 more of an 8-replica task holds all 8. Read the
// other way round, an exactly-sized domain would look one replica short and the
// sizing would quietly never fire on the clusters it was written for.
func TestSizingCountsThePodBeingPlacedInTheDomainItOpens(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	exact := topoNode("node-exact", "1", "1", "1")
	short := topoNode("node-short", "2", "2", "2")
	registerTopoNode(t, s, exact)
	registerTopoNode(t, s, short)

	scores := scoreList(nodeScore(exact, 1), nodeScore(short, 99))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithReplicas("svc-0", "svc-exact", 8),
		sizingFor(map[string]int64{"node-exact": 7, "node-short": 6}))

	assert.Equal(t, 1, len(scores.NodeList), "order=%v", orderOf(scores))
	assert.Equal(t, "node-exact", scores.NodeList[len(scores.NodeList)-1].NodeID)
	assert.DeepEqual(t, []string{"node-short"}, droppedIDs(dropped))
	assert.Assert(t, strings.Contains(outcome.CapacityHold, "superpod 2 holds 7 of 8"), "CapacityHold=%q", outcome.CapacityHold)
}

// One replica is the whole service, and every candidate already fits one pod -
// that is why it is on the list. Measuring a single-replica task would be
// arithmetic with no possible effect, so it must not even be reported.
func TestSizingIsInertForASingleReplica(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	small := topoNode("node-small-sp", "1", "1", "1")
	big := topoNode("node-big-sp", "2", "2", "2")
	registerTopoNode(t, s, small)
	registerTopoNode(t, s, big)

	scores := scoreList(nodeScore(small, 90), nodeScore(big, 10))
	dropped, outcome := s.applySuperpodPolicy(scores,
		taskPod("svc-0", "svc-one"),
		sizingFor(map[string]int64{"node-small-sp": 1, "node-big-sp": 1}))

	// Whatever the ordinary convergence does with the two domains is unchanged;
	// what must not appear is any capacity reasoning.
	assert.Equal(t, int64(0), outcome.Replicas)
	for _, reason := range dropped {
		assert.Assert(t, !strings.Contains(reason, "held back"), "reason=%q", reason)
	}
	assert.Assert(t, outcome.CapacityHold == "" && outcome.CapacityShort == "")
}

// A pod that requests no accelerator has nothing to size. Without this the
// division would report infinite room and the layer would gate candidates for
// a workload whose shape it cannot even measure.
func TestSizingIsInertForAPodWithNoDeviceRequest(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })

	a := topoNode("node-a", "1", "1", "1")
	b := topoNode("node-b", "2", "2", "2")
	registerTopoNode(t, s, a)
	registerTopoNode(t, s, b)

	scores := scoreList(nodeScore(a, 3), nodeScore(b, 4))
	dropped, outcome := s.applySuperpodPolicy(scores,
		podWithReplicas("svc-0", "svc-cpu", 8),
		newServiceSizing(sizedUsageMap(map[string]int64{"node-a": 0, "node-b": 0}), device.PodDeviceRequests{{}}))

	assert.Equal(t, int64(0), outcome.Replicas, "there is no device shape to size, so the layer must stay silent")
	for _, reason := range dropped {
		assert.Assert(t, !strings.Contains(reason, "held back"), "reason=%q", reason)
	}
	assert.Assert(t, outcome.CapacityHold == "" && outcome.CapacityShort == "")
}

// The operator has to be able to tell "the service opened where it could fit"
// apart from "the service opened where the hash put it", because the first is a
// capacity decision and the second is not.
func TestSizingOutcomeRendersForTheEvent(t *testing.T) {
	hold := superpodOutcome{
		SelectedNodeID: "node-big-sp", Tier: "same-superpod", SuperPodID: 2,
		TaskID: "svc-size", Replicas: 8, Domains: "2", IntentAnchor: true,
		CapacityHold: "superpod 1 holds 2 of 8 replica(s)",
	}
	event := hold.EventSuffix()
	assert.Assert(t, strings.Contains(event, "held back"), "event=%q", event)
	assert.Assert(t, strings.Contains(event, "superpod 1 holds 2 of 8"), "event=%q", event)

	short := superpodOutcome{
		SelectedNodeID: "node-b", Tier: "same-superpod", SuperPodID: 2,
		TaskID: "svc-short", Replicas: 8, Domains: "2", IntentAnchor: true,
		CapacityShort: "no candidate superpod holds 8 replica(s); largest holds 5",
	}
	event = short.EventSuffix()
	assert.Assert(t, strings.Contains(event, "largest holds 5"), "event=%q", event)
}

// Everything above calls the policy directly. This one proves the sizing is
// wired into Filter with the real usage of the candidate nodes: an 8-replica
// service must not open in a superpod that has room for one. Several task ids
// are run through it because the deterministic intent hashes them onto either
// superpod - only the sizing makes the answer the same for all of them.
func TestFilterWiringFirstPodOpensTheSizedDomain(t *testing.T) {
	for _, taskID := range []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "svc-f", "svc-g", "svc-h"} {
		t.Run(taskID, func(t *testing.T) {
			s := NewScheduler()
			t.Cleanup(func() { close(s.stopCh) })
			client.KubeClient = fake.NewClientset()
			s.kubeClient = client.KubeClient
			s.podLister = informers.NewSharedInformerFactoryWithOptions(client.KubeClient, time.Hour).Core().V1().Pods().Lister()
			initTopoDevices(t)

			// One slot each, so a 1-card pod shape has 1 unit of room left on
			// both nodes once this pod is booked into the usage.
			roomy := device.DeviceInfo{ID: "gpu-roomy", Count: 8, Devmem: 8 * sizeMem, Devcore: 8 * sizeCores, Type: "NVIDIA-A100", DeviceVendor: nvidia.NvidiaGPUDevice, Health: true}
			tight := device.DeviceInfo{ID: "gpu-tight", Count: 1, Devmem: sizeMem, Devcore: sizeCores, Type: "NVIDIA-A100", DeviceVendor: nvidia.NvidiaGPUDevice, Health: true}
			s.addNode("node-tight", &device.NodeInfo{
				ID: "node-tight", Node: topoNode("node-tight", "1", "1", "1"),
				Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {tight}},
			})
			s.addNode("node-roomy", &device.NodeInfo{
				ID: "node-roomy", Node: topoNode("node-roomy", "2", "2", "2"),
				Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {roomy}},
			})

			pending := topoGPUPod("decode-0", "decode-0-uid", taskID, sizeMem, sizeCores)
			pending.Annotations = map[string]string{util.TaskReplicaCountAnnotationKey: "8"}
			_, err := client.KubeClient.CoreV1().Pods(pending.Namespace).Create(context.Background(), pending, metav1.CreateOptions{})
			assert.NilError(t, err)

			res, err := s.Filter(extenderv1.ExtenderArgs{Pod: pending, NodeNames: &[]string{"node-tight", "node-roomy"}})
			assert.NilError(t, err)
			assert.Assert(t, res.NodeNames != nil, "a sized domain is available, so the pod must not be left pending")
			assert.DeepEqual(t, []string{"node-roomy"}, []string(*res.NodeNames))
		})
	}
}
