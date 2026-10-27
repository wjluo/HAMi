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
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/topotree"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

// coreMaskAccounted reports whether a vendor stores its per-device cores as a
// bit mask instead of an additive count. Such a figure cannot be compared with
// a requested number of cores, so the sizing drops that dimension rather than
// reading a node as full. It is a variable so the sizing arithmetic can be
// tested without registering a backend that implements device.CoreMaskAccounting.
var coreMaskAccounted = func(vendor string) bool {
	dev, ok := device.GetDevices()[vendor]
	if !ok || dev == nil {
		return false
	}
	_, ok = dev.(device.CoreMaskAccounting)
	return ok
}

// deviceDemand is one pod's request against one device vendor, flattened over
// the pod's containers: the slots, memory, and cores a single replica needs.
type deviceDemand struct {
	slots      int64
	mem        int64
	cores      int64
	memPercent int64
}

// serviceSizing is what measuring a whole task costs: the live device usage of
// the candidate nodes, and the shape of one of the task's pods. It is built per
// scheduling round from the same snapshot the scoring used, so the two cannot
// disagree about how much room a node has.
type serviceSizing struct {
	usage map[string]*NodeUsage
	reqs  device.PodDeviceRequests
}

func newServiceSizing(usage map[string]*NodeUsage, reqs device.PodDeviceRequests) *serviceSizing {
	return &serviceSizing{usage: usage, reqs: reqs}
}

// podDeviceDemand flattens a pod's per-container device requests into one
// demand per vendor. A pod's replicas are placed as whole pods, so the room a
// domain holds is counted in pods, not in cards.
func podDeviceDemand(reqs device.PodDeviceRequests) map[string]deviceDemand {
	out := make(map[string]deviceDemand)
	for _, container := range reqs {
		for vendor, request := range container {
			if request.Nums <= 0 && request.Memreq <= 0 && request.MemPercentagereq <= 0 {
				continue
			}
			key := request.Type
			if key == "" {
				key = vendor
			}
			demand := out[key]
			demand.slots += max(request.Nums, 0)
			demand.mem += max(request.Memreq, 0)
			demand.cores += max(request.Coresreq, 0)
			if request.MemPercentagereq > 0 && request.MemPercentagereq != 101 {
				demand.memPercent += request.MemPercentagereq
			}
			out[key] = demand
		}
	}
	return out
}

// nodePodCapacity returns how many more pods of this shape fit on the node with
// the capacity that is free right now. The answer is the tightest of the
// requested dimensions, and the vendor whose room is missing decides it: a node
// with no such device at all holds none.
//
// Room is aggregated over the node's devices of a vendor because a pod's slots
// may be spread across them, which is what the device fit itself allows. The
// consequence is that this reads a fragmented node as roomier than a per-device
// count would: two devices with half their memory left look like one full pair.
// That is why the sizing only ever expresses a preference and never rejects a
// workload outright.
func nodePodCapacity(usage *NodeUsage, demand map[string]deviceDemand) int64 {
	if usage == nil || len(demand) == 0 {
		return 0
	}
	type freeCapacity struct {
		slots, mem, cores, devices, totalMem int64
	}
	free := make(map[string]*freeCapacity)
	for _, listed := range usage.Devices.DeviceLists {
		dev := listed.Device
		if dev == nil {
			continue
		}
		capacity, ok := free[dev.DeviceVendor]
		if !ok {
			capacity = &freeCapacity{}
			free[dev.DeviceVendor] = capacity
		}
		capacity.devices++
		capacity.slots += max(dev.Count-dev.Used, 0)
		capacity.totalMem += dev.Totalmem
		capacity.mem += max(dev.Totalmem-dev.Usedmem, 0)
		capacity.cores += max(dev.Totalcore-dev.Usedcores, 0)
	}

	var pods int64
	boundedNodes := false
	for vendor, want := range demand {
		capacity, ok := free[vendor]
		if !ok {
			return 0
		}
		var fits int64
		bounded := false
		consider := func(limit int64) {
			if !bounded || limit < fits {
				fits = limit
			}
			bounded = true
		}
		if want.slots > 0 {
			consider(capacity.slots / want.slots)
		}
		if want.mem > 0 {
			consider(capacity.mem / want.mem)
		}
		if want.memPercent > 0 && capacity.devices > 0 {
			// A percentage request reserves a share of each device, so price it
			// against the average device on this node the way the device fit
			// books it.
			perPod := max(capacity.totalMem/capacity.devices*want.memPercent/100, 1)
			consider(capacity.mem / perPod)
		}
		if want.cores > 0 && !coreMaskAccounted(vendor) {
			consider(capacity.cores / want.cores)
		}
		if !bounded {
			// The request named a vendor but no comparable dimension, so it
			// constrains nothing here.
			continue
		}
		if !boundedNodes || fits < pods {
			pods = fits
		}
		boundedNodes = true
	}
	if !boundedNodes {
		return 0
	}
	return pods
}

// taskReplicas is how many pods the task will want, this pod included.
//
// The count is the business side's to state: a scaled or lazily-created
// workload knows its own size before the cluster has seen every pod, and
// hami.io/task-replica-count is where it says so. Failing that, the pods already
// created for the task are the service, which is exact for a Deployment or
// StatefulSet that builds all of its pods up front. Anything else returns one,
// which leaves the sizing inert rather than inventing a constraint from a guess.
func (s *Scheduler) taskReplicas(pod *corev1.Pod, taskID string) int64 {
	if pod == nil {
		return 1
	}
	if raw, ok := pod.GetAnnotations()[util.TaskReplicaCountAnnotationKey]; ok {
		if count, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil && count >= 1 {
			return count
		}
		klog.V(4).Infof("ignoring unusable %s=%q on pod %s/%s, counting the task's pods instead",
			util.TaskReplicaCountAnnotationKey, raw, pod.Namespace, pod.Name)
	}
	if taskID == "" || s.podLister == nil {
		return 1
	}
	pods, err := s.podLister.List(labels.NewSelector())
	if err != nil {
		klog.V(4).Infof("cannot list pods to size task %s: %v", taskID, err)
		return 1
	}
	var count int64
	seenSelf := false
	for _, live := range pods {
		if live == nil || isPodFinished(live) || util.GetTaskID(live) != taskID {
			continue
		}
		if pod.UID != "" && live.UID == pod.UID {
			seenSelf = true
		}
		count++
	}
	if !seenSelf {
		count++
	}
	return count
}

// shrinkToSizedDomains holds back the candidate superpods that cannot hold the
// whole task, so the first pod opens where the rest of its replicas can follow.
// It returns the candidates to keep, the ones held back for capacity, and the
// figures to report.
//
// Two rules keep this a preference and not a second gate. Nothing is removed
// unless at least one superpod among the candidates can hold the task, because
// the room measured here is an estimate and a workload must not be stranded by
// one node's stale usage. And a node that reports no topology belongs to no
// domain, so no domain shortage can be shown against it.
func (s *Scheduler) shrinkToSizedDomains(entries []topotree.Entry, pod *corev1.Pod, taskID string, sizing *serviceSizing) (kept, held []topotree.Entry, replicas int64, hold, short string) {
	if sizing == nil || len(entries) == 0 {
		return entries, nil, 0, "", ""
	}
	demand := podDeviceDemand(sizing.reqs)
	if len(demand) == 0 {
		return entries, nil, 0, "", ""
	}
	count := s.taskReplicas(pod, taskID)
	if count < 2 {
		return entries, nil, 0, "", ""
	}

	capacity := make(map[int64]int64)
	for _, entry := range entries {
		if entry.SuperPodID < 0 {
			continue
		}
		capacity[entry.SuperPodID] += nodePodCapacity(sizing.usage[entry.NodeID], demand)
	}
	if len(capacity) == 0 {
		return entries, nil, count, "", ""
	}
	// The usage the scoring handed over is the node as it would look with this
	// pod already on it, so the pod being placed has to be added back before a
	// domain is compared with the task: a superpod with room for 7 more of an
	// 8-replica task holds all 8, not 7.
	fits := make(map[int64]bool)
	var largest int64
	var largestIDs []int64
	for _, superpodID := range slices.Sorted(maps.Keys(capacity)) {
		room := capacity[superpodID] + 1
		capacity[superpodID] = room
		if room >= count {
			fits[superpodID] = true
		}
		if room > largest {
			largest = room
			largestIDs = []int64{superpodID}
		} else if room == largest {
			largestIDs = append(largestIDs, superpodID)
		}
	}
	if len(fits) == 0 {
		return entries, nil, count, "", fmt.Sprintf(
			"no candidate superpod holds %d replica(s); largest holds %d (superpod %s)",
			count, largest, strings.Join(idList(largestIDs), ","))
	}

	kept = make([]topotree.Entry, 0, len(entries))
	var heldDomains []int64
	for _, entry := range entries {
		if entry.SuperPodID < 0 || fits[entry.SuperPodID] {
			kept = append(kept, entry)
			continue
		}
		held = append(held, entry)
		if !slices.Contains(heldDomains, entry.SuperPodID) {
			heldDomains = append(heldDomains, entry.SuperPodID)
		}
	}
	if len(held) == 0 {
		return entries, nil, count, "", ""
	}
	slices.Sort(heldDomains)
	parts := make([]string, 0, len(heldDomains))
	for _, superpodID := range heldDomains {
		parts = append(parts, fmt.Sprintf("superpod %d holds %d of %d replica(s)", superpodID, capacity[superpodID], count))
	}
	return kept, held, count, strings.Join(parts, ", "), ""
}

// idList renders superpod ids for the shortfall message.
func idList(ids []int64) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, strconv.FormatInt(id, 10))
	}
	return out
}

// capacityClause renders the sizing for the Pod Event, or nothing when the
// layer had no opinion. A first pod that avoided an under-sized superpod and a
// task that could not fit anywhere need explaining differently: the first is a
// decision this scheduler made, the second is a capacity shortfall the operator
// has to see.
func (o superpodOutcome) capacityClause() string {
	if o.CapacityHold != "" {
		return fmt.Sprintf("; sizing held back superpod(s): %s", o.CapacityHold)
	}
	if o.CapacityShort != "" {
		return "; sizing: " + o.CapacityShort
	}
	return ""
}
