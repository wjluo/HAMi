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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/device/nvidia"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/util"
	"github.com/Project-HAMi/HAMi/pkg/util/client"
)

func initTopoDevices(t *testing.T) {
	t.Helper()
	sConfig := &config.Config{
		NvidiaConfig: nvidia.NvidiaConfig{
			ResourceCountName:            "hami.io/gpu",
			ResourceMemoryName:           "hami.io/gpumem",
			ResourceMemoryPercentageName: "hami.io/gpumem-percentage",
			ResourceCoreName:             "hami.io/gpucores",
			DefaultMemory:                0,
			DefaultCores:                 0,
			DefaultGPUNum:                1,
		},
	}
	require.NoError(t, config.InitDevicesWithConfig(sConfig))
	config.NodeSchedulerPolicy = util.NodeSchedulerPolicyBinpack.String()
}

func topoGPUDevice(id string) device.DeviceInfo {
	return device.DeviceInfo{
		ID:           id,
		Count:        10,
		Devmem:       8000,
		Devcore:      100,
		Type:         nvidia.NvidiaGPUDevice,
		Mode:         "hami",
		Health:       true,
		DeviceVendor: nvidia.NvidiaGPUDevice,
	}
}

func topoGPUPod(name, uid, serviceID string, mem, cores int64) *corev1.Pod {
	labels := map[string]string{}
	if serviceID != "" {
		labels[util.InferServiceIDLabel] = serviceID
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(uid),
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "worker",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						"hami.io/gpu":      *resource.NewQuantity(1, resource.BinarySI),
						"hami.io/gpumem":   *resource.NewQuantity(mem, resource.BinarySI),
						"hami.io/gpucores": *resource.NewQuantity(cores, resource.BinarySI),
					},
				},
			}},
		},
	}
}

func topoAlloc(deviceID string, mem, cores int64) device.PodDevices {
	return device.PodDevices{
		nvidia.NvidiaGPUDevice: device.PodSingleDevice{
			device.ContainerDevices{{Idx: 0, UUID: deviceID, Type: nvidia.NvidiaGPUDevice, Usedmem: mem, Usedcores: cores}},
		},
	}
}

// seedSuperpodCluster builds two single-GPU nodes in two different superpods.
// The busy node (sp2) already holds an unrelated workload, so binpack prefers
// it; the quiet node (sp1) holds the inference service's first pod, so only a
// topology-aware scheduler can choose it.
func seedSuperpodCluster(t *testing.T, serviceID string) *Scheduler {
	t.Helper()
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	client.KubeClient = fake.NewClientset()
	s.kubeClient = client.KubeClient
	s.podLister = informers.NewSharedInformerFactoryWithOptions(client.KubeClient, time.Hour).Core().V1().Pods().Lister()
	initTopoDevices(t)

	s.addNode("node-sp1", &device.NodeInfo{
		ID:      "node-sp1",
		Node:    topoNode("node-sp1", "1", "1", "1"),
		Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {topoGPUDevice("gpu-sp1")}},
	})
	s.addNode("node-sp2", &device.NodeInfo{
		ID:      "node-sp2",
		Node:    topoNode("node-sp2", "2", "5", "2"),
		Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {topoGPUDevice("gpu-sp2")}},
	})

	prefill := topoGPUPod("prefill-0", "prefill-0-uid", serviceID, 1000, 20)
	require.True(t, s.podManager.AddPod(prefill, "node-sp1", topoAlloc("gpu-sp1", 1000, 20)))
	unrelated := topoGPUPod("batch-0", "batch-0-uid", "", 6000, 70)
	require.True(t, s.podManager.AddPod(unrelated, "node-sp2", topoAlloc("gpu-sp2", 6000, 70)))
	return s
}

// The policy only takes effect where it is wired in, so these cases go
// through Filter rather than calling applySuperpodPolicy directly.
func TestFilterWiringConvergesInferenceServiceIntoOneSuperpod(t *testing.T) {
	tests := []struct {
		name      string
		serviceID string
		wantNode  string
	}{
		{"labeled pod leaves the packed node for its own superpod", "svc-a", "node-sp1"},
		{"unlabeled pod keeps the binpack winner", "", "node-sp2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := seedSuperpodCluster(t, tc.serviceID)
			pending := topoGPUPod("decode-0", "decode-0-uid", tc.serviceID, 500, 10)
			_, err := client.KubeClient.CoreV1().Pods(pending.Namespace).Create(context.Background(), pending, metav1.CreateOptions{})
			require.NoError(t, err)

			res, err := s.Filter(extenderv1.ExtenderArgs{Pod: pending, NodeNames: &[]string{"node-sp1", "node-sp2"}})
			require.NoError(t, err)
			require.NotNil(t, res.NodeNames)
			require.Equal(t, []string{tc.wantNode}, *res.NodeNames)
		})
	}
}

// seedSplitCluster puts the service's first pod on a node in superpod 1 that
// has no room left for another card, while an unrelated node in superpod 2
// still has plenty. Only a scheduler that ignores the superpod policy would
// find capacity for such a pod.
func seedSplitCluster(t *testing.T, serviceID string) *Scheduler {
	t.Helper()
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	client.KubeClient = fake.NewClientset()
	s.kubeClient = client.KubeClient
	s.podLister = informers.NewSharedInformerFactoryWithOptions(client.KubeClient, time.Hour).Core().V1().Pods().Lister()
	initTopoDevices(t)

	s.addNode("node-sp1", &device.NodeInfo{
		ID:      "node-sp1",
		Node:    topoNode("node-sp1", "1", "1", "1"),
		Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {topoGPUDevice("gpu-sp1")}},
	})
	s.addNode("node-sp2", &device.NodeInfo{
		ID:      "node-sp2",
		Node:    topoNode("node-sp2", "2", "5", "2"),
		Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {topoGPUDevice("gpu-sp2")}},
	})

	prefill := topoGPUPod("prefill-0", "prefill-0-uid", serviceID, 7500, 20)
	require.True(t, s.podManager.AddPod(prefill, "node-sp1", topoAlloc("gpu-sp1", 7500, 20)))
	batch := topoGPUPod("batch-0", "batch-0-uid", "", 1000, 10)
	require.True(t, s.podManager.AddPod(batch, "node-sp2", topoAlloc("gpu-sp2", 1000, 10)))
	return s
}

// D7: the domain wins or the pod waits. Leaving the candidate list empty is
// the point of a strict policy, so the caller must report it as no-fit instead
// of quietly accepting the foreign node that calcScore handed over.
func TestFilterWiringHoldsPodWhenItsSuperpodIsFull(t *testing.T) {
	s := seedSplitCluster(t, "svc-a")
	// 2000MB does not fit the 500MB left on node-sp1, so only node-sp2 can fit.
	pending := topoGPUPod("decode-0", "decode-0-uid", "svc-a", 2000, 10)
	_, err := client.KubeClient.CoreV1().Pods(pending.Namespace).Create(context.Background(), pending, metav1.CreateOptions{})
	require.NoError(t, err)

	res, err := s.Filter(extenderv1.ExtenderArgs{Pod: pending, NodeNames: &[]string{"node-sp1", "node-sp2"}})
	require.NoError(t, err)
	require.Nil(t, res.NodeNames, "a full domain must not be answered with a foreign node")
	require.Contains(t, res.FailedNodes, "node-sp2")
	require.Contains(t, res.FailedNodes["node-sp2"], "superpod")
}

// The switch is the operator's escape valve: with the policy off the same pod
// is placed on the node with room, which is what binpack would do alone.
func TestFilterWiringDisabledPolicyPlacesForeignNode(t *testing.T) {
	withSuperpodPolicy(t, util.SuperpodSchedulerPolicyDisable.String())
	s := seedSplitCluster(t, "svc-a")
	pending := topoGPUPod("decode-0", "decode-0-uid", "svc-a", 2000, 10)
	_, err := client.KubeClient.CoreV1().Pods(pending.Namespace).Create(context.Background(), pending, metav1.CreateOptions{})
	require.NoError(t, err)

	res, err := s.Filter(extenderv1.ExtenderArgs{Pod: pending, NodeNames: &[]string{"node-sp1", "node-sp2"}})
	require.NoError(t, err)
	require.NotNil(t, res.NodeNames)
	require.Equal(t, []string{"node-sp2"}, *res.NodeNames)
}

// Autoscaler simulation must agree with the real Filter, otherwise it scales
// capacity in a superpod the service will not land in. Simulation candidates
// all score zero, so the plain ordering is map-dependent and the assertion is
// repeated to catch the wiring only sometimes being effective.
// The simulation path sees the same domain rule, so a cluster autoscaler
// asking where this pod would go is told "nowhere in its superpod" rather than
// being pointed at a node in another one.
func TestFilterSimulationHoldsPodWhenOnlyForeignNodesFit(t *testing.T) {
	s := NewScheduler()
	t.Cleanup(func() { close(s.stopCh) })
	client.KubeClient = fake.NewClientset()
	s.kubeClient = client.KubeClient
	s.podLister = informers.NewSharedInformerFactoryWithOptions(client.KubeClient, time.Hour).Core().V1().Pods().Lister()
	initTopoDevices(t)

	occupied := topoNode("node-sp1", "1", "1", "1")
	s.addNode(occupied.Name, &device.NodeInfo{
		ID:      occupied.Name,
		Node:    occupied,
		Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {topoGPUDevice("gpu-sp1")}},
	})
	prefill := topoGPUPod("prefill-0", "prefill-0-uid", "svc-a", 1000, 20)
	require.True(t, s.podManager.AddPod(prefill, occupied.Name, topoAlloc("gpu-sp1", 1000, 20)))

	registerAnno := `[{"id":"GPU-new","count":10,"devmem":8000,"devcore":100,"type":"NVIDIA-A100","numa":0,"health":true,"index":0,"mode":"hami-core"}]`
	foreign := topoNode("scale-other-sp", "2", "5", "")
	foreign.Annotations[nvidia.RegisterAnnos] = registerAnno
	nodes := &corev1.NodeList{Items: []corev1.Node{*foreign}}
	pending := topoGPUPod("decode-0", "decode-0-uid", "svc-a", 500, 10)

	res, err := s.Filter(extenderv1.ExtenderArgs{Pod: pending, Nodes: nodes})
	require.NoError(t, err)
	require.True(t, res.Nodes == nil || len(res.Nodes.Items) == 0, "simulation must not offer a foreign superpod")
	require.Contains(t, res.FailedNodes, "scale-other-sp")
}

func TestFilterSimulationWiringMatchesRealFilter(t *testing.T) {
	s := NewScheduler()

	t.Cleanup(func() { close(s.stopCh) })
	client.KubeClient = fake.NewClientset()
	s.kubeClient = client.KubeClient
	s.podLister = informers.NewSharedInformerFactoryWithOptions(client.KubeClient, time.Hour).Core().V1().Pods().Lister()
	initTopoDevices(t)

	// The service already holds a card in superpod 1 / rack 1.
	occupied := topoNode("node-sp1", "1", "1", "1")
	s.addNode(occupied.Name, &device.NodeInfo{
		ID:      occupied.Name,
		Node:    occupied,
		Devices: map[string][]device.DeviceInfo{nvidia.NvidiaGPUDevice: {topoGPUDevice("gpu-sp1")}},
	})
	prefill := topoGPUPod("prefill-0", "prefill-0-uid", "svc-a", 1000, 20)
	require.True(t, s.podManager.AddPod(prefill, occupied.Name, topoAlloc("gpu-sp1", 1000, 20)))

	registerAnno := `[{"id":"GPU-new","count":10,"devmem":8000,"devcore":100,"type":"NVIDIA-A100","numa":0,"health":true,"index":0,"mode":"hami-core"}]`
	template := func(name, superpod, rack string) corev1.Node {
		node := topoNode(name, superpod, rack, "")
		node.Annotations[nvidia.RegisterAnnos] = registerAnno
		return *node
	}
	nodes := &corev1.NodeList{Items: []corev1.Node{
		template("scale-in-sp", "1", "1"),
		template("scale-other-sp", "2", "5"),
	}}
	pending := topoGPUPod("decode-0", "decode-0-uid", "svc-a", 500, 10)

	for range 20 {
		res, err := s.Filter(extenderv1.ExtenderArgs{Pod: pending, Nodes: nodes})
		require.NoError(t, err)
		require.NotNil(t, res.Nodes)
		require.Len(t, res.Nodes.Items, 1)
		require.Equal(t, "scale-in-sp", res.Nodes.Items[0].Name)
	}
}

// The extender has no /score endpoint, so a Pod event is the only place a
// topology decision reaches someone who is not reading scheduler logs at V(2).
// Both outcomes have to be explainable from `kubectl describe pod`: which
// domain a placement went into, and which domain a pending pod is waiting for.
func TestFilterEventsReportTheSuperpodDecision(t *testing.T) {
	t.Run("placement names the domain it landed in", func(t *testing.T) {
		s := seedSuperpodCluster(t, "svc-a")
		recorder := record.NewFakeRecorder(10)
		s.eventRecorder = recorder

		pending := topoGPUPod("decode-0", "decode-0-uid", "svc-a", 500, 10)
		_, err := client.KubeClient.CoreV1().Pods(pending.Namespace).Create(context.Background(), pending, metav1.CreateOptions{})
		require.NoError(t, err)

		_, err = s.Filter(extenderv1.ExtenderArgs{Pod: pending, NodeNames: &[]string{"node-sp1", "node-sp2"}})
		require.NoError(t, err)

		events := recordedEvents(recorder)
		require.Contains(t, events, EventReasonFilteringSucceed)
		for _, want := range []string{"node=node-sp1", "superpod=1", "svc-a", "gated out as foreign"} {
			require.Contains(t, events, want, "event %q must mention %q", events, want)
		}
	})

	t.Run("no fit names the domain the pod waits for", func(t *testing.T) {
		s := seedSplitCluster(t, "svc-a")
		recorder := record.NewFakeRecorder(10)
		s.eventRecorder = recorder

		pending := topoGPUPod("decode-0", "decode-0-uid", "svc-a", 2000, 10)
		_, err := client.KubeClient.CoreV1().Pods(pending.Namespace).Create(context.Background(), pending, metav1.CreateOptions{})
		require.NoError(t, err)

		_, err = s.Filter(extenderv1.ExtenderArgs{Pod: pending, NodeNames: &[]string{"node-sp1", "node-sp2"}})
		require.NoError(t, err)

		events := recordedEvents(recorder)
		require.Contains(t, events, EventReasonFilteringFailed)
		require.Contains(t, events, "converged into superpod(s) [1]",
			"event %q must name the domain the pod is held to", events)
	})
}

// recordedEvents joins everything the scheduler reported so far, so an
// assertion can say "some event mentions this" without depending on how many
// events one Filter round produces.
func recordedEvents(recorder *record.FakeRecorder) string {
	var out strings.Builder
	for {
		select {
		case event := <-recorder.Events:
			out.WriteString(event)
			out.WriteString("\n")
		default:
			return out.String()
		}
	}
}
