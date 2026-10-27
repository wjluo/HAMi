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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"gotest.tools/v3/assert"

	"github.com/Project-HAMi/HAMi/pkg/topology/plane"
	"github.com/Project-HAMi/HAMi/pkg/util"
)

func cmWithRegistry(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      plane.DomainRegistryConfigMapName,
			Namespace: "kube-system",
		},
		Data: data,
	}
}

func TestLoadDomainRegistryAbsentMeansNoVendorMapping(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "kube-system")
	s := &Scheduler{kubeClient: fake.NewSimpleClientset()}

	reg, err := s.loadDomainRegistry()
	assert.NilError(t, err, "a cluster with no registry is the normal case, not a failure")
	assert.Assert(t, reg == nil)
}

func TestLoadDomainRegistryFromConfigMap(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "kube-system")
	s := &Scheduler{kubeClient: fake.NewSimpleClientset(cmWithRegistry(map[string]string{
		plane.DomainRegistryConfigMapKey: "plane: compute\ndomains:\n  - name: PF-SZ01\n    match: DC1_FB01-LF01\n",
	}))}

	reg, err := s.loadDomainRegistry()
	assert.NilError(t, err)
	name, ok := reg.DomainFor("leaf-SITE01R1D10U39-DCN-DC1_FB01-LF01_A")
	assert.Assert(t, ok)
	assert.Equal(t, name, "PF-SZ01")
}

func TestLoadDomainRegistryReportsBadFiles(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "kube-system")
	s := &Scheduler{kubeClient: fake.NewSimpleClientset(cmWithRegistry(map[string]string{
		plane.DomainRegistryConfigMapKey: "plane: compute\ndomains:\n  - name: PF-SZ01\n",
	}))}

	_, err := s.loadDomainRegistry()
	assert.Assert(t, err != nil, "an incomplete rule must not degrade silently to an unmapped fleet")
}

func TestReportSuperpodTopologyIsAOneShotAudit(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "kube-system")
	s := &Scheduler{kubeClient: fake.NewSimpleClientset()}
	nodes := []*corev1.Node{
		planeNode(map[string]string{"hami.io/topo.compute.domain": "SO-SET1", "hami.io/topo.compute.leaf": "LF49_A"}),
		planeNode(map[string]string{"hami.io/topo.compute.domain": "none"}),
	}

	sum, reported := s.reportSuperpodTopology(nodes)
	assert.Assert(t, reported, "the first register pass has to publish what the fleet actually looks like")
	assert.Equal(t, sum.Nodes, 2)
	assert.Equal(t, sum.ByState(plane.Compute, plane.Labeled), 1)
	assert.Equal(t, sum.ByState(plane.Compute, plane.Absent), 1)

	_, again := s.reportSuperpodTopology(nodes)
	assert.Assert(t, !again, "a 15 second register loop must not re-log the same audit every pass")
}

// The audit is the instrument an operator uses to decide whether it is safe to
// turn the policy on, so the kill switch must not silence it. Coupling them
// would mean the only way to check a cluster's labelling is to start gating on
// it.
func TestTopologyAuditRunsWhileThePolicyIsDisabled(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "kube-system")
	withSuperpodPolicy(t, util.SuperpodSchedulerPolicyDisable.String())

	s := &Scheduler{kubeClient: fake.NewSimpleClientset()}
	_, reported := s.reportSuperpodTopology([]*corev1.Node{
		planeNode(map[string]string{"hami.io/topo.compute.domain": "SO-SET1", "hami.io/topo.compute.leaf": "LF49_A"}),
	})

	assert.Assert(t, reported)
}

func planeNode(labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: labels}}
}
