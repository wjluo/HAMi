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
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
	"github.com/Project-HAMi/HAMi/pkg/topology/plane"
)

// maxTopologySuspectsLogged bounds one audit report; the count tells the
// operator whether the fix is three typos or a fleet-wide labelling gap.
const maxTopologySuspectsLogged = 20

// loadDomainRegistry reads the table that turns a vendor leaf path into a short
// compute domain name. A missing ConfigMap is the normal case and yields no
// registry; a present but unreadable one is reported, because half a registry
// silently unmaps every node on the fabric it was meant to describe.
func (s *Scheduler) loadDomainRegistry() (*plane.Registry, error) {
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = metav1.NamespaceSystem
	}
	cm, err := s.kubeClient.CoreV1().ConfigMaps(namespace).Get(context.Background(),
		plane.DomainRegistryConfigMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	text, ok := cm.Data[plane.DomainRegistryConfigMapKey]
	if !ok {
		return nil, fmt.Errorf("configmap %s/%s has no %q key",
			namespace, plane.DomainRegistryConfigMapName, plane.DomainRegistryConfigMapKey)
	}
	reg, err := plane.ParseRegistry(text)
	if err != nil {
		return nil, fmt.Errorf("configmap %s/%s: %w", namespace, plane.DomainRegistryConfigMapName, err)
	}
	return reg, nil
}

// reportSuperpodTopology publishes what the fleet actually says about the three
// network planes, once per scheduler process. It runs before any placement
// depends on the answer, so the operator learns which nodes are labelled, which
// are genuinely off a plane, and which contradict themselves without having to
// schedule anything to find out. The report is read-only: it changes no
// candidate and no score.
func (s *Scheduler) reportSuperpodTopology(nodes []*corev1.Node) (plane.AuditSummary, bool) {
	if !s.topoAudited.CompareAndSwap(false, true) {
		return plane.AuditSummary{}, false
	}

	reg, err := s.loadDomainRegistry()
	if err != nil {
		klog.Warningf("topo-plane domain registry unavailable, vendor leaf paths stay unmapped: %v", err)
	}
	cfg := plane.Config{ServiceDefault: config.TopologyServicePlaneDomain}
	summary := plane.Audit(nodes, reg, cfg)

	for _, p := range plane.All() {
		klog.InfoS("topo-plane",
			"plane", string(p),
			"nodes", summary.Nodes,
			"labeled", summary.ByState(p, plane.Labeled),
			"absent", summary.ByState(p, plane.Absent),
			"unlabeled", summary.ByState(p, plane.Unlabeled),
			"conflict", summary.ByState(p, plane.Conflict))
	}
	// Without a registry nobody promised to map the vendor paths, so an unmapped
	// path is not a finding yet.
	logged := 0
	for _, suspect := range summary.Suspects {
		if reg == nil && suspect.Reason == plane.ReasonPathUnmapped {
			continue
		}
		if logged == maxTopologySuspectsLogged {
			klog.Warningf("topo-plane audit suppressed the remaining %d suspect findings, see the plane counts above",
				len(summary.Suspects)-logged)
			break
		}
		klog.Warningf("topo-plane %s on node %s: %s (%s)",
			suspect.Plane, suspect.Node, suspect.State, suspect.Reason)
		logged++
	}
	return summary, true
}
