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

package routes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	"gotest.tools/v3/assert"

	"github.com/Project-HAMi/HAMi/pkg/scheduler"
	"github.com/Project-HAMi/HAMi/pkg/scheduler/config"
)

func TestMaxRequestSize(t *testing.T) {
	hugePayload := strings.Repeat(" ", maxRequestSize+100)

	req := httptest.NewRequest("POST", "/predicate", strings.NewReader(hugePayload))
	w := httptest.NewRecorder()

	s := &scheduler.Scheduler{}
	handler := PredicateRoute(s)
	handler(w, req, nil)
	respBody := w.Body.String()

	if !strings.Contains(respBody, "EOF") && !strings.Contains(respBody, "unexpected EOF") {
		t.Errorf("LimitReader failed to trigger EOF. Response body: %s", respBody)
	} else {
		t.Logf("Success! Caught expected error: %s", respBody)
	}
}

func TestHealthzRoute(t *testing.T) {
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handler := HealthzRoute()
	handler(w, req, nil)

	if w.Code != 200 {
		t.Errorf("Expected status 200 for health check, got %d", w.Code)
	}
}

func TestMaxRequestSizeBind(t *testing.T) {
	hugePayload := strings.Repeat(" ", maxRequestSize+100)
	req := httptest.NewRequest("POST", "/bind", strings.NewReader(hugePayload))
	w := httptest.NewRecorder()
	s := &scheduler.Scheduler{}
	handler := Bind(s)

	handler(w, req, nil)

	respBody := w.Body.String()
	if !strings.Contains(respBody, "EOF") && !strings.Contains(respBody, "unexpected EOF") {
		t.Errorf("LimitReader failed in Bind. Response: %s", respBody)
	}
}

func TestWebHookRoute(t *testing.T) {
	handler := WebHookRoute()
	if handler == nil {
		t.Fatal("WebHookRoute returned nil handler")
	}

	req := httptest.NewRequest("POST", "/webhook", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler(w, req, nil)

	if w.Code == 0 {
		t.Error("Expected a non-zero status code from webhook handler")
	}
}

// readyzLease builds the lease that says "this pod holds the lock". The holder
// identity is hostname + "_" + uuid, exactly as kube-scheduler writes it, and
// the leader manager matches itself with a prefix test.
func readyzLease(hostname, namespace, name string) *coordinationv1.Lease {
	holder := hostname + "_09b916af-2a24-4318-8ce3-ad3309992531"
	now := metav1.NewMicroTime(time.Now())
	duration := int32(15)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			AcquireTime:          &now,
			LeaseDurationSeconds: &duration,
		},
	}
}

// withRealLeaderElect points the manager at a lease of our own making, so the
// test decides leadership by writing the lease instead of by hoping the default
// (leader-election disabled) stays convenient.
func withRealLeaderElect(t *testing.T) {
	t.Helper()
	origLeaderElect, origHostname := config.LeaderElect, config.HostName
	origName, origNamespace := config.LeaderElectResourceName, config.LeaderElectResourceNamespace
	config.LeaderElect = true
	config.HostName = "readyz-host"
	config.LeaderElectResourceName = "hami-scheduler"
	config.LeaderElectResourceNamespace = "kube-system"
	t.Cleanup(func() {
		config.LeaderElect, config.HostName = origLeaderElect, origHostname
		config.LeaderElectResourceName, config.LeaderElectResourceNamespace = origName, origNamespace
	})
}

func TestReadyzRouteElectDisabledIsAlwaysReady(t *testing.T) {
	// A deployment that never asked for leader election has no lease to wait for.
	// Failing readiness there would pull the only replica out of the extender
	// Service endpoints and stop the cluster's GPU scheduling entirely.
	origLeaderElect := config.LeaderElect
	config.LeaderElect = false
	t.Cleanup(func() { config.LeaderElect = origLeaderElect })

	s := scheduler.NewScheduler()
	handler := ReadyzRoute(s)

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	handler(w, req, nil)

	assert.Equal(t, w.Code, http.StatusOK)
}

func TestReadyzRouteLeaderIsReady(t *testing.T) {
	withRealLeaderElect(t)
	s := scheduler.NewScheduler()
	s.GetLeaderManager().OnAdd(readyzLease(config.HostName, config.LeaderElectResourceNamespace, config.LeaderElectResourceName), true)
	assert.Equal(t, true, s.GetLeaderManager().IsLeader(), "fixture must produce a leader")

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	ReadyzRoute(s)(w, req, nil)

	assert.Equal(t, w.Code, http.StatusOK)
	assert.Equal(t, "", w.Body.String())
}

func TestReadyzRouteNotLeaderIsNotReady(t *testing.T) {
	// The standby answers /filter from a cache that registration fills only for
	// the leader (see Scheduler.register), so a 200 here advertises a replica
	// that is one Service round-robin away from reporting a healthy cluster as
	// empty. Readiness has to say what it actually means.
	withRealLeaderElect(t)
	s := scheduler.NewScheduler()
	assert.Equal(t, false, s.GetLeaderManager().IsLeader(), "no lease observed => standby")

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	ReadyzRoute(s)(w, req, nil)

	assert.Equal(t, w.Code, http.StatusServiceUnavailable)
	assert.Assert(t, strings.Contains(strings.ToLower(w.Body.String()), "leader"),
		"the response has to name the cause, got %q", w.Body.String())
}

func TestReadyzRouteRegainsReadinessWhenElected(t *testing.T) {
	// Rollouts hand the lease over: the new pod starts as standby and must become
	// ready by itself, otherwise this fix turns every restart into a manual job.
	withRealLeaderElect(t)
	s := scheduler.NewScheduler()
	handler := ReadyzRoute(s)

	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "/readyz", nil), nil)
	assert.Equal(t, w.Code, http.StatusServiceUnavailable)

	s.GetLeaderManager().OnAdd(readyzLease(config.HostName, config.LeaderElectResourceNamespace, config.LeaderElectResourceName), true)
	w = httptest.NewRecorder()
	handler(w, httptest.NewRequest("GET", "/readyz", nil), nil)
	assert.Equal(t, w.Code, http.StatusOK)
}

func TestCheckBodyNil(t *testing.T) {
	req := httptest.NewRequest("POST", "/test", nil)
	req.Body = nil
	w := httptest.NewRecorder()

	if checkBody(w, req) {
		t.Error("expected checkBody to reject a nil body")
	}

	if w.Code != 400 {
		t.Errorf("Expected status 400 for nil body, got %d", w.Code)
	}
}

func TestCheckBody_ValidBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/test", strings.NewReader("some-body"))
	w := httptest.NewRecorder()

	valid := checkBody(w, req)

	if !valid {
		t.Error("Expected checkBody to return true for valid body, got false")
	}
	if w.Code != 200 {
		t.Errorf("Expected status 200 (default) for valid body, got %d", w.Code)
	}
}

func TestPredicateRoute_NilBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/predicate", nil)
	req.Body = nil
	w := httptest.NewRecorder()

	s := &scheduler.Scheduler{}
	handler := PredicateRoute(s)
	handler(w, req, nil)

	if w.Code != 400 {
		t.Errorf("expected 400 for nil body, got %d", w.Code)
	}
}

func TestBind_NilBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/bind", nil)
	req.Body = nil
	w := httptest.NewRecorder()

	s := &scheduler.Scheduler{}
	handler := Bind(s)

	// This should not panic
	handler(w, req, nil)

	if w.Code != 400 {
		t.Errorf("Expected status 400 for nil body in Bind, got %d", w.Code)
	}
}

func TestPredicateRoute_DecodeError(t *testing.T) {
	req := httptest.NewRequest("POST", "/predicate", strings.NewReader("{not-json"))
	w := httptest.NewRecorder()

	s := &scheduler.Scheduler{}
	handler := PredicateRoute(s)
	handler(w, req, nil)

	if w.Code != 200 {
		t.Errorf("expected 200 (error reported in body, not status), got %d", w.Code)
	}

	var result extenderv1.ExtenderFilterResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if result.Error == "" {
		t.Error("expected a decode error to be reported in the filter result")
	}
}

func TestPredicateRoute_NilPod(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "pod field omitted", body: `{"NodeNames":["node1"]}`},
		{name: "pod field explicitly null", body: `{"Pod":null,"NodeNames":["node1"]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/predicate", strings.NewReader(tt.body))
			w := httptest.NewRecorder()

			s := &scheduler.Scheduler{}
			handler := PredicateRoute(s)

			// Must not panic when Pod is missing/null.
			handler(w, req, nil)

			if w.Code != 200 {
				t.Errorf("expected 200 (error reported in body, not status), got %d", w.Code)
			}

			var result extenderv1.ExtenderFilterResult
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatalf("failed to unmarshal response: %v", err)
			}
			if result.Error == "" {
				t.Error("expected a missing-pod error to be reported in the filter result")
			}
		})
	}
}

func TestPredicateRoute_CacheNotSynced(t *testing.T) {
	args := extenderv1.ExtenderArgs{Pod: &corev1.Pod{}}
	body, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("failed to marshal args: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so WaitForCacheSync fails fast instead of polling forever

	req := httptest.NewRequest("POST", "/predicate", bytes.NewReader(body)).WithContext(ctx)
	w := httptest.NewRecorder()

	s := &scheduler.Scheduler{} // zero value: synced defaults to false
	handler := PredicateRoute(s)
	handler(w, req, nil)

	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var result extenderv1.ExtenderFilterResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if !strings.Contains(result.Error, "context cancelled") {
		t.Errorf("expected 'context cancelled' error, got %q", result.Error)
	}
}

func TestBind_DecodeError(t *testing.T) {
	req := httptest.NewRequest("POST", "/bind", strings.NewReader("{not-json"))
	w := httptest.NewRecorder()

	s := &scheduler.Scheduler{}
	handler := Bind(s)
	handler(w, req, nil)

	if w.Code != 200 {
		t.Errorf("expected 200 (error reported in body, not status), got %d", w.Code)
	}

	var result extenderv1.ExtenderBindingResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if result.Error == "" {
		t.Error("expected a decode error to be reported in the bind result")
	}
}

type errorResponseWriter struct {
	http.ResponseWriter
}

func (w *errorResponseWriter) Write(b []byte) (int, error) {
	return 0, errors.New("mock write error")
}
func TestPredicateRoute_WriteError(t *testing.T) {
	var buf bytes.Buffer
	klog.SetOutput(&buf)
	klog.LogToStderr(false)
	defer func() {
		klog.SetOutput(os.Stderr)
		klog.LogToStderr(true)
	}()
	req := httptest.NewRequest("POST", "/predicate", strings.NewReader("{not-json"))
	w := httptest.NewRecorder()
	ew := &errorResponseWriter{ResponseWriter: w}
	s := &scheduler.Scheduler{}
	handler := PredicateRoute(s)
	handler(ew, req, nil)
	klog.Flush()
	if !strings.Contains(buf.String(), "Failed to write response") {
		t.Errorf("Expected 'Failed to write response' in log output, but got: %s", buf.String())
	}
}
func TestBind_WriteError(t *testing.T) {
	var buf bytes.Buffer
	klog.SetOutput(&buf)
	klog.LogToStderr(false)
	defer func() {
		klog.SetOutput(os.Stderr)
		klog.LogToStderr(true)
	}()
	req := httptest.NewRequest("POST", "/bind", strings.NewReader("{not-json"))
	w := httptest.NewRecorder()
	ew := &errorResponseWriter{ResponseWriter: w}
	s := &scheduler.Scheduler{}
	handler := Bind(s)
	handler(ew, req, nil)
	klog.Flush()
	if !strings.Contains(buf.String(), "Failed to write response") {
		t.Errorf("Expected 'Failed to write response' in log output, but got: %s", buf.String())
	}
}
