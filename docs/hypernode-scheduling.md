# HyperNode (SuperPoD) Aware Scheduling

HAMi can make GPU placement aware of **hypernodes** (also called SuperPoDs,
e.g. Huawei Atlas 900 A3 SuperPoD / CloudMatrix pods): a group of nodes that
are fully interconnected inside one high-bandwidth, low-latency performance
domain, while cross-hypernode communication is significantly slower. In a
cluster with multiple hypernodes, a distributed job should keep its pods
inside as few hypernodes as possible.

The design mirrors Volcano's network-topology-aware scheduling (HyperNode CRD)
but follows HAMi's scheduler-extender architecture: topology is expressed with
**node labels** and consumed at pod scheduling time, no CRD or extra controller
is required.

## 1. Labelling nodes

Annotate every node with the hypernode it belongs to:

```yaml
# kubectl label node <node> hami.io/hypernode=supernode-a
```

The Volcano-compatible label `volcano.sh/hypernode` is honoured as a fallback,
so clusters already labelled for Volcano's network-topology-aware scheduling
work without relabelling. If both labels are present, `hami.io/hypernode`
wins. Nodes without a label belong to no hypernode.

## 2. Pod annotations

Both annotations take effect only for pods that **request HAMi-managed
resources** (for example `nvidia.com/gpu` or `huawei.com/Ascend910`): a pod
without device requests never reaches HAMi's filter, and the scheduler logs a
warning when such a pod carries the annotations. Pin device-less pods with a
native nodeSelector on the same label instead:

```yaml
spec:
  nodeSelector:
    hami.io/hypernode: supernode-a
```

### 2.1 Hard affinity: `hami.io/hypernode-affinity`

Pins a pod to one hypernode. Candidate nodes outside the pinned hypernode are
rejected during filtering with a `HyperNodeNotFit` reason (returned in the
extender's failedNodes and, when no node fits, named in the pod's scheduling
event); if no node of that hypernode can host the pod, the pod stays Pending —
the same all-or-nothing semantics as Volcano's `networkTopology.mode: hard`.
The filter applies on both the real scheduling path and the simulation path
(for example the cluster autoscaler), so simulated placements respect the
pinned performance domain too.

```yaml
metadata:
  annotations:
    hami.io/hypernode-affinity: supernode-a
```

### 2.2 Soft packing: `hami.io/hypernode-group`

Packs the pods of one distributed job into the same hypernode when capacity
allows. Give every pod of the job the same group value (for example the job
name):

```yaml
metadata:
  annotations:
    hami.io/hypernode-group: my-llm-job
```

The scheduler collects the hypernodes already occupied by scheduled pods of
the same group and re-weights node scores so those hypernodes win over
otherwise-equal candidates. The first pod of the group is placed freely
(first-wins); later pods prefer the hypernode that already hosts the group,
similar to Volcano's LCA-based `batchNodeOrder`. When no candidate node lies
inside the occupied hypernodes (or every candidate does), scores are left
untouched and scheduling degrades to the plain node policy, so the pod never
starves because of the group preference.

The packing penalty dominates the binpack/spread device placement preference
but preserves the relative order of nodes within the same affinity class, and
it composes with `hami.io/node-scheduler-policy` (binpack/spread) and the
per-pod GPU policies.

## 3. Interaction with Volcano

HAMi reads the same node labels Volcano's label discoverer uses
(`volcano.sh/hypernode`, part of its Ascend A3 topology template), so both
schedulers can run against one labelled cluster: Volcano orchestrates gang /
multi-pod placement with HyperNode CRs, HAMi handles GPU virtualization and
device selection with the same topology awareness.

## 4. Scope and limitations

- One pod's devices always stay on one node (HAMi's extender model); the
  hypernode is a node-group level constraint on top of that.
- The hard affinity pins to a hypernode name; wildcard or tiered constraints
  (`highestTierAllowed`) are not modelled. For tiered, gang-scheduled,
  partitioned placement use Volcano's HyperNode feature.
- Topology must be maintained via labels (no auto-discovery in HAMi).
