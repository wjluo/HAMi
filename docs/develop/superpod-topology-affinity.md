# Superpod-aware scheduling

Inference workloads such as PD-disaggregated serving want the pods of one
service on the same superpod (and, where the hardware has one, the same rack),
because cross-superpod traffic pays a RoCE hop that intra-rack traffic does not.
HAMi reaches that outcome without any chip-specific code in the scheduler: the
device plugin publishes the node's topology as annotations, the user labels the
pods, and the superpod policy narrows and re-orders the scheduler's
already-scored candidates by topology.

## Contract

Node annotations, written by whichever component owns the machine:

| Key | Meaning | Required |
| --- | --- | --- |
| `hami.io/superpodid` | Top-level convergence domain | Yes on superpod hardware |
| `hami.io/rackid` | Middle domain inside one superpod | Only if the chip has that level |
| `hami.io/serverid` | Server index inside the superpod | No, observability |

Pod label:

| Key | Meaning |
| --- | --- |
| `hami.io/inferserviceid` | One value for every pod of one service, prefill and decode included. `inferserviceid` (no prefix) is also read, for jobs labelled the way Huawei's MindCluster labels them. |

Values are plain decimal integers (`"3"`, not `"supernode-3"`). Anything the
driver could not resolve - a missing key, a blank value, a negative number, the
driver's `0xffffffff` sentinel mapped to `-1` - is treated as "this level does
not exist", never as an identity.

For Ascend, `ascend-device-plugin` writes the three keys during its regular
30s node registration, and gates `rackid` on the card type (only 910A5 exposes
a rack). Any other vendor, or a cluster where no component reports topology,
can set them by hand:

```bash
kubectl annotate node npu-11 hami.io/superpodid=1 hami.io/rackid=2 hami.io/serverid=3 --overwrite
```

## How the scheduler uses it

`pkg/scheduler/topotree` ranks each candidate after the normal scoring:

1. same superpod **and** same rack as the service's already-placed pods;
2. same superpod, different rack;
3. a superpod the service does not use yet;
4. a node with no topology annotation.

Whether step 2 exists is decided from the domains the workload already occupies:
once a pod of the service sits on a node with a usable `hami.io/rackid`, the
service converges at rack granularity. Reading it from the candidate set instead
would let one round whose candidates happen to carry no rack id downgrade a
three-tier cluster, so the same code serves a two-tier A3 cluster and a
three-tier A5 one, and a vendor that later adds a level needs no scheduler
change.

## The superpod policy

`--superpod-scheduler-policy` is a scheduling policy in its own right, set
beside the node and device policies (`scheduler.defaultSchedulerPolicy.superpodSchedulerPolicy`
in the chart) and on by default.

| Value | Behaviour |
| --- | --- |
| `enable` | Candidates in a superpod the workload does not belong to are taken out of the candidate list. If that leaves nothing, the Pod stays pending - it is not placed in a foreign superpod. |
| `spill` | The same strict hold while any candidate in the workload's own superpod fits the Pod, but when the occupied domain has no fitting candidate this round the Pod is placed in a foreign superpod instead of being left pending. The Pod event and the decision log name the domain it spilled out of, so the topology-for-delivery trade is visible without reading scheduler internals. It is the capacity escape for a converged service whose domain has filled: this layer cannot see a replica count to pre-size against, but it can see that no candidate in the domain fits the Pod in front of it. |
| `disable` | No topology handling at all, and the ranking above goes with it: the binpack/spread scores alone pick the node. |

An unrecognised value is reported and read as `enable`, so a typo cannot
silently drop convergence.

The same values can be set on one Pod with the
`hami.io/superpod-scheduler-policy` annotation, which overrides the cluster
default for that workload in any direction. It exists because the cluster
default is strict: one wrong node annotation can leave every Pod of a service
pending, and without the annotation the only remedy is to turn convergence off
for the whole cluster. An unrecognised annotation value is reported and the
configured policy is used, so a typo cannot silently free a workload from its
domain.

```yaml
metadata:
  annotations:
    hami.io/superpod-scheduler-policy: disable
```

Three rules decide who is gated, and each exists because the alternative was a
cluster that cannot deliver compute:

- **Only a pod that names an inference service is gated.** The label states
  which pods must share a domain; a pod that states nothing is *ordered*
  towards the superpods already holding devices, which keeps it from opening a
  new domain without ever refusing a node.
- **A node without a topology annotation is never a foreign node.** It means
  the plugin did not report where the node is, not that the node is elsewhere.
  Gating those would lock out a mixed vendor cluster, and would punish a
  device-plugin rollout that has not reached every node yet.
- **The first pod of a labelled service opens by deterministic intent.** With
  nothing bound yet, two pods of one service evaluated in the same round would
  each keep their own best node and open the service in two superpods at once.
  So before any sibling exists, the pod is anchored to one superpod chosen as
  `crc32(serviceID)` over the sorted candidate superpods, and the gate holds it
  there exactly as it would for a later pod. The target is drawn from the
  candidates that fit this round, so the intent never strands the first pod on
  an empty domain. Once any sibling binds, the real occupancy takes over and the
  intent is inert. An unlabelled pod, which states no service to converge, still
  sees the full candidate list and is only *ordered* towards occupied domains.

The rack level narrows ordering but never the candidate set: refusing a node
for sitting in another rack of the same superpod would turn a bandwidth
preference into a capacity wall.

Both entry points get the policy - `Filter` for real scheduling and
`filterSimulation` for autoscaler dry runs - so a scale-up is requested in the
superpod the service will actually land in, and a dry run cannot offer a node
the live path would refuse. Gated nodes are returned as failed nodes with the
domain they were refused for, so `kubectl describe pod` names the superpod to
go and add capacity to. Within the surviving candidates the existing
binpack/spread scores still decide, so a topology preference outranks the
packing policy rather than blending into it.

A cluster whose nodes carry no annotations is untouched: nothing is gated,
nothing is reordered, and scheduling is exactly what it was before this
feature.

The scheduler reads topology from its node informer rather than from the copy of
the Node inside its device cache, because that cache only refreshes when the
device list changes. Without this, a plugin that writes `hami.io/superpodid`
after registering its devices - which is what every plugin does - would leave the
node permanently exempt from a policy that should have homed it.

Topology is only as current as the node's annotations, so a server moved to
another rack or out of the superpod has to have its old values removed or the
scheduler keeps converging workloads into a domain it no longer belongs to.
`ascend-device-plugin` registration merges, and deletes nothing unless
`--topology_prune_stale` is added to its `daemonSet.args`. That is the safe
default: a hand-set `hami.io/superpodid` is indistinguishable from a
plugin-written one, and the plugin must not silently take an operator's values
away. Re-convergence is not attempted either way; the policy decides placement,
not migration.

## Onboarding a cluster that is not Ascend

No code is vendor-specific: the scheduler only reads the three annotation keys,
so any chip - or a lab with no chip at all - can be brought under the policy in
one command per node, and the same verification afterwards:

```bash
kubectl annotate node <node> hami.io/superpodid=1 hami.io/rackid=2 hami.io/serverid=3 --overwrite
kubectl get nodes -o custom-columns=NAME:.metadata.name,SUPERPOD:.metadata.annotations.hami\.io/superpodid,RACK:.metadata.annotations.hami\.io/rackid
```

Order matters only in that the annotations should exist before the first pod of
a service is placed: with nothing annotated and nothing occupied the policy is
inert, so the first pod can open a domain anywhere.

A change to an existing node's annotations takes effect on the next Filter
round - the scheduler reads them from its node informer, not from the copy of
the Node in its device cache. It does not move anything that is already placed:
convergence decides placement, never migration, so a service whose first pod is
on a node that later moves to another superpod stays where it is until that pod
is rescheduled.

## Labelling one service

`hami.io/inferserviceid` is not generated by anything in HAMi; the workload
owner (or the platform's admission layer) sets it, and only pods carrying the
same value are treated as one service. For PD-disaggregated serving that means
the prefill pods and the decode pods share the value - they are the ones that
have to be near each other. A pod with no label is never gated, so mislabelling
costs a service nothing but its convergence.

`inferserviceid` without the `hami.io/` prefix is read as well, for jobs
labelled the way Huawei's MindCluster labels them; new setups should use the
prefixed key, because the unprefixed one is not namespaced and collides with
whatever else a chart writes into a pod's labels.

## What strict convergence costs

The gate can leave a pod pending that would otherwise fit, so a superpod domain
has to be able to hold the whole service. The rule to check before turning the
policy on for a workload:

> the cards one service needs at once, summed over its prefill and decode pods,
> must be deliverable inside one superpod - not across the cluster.

Capacity in the domain the workload already occupies is what it gets.

| Situation | Outcome |
| --- | --- |
| Domain has room | All pods of the service land in it, at the closest rack available. |
| Domain full, another superpod has room | The pod stays pending, and its event names the domain it is waiting for. Free it with the Pod annotation, or add capacity in the domain. |
| No pod of the service placed yet | Full candidate list; wherever the scores pick becomes the service's domain. |
| Nodes unannotated | The policy changes nothing at all. |

## What the requirement asked for, and what shipped

The superpod requirement asks for topology *affinity* - keep the pods of one
service together so their traffic stays inside the rack. It does not ask for the
scheduler to refuse a node, and refusing is a different promise: it turns a
bandwidth preference into a scheduling constraint that can hold a service
pending when the cluster has capacity elsewhere.

The default that shipped is the strict one, because a preference the scheduler
can quietly ignore is not something an operator can rely on. The escape valves
are what makes the strict default safe to ship: `disable` for the whole cluster,
`hami.io/superpod-scheduler-policy: disable` on the pods that must not wait for
their domain, and `hami.io/superpod-scheduler-policy: spill` for a workload that
wants to stay converged while its domain has room but would rather be delivered
than pending once it does not. If the product owner would rather have
affinity-only semantics for every workload, that is a one-line change to the
default in the chart - and the rack level is already preference-only, never a
gate, for exactly this reason.

## What the decision looks like from outside

A scheduler extender has no `/score` endpoint: HAMi is asked "which of these
nodes fit", so the tier ranking never appears in kube-scheduler's own output and
a placement made against the packing score looks, from the outside, like a
packing decision. Both the reason and the exception are therefore written where
a user will actually look - the Pod event and the scheduler's info log:

```
Normal   FilteringSucceed  find fit node(node-sp1), 1 nodes not fit, 1 nodes fit(node-sp1:4.25);
  superpod placement: node=node-sp1 tier=same-rack superpod=1 rack=1 service=svc-a
  converged into superpod(s) [1], 1 node(s) gated out as foreign,
  topology outranked the binpack score order (score preferred node-sp2)

Warning  FilteringFailed   no available node in the required superpod domain, 2 nodes do not meet;
  superpod policy left no candidate: service=svc-a converged into superpod(s) [1],
  1 node(s) gated out as foreign
```

The first pod of a service has no domain to converge into yet, and says so
rather than naming an empty set:

```
Normal   FilteringSucceed  find fit node(node-sp1), 0 nodes not fit, 1 nodes fit(node-sp1:4.25);
  superpod placement: node=node-sp1 tier=other-superpod superpod=1
  service=svc-a has no placement in any superpod yet
```

`kubectl describe pod` shows both, and the pending one names the domain to add
capacity to. `tier=` says how close the winner sits to the workload's domain,
and `topology outranked the ... score order` is the sentence that answers "why
did my spread pod land on the busier node". The full candidate ordering,
including the nodes that lost, is one level deeper:

```bash
kubectl -n kube-system logs deploy/hami-scheduler --tail=200 | grep 'Applied superpod scheduling policy'
```

A node whose annotations contradict its peers - the same `hami.io/serverid`
reported under two different `hami.io/superpodid` values, which is what a
hand-edited or stale annotation produces - is reported rather than guessed at:
the pair is named in the event of every pod scheduled over it, and once in the
scheduler's warnings. Nothing is gated on it, because the scheduler cannot tell
which of the two is right.

## Who is trusted to write these values

Topology input is trusted, not validated, so the permissions that can write it
are the security boundary of this feature:

- **`hami.io/superpodid`, `hami.io/rackid`, `hami.io/serverid` on a Node** are
  cluster-scoped and decide where other people's compute lands. Anyone who can
  patch nodes can move any workload into - or gate it out of - any superpod.
  The scheduler itself only reads them; it holds `nodes: patch` for the per-node
  device locks it takes while binding, and the admission webhook uses node write
  access as the test for "this caller is a HAMi component". Treat `nodes: patch`
  accordingly: grant it to the device plugin service accounts and to the
  scheduler, and never to a workload role.
- **`hami.io/inferserviceid` on a Pod** is set by whoever writes the workload.
  It does not buy devices - the quota manager and the extender still decide how
  much a pod gets - but it does choose which superpod the pod insists on, and
  which other pods it converges with. It is also the seed for the first pod's
  deterministic intent anchor, so the label value itself picks which candidate
  superpod the service opens in (via `crc32(serviceID)` over the sorted
  candidate superpods). Where co-location inside a domain is sensitive,
  validate the label's value per namespace (a validating webhook or an
  admission policy) instead of relying on tenants to label honestly - a caller
  who can enumerate service IDs can also steer a workload toward a specific
  superpod.
- **`hami.io/superpod-scheduler-policy` on a Pod** lets a workload opt out of
  convergence for itself. That is deliberate: a policy that can pin every pod of
  a service must also have a per-workload release valve, and freeing one's own
  pod cannot take capacity away from anyone else.

The device plugin side of the boundary is `--topology_prune_stale`, off by
default: leaving it off means a value nobody wrote any more can survive on the
Node object, and the annotation is then a statement about the cluster's
configuration, not about the hardware.

## Tests

```bash
go test ./pkg/util/ ./pkg/scheduler/...              # parsing, ranking, wiring
cd ../ascend-device-plugin && go test ./internal/topology/ ./internal/server/
```

`pkg/scheduler/topology_wiring_test.go` drives the real `Filter` so the
injection point itself is covered. The two key-pinning tests
(`TestTopologyKeysAreTheCrossRepoContract` and
`TestAnnotationKeysMatchTheHamiContract`) hold the same table in each repo:
they do not detect drift on their own, but they make renaming a key a
deliberate edit in two places instead of a silent one.
