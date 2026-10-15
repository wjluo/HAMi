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

Whether step 2 exists is inferred from the candidates themselves (any node with
a usable rack id means the cluster has a rack tier), so the same code serves a
two-tier A3 cluster and a three-tier A5 one, and a vendor that later adds a
level needs no scheduler change.

## The superpod policy

`--superpod-scheduler-policy` is a scheduling policy in its own right, set
beside the node and device policies (`scheduler.defaultSchedulerPolicy.superpodSchedulerPolicy`
in the chart) and on by default.

| Value | Behaviour |
| --- | --- |
| `enable` | Candidates in a superpod the workload does not belong to are taken out of the candidate list. If that leaves nothing, the Pod stays pending - it is not placed in a foreign superpod. |
| `disable` | No topology handling at all, and the ranking above goes with it: the binpack/spread scores alone pick the node. |

An unrecognised value is reported and read as `enable`, so a typo cannot
silently drop convergence.

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
- **Nothing placed yet means nothing to converge into.** The first pod of a
  service, or the first workload on an empty cluster, sees the full candidate
  list.

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
feature. The one thing to expect is that a node's annotations arrive with the
device plugin's registration, so inside that window the node is exempt rather
than homed, and a pod may land in another superpod. Re-convergence is not
attempted; the policy decides placement, not migration.

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
