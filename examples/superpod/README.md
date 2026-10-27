# 超节点 / 多平面拓扑调度 —— 用户使用案例

三张网络平面（与运维/业务口径一致）：

| 平面 | 中文 | 承载 | 谁打标 |
|---|---|---|---|
| `scaleup` | 超节点内部互联 | 超节点内 UB/LQC 总线，卡间紧耦合 | ADP 探测自动写 |
| `compute` | 计算面网络 | spine+leaf RDMA（A3 参数面 / A5 scale-out） | 运维手工 label |
| `service` | 业务面网络 | 25GE 南北向业务请求 | 全网互联，无需打标 |

## 1. 用户只需要写两个东西

```yaml
labels:
  hami.io/taskid: "job-20261026-qwen3-tp16"   # 同一任务的所有 Pod 用同一个值（含 PD 两侧）
annotations:
  hami.io/fabric-requirement: "compute"        # 这个任务必须走哪张网
```

`hami.io/taskid` 决定"哪些 Pod 是一家的、要收拢到同一个域"；
`fabric-requirement` 决定"这个域必须是哪种网络能力"。

`fabric-requirement` 四个取值：

| 取值 | 含义 | 典型作业 | 不满足时 |
|---|---|---|---|
| 不写 / `any` | 不设能力门，行为与今天一致 | 存量任务、不敏感批处理 | 不拒绝 |
| `service` | 只需业务面可达 → 全集群恒满足 | 在线推理、Notebook | 不拒绝 |
| `compute` | 必须有计算面 RDMA，且**同一个 `compute.domain`** | 跨机 TP/PP/DP、跨超节点 scale-out 训练 | **Pending + 可解释事件** |
| `scaleup` | 必须在同一个超节点内部 | 超大张量并行 / EP / 200ns 级时延作业 | **Pending + 可解释事件** |

## 2. 多副本服务：告诉调度器这个服务有多大（可选）

多副本服务（Deployment/StatefulSet/裸 Pod 起的一整个服务）建议再写第三个，可选：

```yaml
annotations:
  hami.io/task-replica-count: "8"          # 这个 taskid 一共要开几个 Pod（PD 两侧之和）
```

它只在**任务的首个 Pod** 选域时起作用：调度器先把"装不下 8 个副本"的超节点从候选里
剔掉，再在剩下的域里定锚，于是后面的 7 个副本到齐时不会撞在满域外面 Pending。
不写这个注解时，调度器改数集群里同 `taskid` 的现存 Pod（对 Deployment/StatefulSet 一样准）；
两个来源都给不出规模时，这一层完全不动作。

三条边界（性能优先但不牺牲交付）：

- **只偏好、不硬拒**：如果候选里没有任何一个域装得下整服务，就一个都不剔除，按原样落位，
  只在事件里写 `no candidate superpod holds N replica(s); largest holds M`。
- **兄弟 Pod 不重算**：任务一旦锚定，后续副本沿用那个域，不因为某台节点空闲波动而改主意。
- **按"Pod 个数"折算**：一个域能放几个副本 = 该域内各节点在槽位/显存/核数三个维度上
  最少能放几个的最小值之和（含正在放的这个 Pod）。PD 两侧卡型不同时，这个折算按
  "当前这个 Pod 的形状"估，属于偏好的近似，不是配额。


## 3. 四类节点该选哪个取值

| 节点类 | 有的平面 | 能跑的 `fabric-requirement` | 可扩展上限 |
|---|---|---|---|
| 一 A3 普通 | 只有业务面 | `any` / `service` | 单机内 |
| 二 A3 + 参数面 RDMA | 业务面 + 计算面 | `compute` | 同 `compute.domain` 内的所有 A3 |
| 三 A5 独立超节点 | 业务面 + 超节点内部 | `scaleup` | **单个超节点**（无计算面上联，跨超节点只能走 25GE） |
| 四 A5 + scale-out RDMA（两套） | 三个都有 | `scaleup` / `compute` | 同一 `compute.domain` 覆盖的那几台超节点 |

> 需要卡数 > 单超节点容量的作业，只能落类四。写 `scaleup` 会被锁在单超节点内，写 `compute` 会要求一个能装下全服务的 scale-out 域。

## 4. 案例文件

| 文件 | 场景 |
|---|---|
| `01-train-compute-a3.yaml` | 类二：A3 跨机训练，`requirement=compute`，同参数面域 |
| `02-train-scaleup-a5.yaml` | 类三：A5 超节点内训练，`requirement=scaleup` |
| `03-train-scaleout-a5-pair.yaml` | 类四：跨两套超节点中的一套做 scale-out 训练 |
| `04-inference-service-pd.yaml` | 推理服务 PD 分离，`requirement=service` + 同 taskid 收拢 + 整服务规模注解 |
| `05-escape-valves.yaml` | 两个逃生阀：`spill`（域满外溢）与 `disable`（本 Pod 不参与拓扑调度） |
| `06-namespace-default.yaml` | 平台还没下发注解时的 namespace 级兜底 |
| `10-node-labels.sh` | 运维手工打标（三张面的 label 写法，可直接执行） |
| `11-configmap-domain-registry.yaml` | `compute.domain` 短名 ↔ 现网 leaf-path 整串的映射表 |

## 5. 打完标/起任务后怎么验

```bash
# 看某个节点的三张面都被解析成什么（调度器启动/变更时也会打同样的一行）
kubectl logs -n kube-system -c vgpu-scheduler-extender deployment/hami-scheduler | grep "topo-plane"

# 看某台节点手工打的原始标签
kubectl get node <node> -o jsonpath='{.metadata.labels}' | tr ',' '\n' | grep topo

# 看某任务是否被收拢到同一个域
kubectl get pods -l hami.io/taskid=job-20261026-qwen3-tp16 -o wide
kubectl logs -n kube-system -c vgpu-scheduler-extender deployment/hami-scheduler | grep "TopoPrepare"

# 看 leaf 软打分这一轮对每个候选说了什么（三带同名 SameNode/SameLeaf/CrossLeaf，需 -v=4）
kubectl logs -n kube-system -c vgpu-scheduler-extender deployment/hami-scheduler | grep -E '\[TopoTrace\]' | tail
# 期望: anchors>=1 且 adjacent>=1；reason=no-compute-plane 表示 11 号注册表没生效、这一层整层没动

# 事件（Pod 级，最直观）
kubectl describe pod <pod> | grep -A2 "superpod placement"
```

上线顺序：`10-node-labels.sh` → `11-configmap-domain-registry.yaml` → 重启 extender → 再起任务。注册表一个进程只读一次，先起任务后建表的话，这个进程会一直按"没有计算面"打分，直到下次重启。

## 6. 出问题怎么退

首选改 chart values（与镜像同一次上线），两个旋钮分别是：

```
scheduler.defaultSchedulerPolicy.superpodSchedulerPolicy: disable   # 打分层与域硬门一起关
scheduler.topology.scoreWeight: 0                                   # 只关软排序，域硬门仍然生效
```

现场急停（来不及走 helm，直接改 Deployment；它是 extender 的启动参数，不是环境变量，改完要换新 Pod 才生效）：

```bash
# 一条 flag 同时关掉打分层与硬门（回退红线用这个，不要动镜像）
kubectl -n kube-system patch deploy hami-scheduler --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/1/command/6","value":"--superpod-scheduler-policy=disable"}]'

# 只松开软打分、保留域硬门（域还要，但排序回到原来的 binpack/spread）
kubectl -n kube-system patch deploy hami-scheduler --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/1/command/-","value":"--topology-score-weight=0"}]'

# 只放开单个任务的拓扑约束（不动集群配置）
kubectl annotate pod <pod> hami.io/superpod-scheduler-policy=disable --overwrite
```

下标以实际 spec 为准，先数一遍：`kubectl -n kube-system get deploy hami-scheduler -o jsonpath='{.spec.template.spec.containers[1].command}' | tr ',' '\n' | cat -n`（现网 `--superpod-scheduler-policy` 在第 7 项，即 jsonpatch 下标 6）。第二条是追加，重复执行会堆叠同名 flag（pflag 取最后一个），恢复时删掉而不是再追加一次。

验证开关真的生效（关之前该 task 有 `[TopoPrepare]`/`[TopoTrace]`，关之后同一 task 新 Pod 两类日志都不该出现）：

```bash
kubectl -n kube-system logs -c vgpu-scheduler-extender deploy/hami-scheduler | grep -E "\[Topo(Prepare|Trace|Skip)\]" | tail
```
