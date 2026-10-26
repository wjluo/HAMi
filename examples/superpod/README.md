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
`hami.io/fabric-requirement` 决定"这个域必须是哪种网络能力"。

`fabric-requirement` 四个取值：

| 取值 | 含义 | 典型作业 | 不满足时 |
|---|---|---|---|
| 不写 / `any` | 不设能力门，行为与今天一致 | 存量任务、不敏感批处理 | 不拒绝 |
| `service` | 只需业务面可达 → 全集群恒满足 | 在线推理、Notebook | 不拒绝 |
| `compute` | 必须有计算面 RDMA，且**同一个 `compute.domain`** | 跨机 TP/PP/DP、跨超节点 scale-out 训练 | **Pending + 可解释事件** |
| `scaleup` | 必须在同一个超节点内部 | 超大张量并行 / EP / 200ns 级时延作业 | **Pending + 可解释事件** |

## 2. 四类节点该选哪个取值

| 节点类 | 有的平面 | 能跑的 `fabric-requirement` | 可扩展上限 |
|---|---|---|---|
| 一 A3 普通 | 只有业务面 | `any` / `service` | 单机内 |
| 二 A3 + 参数面 RDMA | 业务面 + 计算面 | `compute` | 同 `compute.domain` 内的所有 A3 |
| 三 A5 独立超节点 | 业务面 + 超节点内部 | `scaleup` | **单个超节点**（无计算面上联，跨超节点只能走 25GE） |
| 四 A5 + scale-out RDMA（两套） | 三个都有 | `scaleup` / `compute` | 同一 `compute.domain` 覆盖的那几台超节点 |

> 需要卡数 > 单超节点容量的作业，只能落类四。写 `scaleup` 会被锁在单超节点内，写 `compute` 会要求一个能装下全服务的 scale-out 域。

## 3. 案例文件

| 文件 | 场景 |
|---|---|
| `01-train-compute-a3.yaml` | 类二：A3 跨机训练，`requirement=compute`，同参数面域 |
| `02-train-scaleup-a5.yaml` | 类三：A5 超节点内训练，`requirement=scaleup` |
| `03-train-scaleout-a5-pair.yaml` | 类四：跨两套超节点中的一套做 scale-out 训练 |
| `04-inference-service-pd.yaml` | 推理服务 PD 分离，`requirement=service` + 同 taskid 收拢 |
| `05-escape-valves.yaml` | 两个逃生阀：`spill`（域满外溢）与 `disable`（本 Pod 不参与拓扑调度） |
| `06-namespace-default.yaml` | 平台还没下发注解时的 namespace 级兜底 |
| `10-node-labels.sh` | 运维手工打标（三张面的 label 写法，可直接执行） |
| `11-configmap-domain-registry.yaml` | `compute.domain` 短名 ↔ 现网 leaf-path 整串的映射表 |

## 4. 打完标/起任务后怎么验

```bash
# 看某个节点的三张面都被解析成什么（调度器启动/变更时也会打同样的一行）
kubectl logs -n kube-system -c vgpu-scheduler-extender deployment/hami-scheduler | grep "topo-plane"

# 看某台节点手工打的原始标签
kubectl get node <node> -o jsonpath='{.metadata.labels}' | tr ',' '\n' | grep topo

# 看某任务是否被收拢到同一个域
kubectl get pods -l hami.io/taskid=job-20261026-qwen3-tp16 -o wide
kubectl logs -n kube-system -c vgpu-scheduler-extender deployment/hami-scheduler | grep "TopoPrepare"

# 事件（Pod 级，最直观）
kubectl describe pod <pod> | grep -A2 "superpod placement"
```

## 5. 出问题怎么退

```bash
# 一条命令同时关掉打分层与硬门（回退红线用这个，不要动镜像）
kubectl set env deployment/hami-scheduler -n kube-system ENABLE_TOPOLOGY=false

# 只放开单个任务的拓扑约束（不动集群配置）
kubectl annotate pod <pod> hami.io/superpod-scheduler-policy=disable --overwrite
```
