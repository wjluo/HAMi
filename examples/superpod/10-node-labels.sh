#!/usr/bin/env bash
# 运维手工打标 · 三张网络平面
#
# 规则（与调度器归一器约定一致）：
#   1) 一层一条 label：hami.io/topo.<plane>.<tier>=<值>
#      —— label 值不能含 "/"、上限 63 字符，所以不把多层压成一个串
#   2) "确实没有" 要显式写 none（小写精确值）；留空 = 还没打标 = 未知，语义完全不同
#   3) scaleup 由 ADP 探测自动写，运维不要手工覆盖；只有需要给没有拓扑 API 的芯片兜底时才手工写
#   4) 现网既有 label topology.ascend.huawei.com/path 不动，它继续充当 compute 平面的 leaf 层
#
# 用法：编辑下面的节点/域对应关系后执行；每条都是 --overwrite 幂等，可反复跑。
set -euo pipefail

# ---------- 类二：A3 传统 RDMA 高网（计算面/参数面）----------
# 现网已投产的 path 标签保持不动，这里只补 domain 短名与 scaleup=none
PF_DOMAIN="PF-SZ01"
for n in \
  st03-az01phmu03-w2.cic.cmbchina.cn \
  st03-az01phmu03-w1.cic.cmbchina.cn
do
  kubectl label node "$n" \
    "hami.io/topo.compute.domain=${PF_DOMAIN}" \
    "hami.io/topo.compute.leaf=LF46_A" \
    "hami.io/topo.scaleup.domain=none" \
    --overwrite
done

# ---------- 类三：A5 独立超节点（无计算面上联）----------
# compute 必须显式 none，否则"未打标"会让硬 requirement=compute 的作业落到这里走 25GE
for n in \
  cicpoc-gpu1.example.com
do
  kubectl label node "$n" \
    "hami.io/topo.compute.domain=none" \
    --overwrite
done

# ---------- 类四：A5 + scale-out RDMA，两套各一个域 ----------
SO_DOMAIN="SO-SET1"          # 另一套改成 SO-SET2
for n in \
  cicpoc-gpu2.example.com
do
  kubectl label node "$n" \
    "hami.io/topo.compute.domain=${SO_DOMAIN}" \
    "hami.io/topo.compute.leaf=LF49_A" \
    --overwrite
done

# ---------- 类一：只有业务面 25GE ----------
for n in \
  cicpoc-ctl1.example.com
do
  kubectl label node "$n" \
    "hami.io/topo.compute.domain=none" \
    "hami.io/topo.scaleup.domain=none" \
    --overwrite
done

# ---------- 核对：每台节点解析出的三张面 ----------
kubectl get nodes -L hami.io/topo.compute.domain -L hami.io/topo.compute.leaf -L hami.io/topo.scaleup.domain
kubectl get nodes -L topology.ascend.huawei.com/path

# domain 短名与现网 path 整串的映射见 11-configmap-domain-registry.yaml；
# 两者不一致时调度器会把该平面降级为"未打标"并在日志里报 domain-mismatch。
