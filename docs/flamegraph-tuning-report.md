# KryoStateBenchmark 火焰图调参实验报告

**日期**: 2026-04-03
**基准 commit**: `020eee3` (Switch to Processing Time Windows to match production flame graph)
**Benchmark**: `KryoStateBenchmark.windowProcessMapPojo`
**backendMode**: `ROCKS`

## 实验目的

在当前 baseline 配置下，火焰图中 RocksDB 读路径（`onProcessingTime → RocksDBListState.getInternal → RocksDB.get`）占比仅 18%，写路径（`processElement → merge`）占 53.6%。目标是通过调参提高读路径占比，使 `RocksDB.get`、`snappy` 解压、`BlockBasedTable::Get` 等读路径细节更清晰可见。

## 实验环境

| 项目 | 值 |
|------|------|
| JDK | OpenJDK 11.0.26 (Corretto) |
| async-profiler | 4.0, event=itimer |
| JMH | fork=1, warmup=1, measurement=3 |
| 窗口类型 | `TumblingProcessingTimeWindows` |
| backendMode | ROCKS |

## 可调参数

| 参数 | 代码位置 | 作用 |
|------|----------|------|
| `RECORDS_PER_INVOCATION` | 类常量 | 每次 invocation 发送的记录数 |
| `numberOfKeys` | `KryoStateContext.numberOfKeys` | key 数量；影响每 key 状态大小和 get 次数 |
| `MAP_ENTRIES` | `MapRecordSource.MAP_ENTRIES` | 每条记录 Map 中的 entry 数；控制单条记录序列化权重 |
| Window size | benchmark 方法中 `Time.seconds(5)` | 窗口大小；影响 window trigger 频率 |
| `FIX_PER_SLOT_MEMORY_SIZE` | `createConfiguration()` | RocksDB 总内存（含 block cache + write buffer） |

## 调参假设

读路径占比由以下因素决定：

1. **写路径 = N × (source开销 + serialize + merge)**：每条记录都走一次完整写路径
2. **读路径 = K × get_native + N × deserialize**：window trigger 时，K 个 key 各一次 get，反序列化 N 条记录
3. 由于 serialize ≈ deserialize（处理相同 N 条记录），**写路径多出的开销**是 source 开销和 per-record merge

因此提升读路径占比的杠杆：
- **增大 MAP_ENTRIES** → 增大每条记录的序列化权重 → 压缩 merge/source 的相对占比
- **减少 keys** → 每 key 状态更大 → 单次 get 需要读更多 RocksDB block → 增大 get_native 和 snappy
- **减小 block cache** → 强制更多 cache miss → 增大 get_native（但同时增加 write stall）

## 实验结果

### 结果总览

| 实验 | records | keys | MAP | cache | Write% | **Read%** | **get native%** | snappy% | deser% | merge% | ops/ms |
|------|---------|------|-----|-------|--------|-----------|-----------------|---------|--------|--------|--------|
| **1 (baseline)** | 1M | 1000 | 20 | 307MB | 53.6 | 18.0 | 2.3 | 1.3 | 15.3 | 8.8 | 36.8 |
| 2 | 1M | 100 | 20 | 307MB | 50.8 | 19.7 | 2.2 | 1.3 | 17.2 | 8.1 | 37.4 |
| 3 | 1M | 100 | 50 | 307MB | 52.4 | 21.8 | 2.8 | 1.8 | 18.6 | 4.7 | 16.8 |
| 4 | 1M | 10 | 20 | 307MB | 53.0 | 18.8 | 2.3 | 1.3 | 16.1 | 8.3 | ~36 |
| **5** | **1M** | **10** | **100** | **307MB** | **53.1** | **25.1** | **4.4** | **3.4** | **20.3** | **2.5** | **9.0** |
| 6 | 1M | 10 | 100 | 64MB | 47.4 | 22.4 | 3.5 | 2.6 | 18.5 | 2.0 | 9.1 |
| 7 | 500K | 10 | 100 | 307MB | 52.6 | 23.8 | 2.9 | 2.1 | 20.5 | 2.4 | 18.2 |
| **8** | **1M** | **10** | **200** | **307MB** | **52.0** | **26.1** | **4.6** | **3.7** | **21.1** | **1.3** | **4.1** |

### 火焰图文件

| 实验 | HTML | Collapsed |
|------|------|-----------|
| 1 (baseline) | `flamegraphs/windowProcessMapPojo_ROCKS.html` | `flamegraphs/windowProcessMapPojo_ROCKS.collapsed` |
| 2 | `flamegraphs/exp2_keys100.html` | `flamegraphs/exp2_keys100.collapsed` |
| 3 | `flamegraphs/exp3_keys100_map50.html` | `flamegraphs/exp3_keys100_map50.collapsed` |
| 4 | `flamegraphs/exp4_keys10.html` | `flamegraphs/exp4_keys10.collapsed` |
| 5 | `flamegraphs/exp5_keys10_map100.html` | `flamegraphs/exp5_keys10_map100.collapsed` |
| 6 | — | `flamegraphs/exp6_keys10_map100_cache64m.collapsed` |
| 7 | — | `flamegraphs/exp7_500K_keys10_map100.collapsed` |
| 8 | `flamegraphs/exp8_keys10_map200.html` | `flamegraphs/exp8_keys10_map200.collapsed` |

## 逐项分析

### 实验 2: 减少 keys (1000→100)

**配置变更**: `numberOfKeys = 100`（其余同 baseline）

读路径从 18.0% → 19.7%，提升不明显。减少 keys 让每 key 积累更多记录（10K vs 1K），但总数据量不变，没有导致更多 cache miss。get 次数从 1000 降到 100，native get 反而可能减少。

**结论**: 单独减少 keys 效果有限。

### 实验 3: 减少 keys + 增大 MAP (50 entries)

**配置变更**: `numberOfKeys = 100`, `MAP_ENTRIES = 50`

读路径 → 21.8%，get native → 2.8%。MAP_ENTRIES 从 20→50 使每条记录序列化权重增大 2.5x，throughput 降到 16.8 ops/ms。更重的序列化压缩了 merge 和 source 的相对占比（merge 从 8.8% → 4.7%）。

**结论**: MAP_ENTRIES 是有效杠杆。

### 实验 4: 极少 keys (10)

**配置变更**: `numberOfKeys = 10`（其余同 baseline）

读路径 18.8%，与 baseline 几乎相同。仅减少 keys 不改变总工作量。

**结论**: 单独减少 keys 无效。

### 实验 5: 少 keys + 大 MAP (100 entries) ⭐ 推荐

**配置变更**: `numberOfKeys = 10`, `MAP_ENTRIES = 100`

读路径跃升至 **25.1%**。关键变化：
- get native: 2.3% → **4.4%**（几乎翻倍）
- snappy: 1.3% → **3.4%**（2.6x）
- BlockBasedTable::Get: 1.7% → **4.0%**
- merge: 8.8% → **2.5%**（被压缩）

大 MAP 使每条记录的序列化字节显著增大，总状态量远超 block cache 容量，导致 window trigger 时 `RocksDB.get()` 必须从 SST 文件读取并解压 — 这正是用户期望看到的读路径细节。

**结论**: 这是读路径可见性最高的实用配置。Throughput ~9 ops/ms，运行时间合理（~2分钟）。

### 实验 6: 缩小 block cache (64MB)

**配置变更**: 同 exp5，`FIX_PER_SLOT_MEMORY_SIZE = 64mb`

读路径反而降到 22.4%。缩小 cache 导致更多 write stall 和 compaction 开销，这些 CPU 消耗进入了非读非写的 background 开销，反而压缩了读路径占比。

**结论**: 缩小 cache 适得其反。

### 实验 7: 减少 records (500K)

**配置变更**: 同 exp5，`RECORDS_PER_INVOCATION = 500_000`

读路径 23.8%，低于 exp5 的 25.1%。更少的记录意味着更小的总状态量，block cache 能容纳更多数据，减少了 native get 的 I/O 开销。

**结论**: 减少 records 不利于暴露读路径。

### 实验 8: 极大 MAP (200 entries)

**配置变更**: `numberOfKeys = 10`, `MAP_ENTRIES = 200`

读路径达到实验最高 **26.1%**。get native **4.6%**，snappy **3.7%**。但 throughput 降至 4.1 ops/ms，每次 invocation 约 16 分钟，运行时间较长。

**结论**: MAP=200 的提升相对于 MAP=100 是边际的（+1%），但运行时间翻倍。

## 结论与建议

### 最有效的参数

| 参数 | 方向 | 效果 | 副作用 |
|------|------|------|--------|
| **MAP_ENTRIES ↑** | 20 → 100 → 200 | 读路径 18% → 25% → 26% | Throughput 下降 |
| **numberOfKeys ↓** | 1000 → 10 | 配合大 MAP 有效 | 单独无效 |
| **block cache ↓** | 307MB → 64MB | 无效（反而下降） | 增加 write stall |
| **records ↓** | 1M → 500K | 无效（减少 cache miss） | 反而降低读路径 |

### 推荐配置

对于需要同时展现**写路径**和**读路径**的火焰图：

```
RECORDS_PER_INVOCATION = 1_000_000
numberOfKeys = 10
MAP_ENTRIES = 100
Window = TumblingProcessingTimeWindows.of(Time.seconds(5))
FIX_PER_SLOT_MEMORY_SIZE = 307MB (default)
```

这个配置（实验 5）在**运行时间可接受**（~2分钟）的前提下，实现了**读路径 25.1%** 的最佳平衡：
- 写路径调用栈完整（serialize → KryoSer → MapSerializer.write → merge）
- 读路径调用栈完整（get → DBImpl::Get → BlockBasedTable → snappy + deserialize → KryoSer → MapSerializer.read）
- RocksDB native 读路径细节清晰可见（4.4% get native，含 snappy 3.4%）

### 为什么读路径不能超过 ~26%

在 ListState + window 的 workload 中，读路径占比有结构性上限：

1. **写路径**: N × (source创建 + serialize + merge) + shuffle
2. **读路径**: K × get_native + N × deserialize

由于 serialize ≈ deserialize，两者计算量大致相等。但写路径额外包含 **N 次 source 对象创建**（每条记录创建 HashMap 和 MapRecord）和 **N 次 merge JNI 调用**。这部分固定开销使写路径始终 > 读路径。

collapsed stack 的额外量化结果也支持这个结论：

| 实验 | ListState 总占比 | keyBy/network 序列化反序列化 | source 构造 |
|------|------------------|-------------------------------|-------------|
| baseline | 42.9% | 32.7% | 7.5% |
| exp5 keys10 map100 | 47.3% | 37.8% | 8.7% |
| exp8 keys10 map200 | 46.9% | 39.2% | 9.7% |

因此，`windowProcessMapPojo` 的价值是复现真实窗口调用栈，而不是最大化 `ListState` 的吞吐信号。继续增大 `MAP_ENTRIES` 主要会同时放大输入网络反序列化和状态序列化，`ListState` 总占比提升有限。

## Benchmark 优化建议

### 保留 window benchmark 作为生产复现基准

代码已将以下项改为 JMH 参数：

| 参数 | 默认值 | 用途 |
|------|--------|------|
| `numberOfKeys` | 1000 | 控制 key 数量 |
| `mapEntries` | 20 | 控制 `MapRecord.counters` 大小 |
| `reuseSourceRecords` | false | 复用 source 端预构造 `MapRecord`，减少 source `HashMap` 构造 |

建议运行配置：

```bash
java -jar target/benchmarks.jar "KryoStateBenchmark.windowProcessMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p reuseSourceRecords=true \
  -f 1 -wi 1 -i 3
```

`reuseSourceRecords=true` 会改变 source payload 的生成方式，但不会改变 `ListState.add/get` 的状态序列化路径。它适合用来降低 source 构造噪声；如需严格保持每条输入都新建 Map 的生产形态，应保持默认 `false`。

### 新增 direct ListState benchmark 作为优化收益基准

新增 `org.apache.flink.state.benchmark.KryoListStateBenchmark`，直接测：

- `listAddMapPojo`: `setCurrentKey` → `ListState.add(MapRecord)`
- `listGetAndIterateMapPojo`: `setCurrentKey` → `ListState.get()` → 遍历返回值

建议运行配置：

```bash
java -jar target/benchmarks.jar "KryoListStateBenchmark.listAddMapPojo|KryoListStateBenchmark.listGetAndIterateMapPojo" \
  -p backendType=ROCKSDB \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p valuesPerKey=1000 \
  -f 1 -wi 1 -i 3
```

这个 direct benchmark 绕开 MiniCluster、source、`keyBy` shuffle、window trigger 和 mailbox，保留 `ListState<MapRecord>` 的 `PojoSerializer -> KryoSerializer -> MapSerializer` 路径。因此它更适合评估未来 `RocksDBListState` 或 list serializer 优化能带来的吞吐提升。
