# KryoListStateBenchmark RocksDB 火焰图报告

**日期**: 2026-04-07
**Benchmark**: `KryoListStateBenchmark`
**backendType**: `ROCKSDB`
**目的**: 评估 direct `ListState<MapRecord>` benchmark 是否比 window benchmark 更适合作为未来 `ListState` 优化的吞吐收益基准。

## 背景

`docs/kryo-state-job.md` 说明了外部 Flink job 的 profiling 原则：不要 profile `flink run` client，要 profile 真正执行状态访问的 JVM。这里跑的是 JMH direct benchmark，不经过外部 Flink 集群和 `flink run` client，因此直接在 JMH fork JVM 上挂 async-profiler。

与 `KryoStateBenchmark.windowProcessMapPojo` 相比，`KryoListStateBenchmark` 绕开 MiniCluster、source、`keyBy` shuffle、window trigger 和 mailbox，只保留：

- `ListState<MapRecord>` 状态访问
- RocksDB state backend
- `PojoSerializer -> KryoSerializer -> MapSerializer`
- RocksDB native `merge/get`

## 运行环境

| 项目 | 值 |
|------|----|
| JDK | Corretto 11.0.26 |
| Flink | 1.16.3 |
| JMH | 1.19 |
| async-profiler | 4.0, `event=itimer` |
| build | `mvn clean -DskipTests package` |

注意：本机默认 `JAVA_HOME` 指向 JDK 24，曾导致 `target/benchmarks.jar` 缺少 `META-INF/BenchmarkList`。本次运行显式使用 JDK 11 构建和运行。

## 运行命令

```bash
export JAVA_HOME=/mnt/data1/wuql/.sdkman/candidates/java/11.0.26-amzn
export PATH="$JAVA_HOME/bin:$PATH"
export AP_LIB=/mnt/data1/wuql/dev/FlinkLargeStateTuning/flink-cluster/async-profiler-4.0-linux-x64/lib/libasyncProfiler.so

mvn clean -DskipTests package
```

写路径 collapsed stack：

```bash
java -jar target/benchmarks.jar "KryoListStateBenchmark.listAddMapPojo" \
  -p backendType=ROCKSDB \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p valuesPerKey=1000 \
  -p recordPoolSize=1024 \
  -f 1 -wi 1 -i 3 \
  -jvmArgs "-agentpath:${AP_LIB}=start,event=itimer,collapsed,file=docs/flamegraphs/kryo-list-state/listAddMapPojo_ROCKSDB.collapsed"
```

读路径 collapsed stack：

```bash
java -jar target/benchmarks.jar "KryoListStateBenchmark.listGetAndIterateMapPojo" \
  -p backendType=ROCKSDB \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p valuesPerKey=1000 \
  -p recordPoolSize=1024 \
  -f 1 -wi 1 -i 3 \
  -jvmArgs "-agentpath:${AP_LIB}=start,event=itimer,collapsed,file=docs/flamegraphs/kryo-list-state/listGetAndIterateMapPojo_ROCKSDB.collapsed"
```

HTML 火焰图使用同样参数，将 `collapsed,file=...collapsed` 替换为 `file=...html`。

## 产物

| Benchmark | HTML | Collapsed |
|-----------|------|-----------|
| `listAddMapPojo` | `docs/flamegraphs/kryo-list-state/listAddMapPojo_ROCKSDB.html` | `docs/flamegraphs/kryo-list-state/listAddMapPojo_ROCKSDB.collapsed` |
| `listGetAndIterateMapPojo` | `docs/flamegraphs/kryo-list-state/listGetAndIterateMapPojo_ROCKSDB.html` | `docs/flamegraphs/kryo-list-state/listGetAndIterateMapPojo_ROCKSDB.collapsed` |

## Mixed get/add benchmark

新增 `listGetAndAddMapPojo`，用于在同一张火焰图里同时观察 `RocksDBListState.get` 和 `RocksDBListState.add`：

- 先对读 key 执行 `ListState.get()` 并完整遍历返回值
- 再对另一组 disjoint write key 执行 `addsPerGet` 次 `ListState.add(MapRecord)`
- 读写 key 分离是为了避免每次 invocation 的 add 持续放大下一次 get 的 list 大小，从而让读侧输入规模更稳定

推荐先用较小的 `valuesPerKey` 搭配 `addsPerGet`，否则 `get` 反序列化 1000 条大 MapRecord 会压倒单次 add 路径：

```bash
java -jar target/benchmarks.jar "KryoListStateBenchmark.listGetAndAddMapPojo" \
  -p backendType=ROCKSDB \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p valuesPerKey=100 \
  -p addsPerGet=100 \
  -f 1 -wi 1 -i 3 \
  -jvmArgs "-agentpath:${AP_LIB}=start,event=itimer,collapsed,file=docs/flamegraphs/kryo-list-state/listGetAndAddMapPojo_ROCKSDB.collapsed"
```

## 吞吐结果

| Benchmark | Throughput |
|-----------|------------|
| `listAddMapPojo` | 41.704 ops/ms |
| `listGetAndIterateMapPojo` | 0.020 ops/ms |

`listGetAndIterateMapPojo` 的一次 operation 会读取并遍历当前 key 下约 1000 个 `MapRecord`，每个 record 含 100 个 map entry，因此吞吐显著低于单次 add 是预期结果。

## 火焰图分析

### 方法体内样本

方法体内样本只统计 JMH `_thrpt_jmhStub` 下的栈，排除 benchmark setup、RocksDB background flush、JIT 和 GC 噪声。

| 指标 | `listAddMapPojo` | `listGetAndIterateMapPojo` |
|------|------------------|----------------------------|
| 方法体样本数 | 403 | 408 |
| `RocksDBListState` | 99.50% | 99.51% |
| `RocksDBListState.add` | 99.50% | N/A |
| `RocksDBListState.get` | N/A | 99.51% |
| `serializeValue` | 86.85% | N/A |
| `deserializeList` | N/A | 79.90% |
| `KryoSerializer.serialize` | 85.86% | N/A |
| `KryoSerializer.deserialize` | N/A | 77.70% |
| `MapSerializer.write` | 84.62% | N/A |
| `MapSerializer.read` | N/A | 75.98% |
| RocksDB native `merge` | 12.41% | N/A |
| RocksDB native `get` | 0.25% | 19.61% |
| snappy | N/A | 12.99% |

### 总样本

总样本包含 setup、JIT、GC 和 RocksDB background flush，因此只用于判断噪声来源，不作为优化收益的主指标。

| 指标 | `listAddMapPojo` | `listGetAndIterateMapPojo` |
|------|------------------|----------------------------|
| 总样本数 | 1101 | 1092 |
| 方法体样本数 | 403 | 408 |
| setup 样本数 | 262 | 270 |
| RocksDB background 样本数 | 124 | 106 |
| JIT 样本数 | 222 | 207 |
| GC 样本数 | 27 | 26 |
| 总体 `RocksDBListState` | 51.95% | 54.03% |

## 结论

`KryoListStateBenchmark` 已经把方法体内热点几乎全部集中到 `RocksDBListState`：

- 写路径：`ListState.add -> serializeValue -> PojoSerializer -> KryoSerializer -> MapSerializer.write -> RocksDB.merge`
- 读路径：`ListState.get -> RocksDB.get -> ListDelimitedSerializer.deserializeList -> PojoSerializer -> KryoSerializer -> MapSerializer.read`

因此它比 `KryoStateBenchmark.windowProcessMapPojo` 更适合作为未来 `ListState` 优化的 throughput gate。window benchmark 仍应保留，用于验证生产形态的 `WindowOperator + timer + RocksDBListState + Kryo` 调用栈。

## 后续建议

1. 对 `ListState` 内部实现做优化时，优先比较 `KryoListStateBenchmark.listAddMapPojo` 和 `listGetAndIterateMapPojo` 的吞吐变化。
2. 如果要降低火焰图中的 setup/JIT 噪声，可以增加 `-wi`、`-i` 或单次 iteration 时间，例如 `-r 5s`。
3. 如果要观察 RocksDB block cache miss 或 snappy 解压，重点看 `listGetAndIterateMapPojo`，因为当前读路径方法体内 RocksDB native `get` 为 19.61%，snappy 为 12.99%。
4. 最后再跑 `KryoStateBenchmark.windowProcessMapPojo`，确认优化在生产式 window pipeline 中仍可见。
