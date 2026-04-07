# Kryo balanced get/add benchmark 火焰图报告

**日期**: 2026-04-07
**目的**: 保留现有 `get-add` benchmark，同时新增一个火焰图中 `RocksDBListState.add:get` 更接近 1:1 的 mixed benchmark。

## 设计

旧 mixed benchmark `keyedListStateGetAddMapPojo` 已经做到每条输入 1 次 `ListState.get()` 和 1 次 `ListState.add()`，但 `getAddStateMaxValuesPerKey=10` 时，`get` 平均要反序列化约 4.5 个元素，导致火焰图中读侧占比明显高于写侧。

新增入口：

- MiniCluster JMH: `KryoStateBenchmark.keyedListStateGetAddBalancedMapPojo`
- Docker/Flink job: `KryoStateJob --mode get-add-balanced`

balanced 入口继续保持每条输入 1 次 `get` 和 1 次 `add`，但默认 `maxValuesPerKey=3`，使 `get` 平均只反序列化约 1 个元素，从而让 `RocksDBListState.get` 和 `RocksDBListState.add` 的样本占比接近 1:1。旧 `keyedListStateGetAddMapPojo` 和 `--mode get-add` 保持不变。

## 运行环境

| 项目 | 值 |
|------|----|
| JDK | Corretto 11.0.26 |
| Flink | 1.16.3 |
| async-profiler | 4.0, `event=itimer` |
| Docker compose | `docker-compose-baseline.yml` |
| Docker backend | RocksDB, `config/flink-conf-rocksdb.yaml` |
| Docker topology | 1 JobManager container, 2 TaskManager containers, 4 `TaskManagerRunner` per TM container |

## 运行命令

MiniCluster JMH：

```bash
export JAVA_HOME=/mnt/data1/wuql/.sdkman/candidates/java/11.0.26-amzn
export PATH="$JAVA_HOME/bin:$PATH"
export AP_LIB=/mnt/data1/wuql/dev/FlinkLargeStateTuning/flink-cluster/async-profiler-4.0-linux-x64/lib/libasyncProfiler.so

java -jar target/benchmarks.jar "KryoStateBenchmark.keyedListStateGetAddBalancedMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=1000 \
  -p mapEntries=20 \
  -p reuseSourceRecords=true \
  -f 1 -wi 0 -i 1 \
  -jvmArgs "-agentpath:${AP_LIB}=start,event=itimer,collapsed,file=docs/flamegraphs/kryo-get-add-balanced/minicluster/keyedListStateGetAddBalancedMapPojo_ROCKS.collapsed"
```

Docker/Flink job：

```bash
cd /mnt/data2/wuql/flink-cluster

docker compose -f docker-compose-baseline.yml cp \
  flink-benchmarks/target/benchmarks.jar \
  jobmanager:/opt/benchmarks.jar

docker compose -f docker-compose-baseline.yml exec -T jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode get-add-balanced \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 10000000 \
  --eventsPerSecond 0 \
  --keys 1000 \
  --mapEntries 20 \
  --reuseSourceRecords true
```

## 产物

| 场景 | HTML | Collapsed |
|------|------|-----------|
| MiniCluster | `docs/flamegraphs/kryo-get-add-balanced/minicluster/keyedListStateGetAddBalancedMapPojo_ROCKS.html` | `docs/flamegraphs/kryo-get-add-balanced/minicluster/keyedListStateGetAddBalancedMapPojo_ROCKS.collapsed` |
| Docker | `docs/flamegraphs/kryo-get-add-balanced/docker/*.html` | `docs/flamegraphs/kryo-get-add-balanced/docker/*.collapsed` |

Docker 产物每个 TaskManager JVM 一组文件，本次共 8 组。

## 吞吐结果

| 场景 | 参数 | 结果 |
|------|------|------|
| MiniCluster collapsed run | `numberOfKeys=1000`, `mapEntries=20`, `reuseSourceRecords=true`, `balancedGetAddStateMaxValuesPerKey=3` | 27.550 ops/ms |
| MiniCluster HTML run | 同上 | 32.580 ops/ms |
| Docker/Flink job | `parallelism=16`, `events=10000000`, `eventsPerSecond=0`, default `maxValuesPerKey=3` | 36.263 s, 约 275.76 K events/s |

相比旧 `get-add` Docker 运行的 71.605 s，balanced 版本更快，主要原因是每次 `get` 反序列化的 list 更短。

## 火焰图统计

以下统计来自 collapsed stack。`processElement` 内占比用于判断状态访问路径，排除了 source、network、JIT 和容器后台线程的影响。

| 指标 | 旧 MiniCluster processElement 内 | 新 MiniCluster processElement 内 | 旧 Docker processElement 内 | 新 Docker processElement 内 |
|------|----------------------------------|----------------------------------|-----------------------------|-----------------------------|
| `RocksDBListState.get` | 80.83% | 52.23% | 81.11% | 52.67% |
| `RocksDBListState.add` | 18.48% | 42.82% | 18.09% | 42.82% |
| RocksDB native `get` | 8.27% | 12.53% | 6.66% | 11.09% |
| RocksDB native `merge` | 5.90% | 17.99% | 6.46% | 15.03% |
| `serializeValue` | 12.51% | 24.76% | 11.55% | 27.60% |
| `deserializeList` | 72.56% | 39.52% | 74.36% | 41.39% |
| `KryoSerializer.serialize` | 12.08% | 23.37% | 11.20% | 26.75% |
| `KryoSerializer.deserialize` | 68.49% | 36.01% | 69.74% | 38.27% |
| `MapSerializer.write` | 10.70% | 20.99% | 10.06% | 24.03% |
| `MapSerializer.read` | 62.31% | 32.27% | 64.03% | 34.83% |

新 benchmark 的 `add:get` 样本比例：

| 场景 | `add:get` |
|------|-----------|
| MiniCluster | `42.82:52.23`, 约 `0.82:1` |
| Docker | `42.82:52.67`, 约 `0.81:1` |

严格来说还不是数学上的 50:50，因为 `RocksDB.get` 和 `deserializeList` 有固定读开销；但相对旧 benchmark 的约 `18:81`，balanced 版本已经把读写热点拉到接近 1:1。

## 结论

新增 balanced benchmark 可以作为同时观察 `ListState.get` 和 `ListState.add` 优化收益的端到端入口。它保留了旧 mixed benchmark 的完整 runtime 形态，但通过默认 `maxValuesPerKey=3` 降低读侧 list 规模，使读写状态路径在同一张火焰图里接近均衡。

建议后续使用：

- 偏读侧 mixed workload：继续跑 `KryoStateBenchmark.keyedListStateGetAddMapPojo` 或 `KryoStateJob --mode get-add`
- 读写相对均衡 mixed workload：跑 `KryoStateBenchmark.keyedListStateGetAddBalancedMapPojo` 或 `KryoStateJob --mode get-add-balanced`
- 纯状态访问 throughput gate：继续跑 `KryoListStateBenchmark.listAddMapPojo` 和 `listGetAndIterateMapPojo`
