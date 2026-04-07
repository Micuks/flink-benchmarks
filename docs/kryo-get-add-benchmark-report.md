# Kryo get/add benchmark 火焰图报告

**日期**: 2026-04-07
**目的**: 新增并验证同一张火焰图里同时包含 `ListState.get()` 和 `ListState.add()` 的 Kryo/RocksDB benchmark。

## Benchmark 入口

本次新增三类入口：

- MiniCluster JMH: `KryoStateBenchmark.keyedListStateGetAddMapPojo`
- Docker/Flink job: `KryoStateJob --mode get-add`
- Direct state JMH: `KryoListStateBenchmark.listGetAndAddMapPojo`

MiniCluster 和 Docker 入口都走完整 `keyBy -> KeyedProcessFunction -> RocksDBListState` 路径。`processElement` 内部先遍历 `ListState.get()`，再执行 `ListState.add(MapRecord)`，并通过 `maxValuesPerKey` 限制每个 key 的 list 长度，避免读侧状态无限膨胀。

## 运行环境

| 项目 | 值 |
|------|----|
| JDK | Corretto 11.0.26 |
| Flink | 1.16.3 |
| async-profiler | 4.0, `event=itimer` |
| Docker compose | `docker-compose-baseline.yml` |
| Docker backend | RocksDB, `config/flink-conf-rocksdb.yaml` |

## 运行命令

构建：

```bash
export JAVA_HOME=/mnt/data1/wuql/.sdkman/candidates/java/11.0.26-amzn
export PATH="$JAVA_HOME/bin:$PATH"
mvn clean -DskipTests package
```

MiniCluster JMH：

```bash
export AP_LIB=/mnt/data1/wuql/dev/FlinkLargeStateTuning/flink-cluster/async-profiler-4.0-linux-x64/lib/libasyncProfiler.so

java -jar target/benchmarks.jar "KryoStateBenchmark.keyedListStateGetAddMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=1000 \
  -p mapEntries=20 \
  -p reuseSourceRecords=true \
  -p getAddStateMaxValuesPerKey=10 \
  -f 1 -wi 0 -i 1 \
  -jvmArgs "-agentpath:${AP_LIB}=start,event=itimer,collapsed,file=docs/flamegraphs/kryo-get-add/minicluster/keyedListStateGetAddMapPojo_ROCKS.collapsed"
```

Docker/Flink job：

```bash
cd /mnt/data2/wuql/flink-cluster
cp config/flink-conf-rocksdb.yaml config/flink-conf.yaml
docker compose -f docker-compose-baseline.yml up -d --remove-orphans
docker compose -f docker-compose-baseline.yml cp \
  flink-benchmarks/target/benchmarks.jar \
  jobmanager:/opt/benchmarks.jar

docker compose -f docker-compose-baseline.yml exec -T jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode get-add \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 10000000 \
  --eventsPerSecond 0 \
  --keys 1000 \
  --mapEntries 20 \
  --maxValuesPerKey 10 \
  --reuseSourceRecords true
```

Docker 火焰图是在提交 job 前，对 `taskmanager1` 和 `taskmanager2` 容器里的全部 8 个 `TaskManagerRunner` JVM 启动 async-profiler；job 完成后 dump HTML 和 collapsed stack。

## 产物

| 场景 | HTML | Collapsed |
|------|------|-----------|
| MiniCluster | `docs/flamegraphs/kryo-get-add/minicluster/keyedListStateGetAddMapPojo_ROCKS.html` | `docs/flamegraphs/kryo-get-add/minicluster/keyedListStateGetAddMapPojo_ROCKS.collapsed` |
| Docker | `docs/flamegraphs/kryo-get-add/docker/*.html` | `docs/flamegraphs/kryo-get-add/docker/*.collapsed` |

Docker 产物每个 TaskManager JVM 一组文件，本次共 8 组。

## 吞吐结果

| 场景 | 参数 | 结果 |
|------|------|------|
| MiniCluster collapsed run | `numberOfKeys=1000`, `mapEntries=20`, `reuseSourceRecords=true`, `maxValuesPerKey=10` | 17.220 ops/ms |
| MiniCluster HTML run | 同上 | 18.082 ops/ms |
| Docker/Flink job | `parallelism=16`, `events=10000000`, `eventsPerSecond=0` | 71.605 s, 约 139.65 K events/s |

## 火焰图统计

以下统计来自 collapsed stack。`总样本` 用于观察整体 job 噪声；`processElement 内` 更适合判断 `ListState` 优化收益，因为它排除了 source、network shuffle、JIT 和容器后台线程。

| 指标 | MiniCluster 总样本 | MiniCluster processElement 内 | Docker 总样本 | Docker processElement 内 |
|------|--------------------|-------------------------------|---------------|--------------------------|
| 样本数 | 8154 | 5357 | 103773 | 68581 |
| `processElement` | 65.70% | 100.00% | 66.09% | 100.00% |
| `RocksDBListState.get` | 53.10% | 80.83% | 53.60% | 81.11% |
| `RocksDBListState.add` | 12.14% | 18.48% | 11.96% | 18.09% |
| `RocksDBListState.clear` | 0.00% | 0.00% | 0.00% | 0.00% |
| RocksDB native `get` | 5.43% | 8.27% | 4.40% | 6.66% |
| RocksDB native `merge` | 3.88% | 5.90% | 4.27% | 6.46% |
| RocksDB native `delete` | 0.29% | 0.45% | 0.31% | 0.47% |
| `serializeValue` | 8.22% | 12.51% | 7.63% | 11.55% |
| `deserializeList` | 47.67% | 72.56% | 49.14% | 74.36% |
| `KryoSerializer.serialize` | 10.58% | 12.08% | 12.85% | 11.20% |
| `KryoSerializer.deserialize` | 55.68% | 68.49% | 57.57% | 69.74% |
| `MapSerializer.write` | 8.82% | 10.70% | 11.28% | 10.06% |
| `MapSerializer.read` | 50.53% | 62.31% | 52.71% | 64.03% |

## 非 processElement 开销

| 指标 | MiniCluster 总样本 | Docker 总样本 |
|------|--------------------|---------------|
| Source | 2.86% | 6.02% |
| Network serialize | 2.76% | 5.59% |
| Network deserialize | 11.74% | 12.62% |

Docker 模式下 source 和 network serialize 比 MiniCluster 更高，原因是外部集群用 `KryoStateJob$BoundedRateLimitedSource` 发数据，并且 profile 了全部 TaskManager JVM。即便如此，`processElement` 仍占总样本约 66%，主要热点仍在 `RocksDBListState.get/add`。

## 结论

新 benchmark 已满足“同一张火焰图同时包含 get 和 add”的目标。MiniCluster 和 Docker 两个环境的占比高度一致：`processElement` 约 66%，其中 `RocksDBListState.get` 约 81%，`RocksDBListState.add` 约 18%。

当前配置下读侧明显更重，核心路径是 `ListState.get -> RocksDB.get -> ListDelimitedSerializer.deserializeList -> KryoSerializer.deserialize -> MapSerializer.read`。写侧也稳定出现在同一张火焰图里，核心路径是 `ListState.add -> serializeValue -> KryoSerializer.serialize -> MapSerializer.write -> RocksDB.merge`。

建议后续把这个 mixed benchmark 作为端到端验证入口，把 `KryoListStateBenchmark.listAddMapPojo` 和 `listGetAndIterateMapPojo` 继续作为更纯粹的 direct state throughput gate。如果希望提高 add 在混合火焰图里的占比，可以降低 `maxValuesPerKey` 或在 `processElement` 中增加每次输入的 add 次数；如果希望更接近当前读写平衡，则保持 `maxValuesPerKey=10`。
