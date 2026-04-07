# Kryo large-state benchmark 火焰图报告

**日期**: 2026-04-07
**目的**: 在保留现有 `get-add-state-payload` benchmark 的同时，新增并验证一个大状态场景，让 `ListState` 路径仍然占主要比例，并通过扩大状态规模提高 RocksDB native `get/merge` 的耗时占比。

## 设计

新增入口：

- MiniCluster JMH: `KryoStateBenchmark.keyedListStateLargeStateMapPojo`
- Docker/Flink job: `KryoStateJob --mode large-state`

核心设计：

- source 和 `keyBy` 继续使用轻量 `SimpleRecord`，避免网络侧 `MapRecord` Kryo 噪声稀释状态路径
- operator 内部状态仍是 `ListState<MapRecord>`，保留状态侧 `PojoSerializer -> KryoSerializer -> MapSerializer`
- 每条输入都执行 `ListState.add(MapRecord)`，持续制造 RocksDB `merge`
- 每个 key 累积到 `largeStateGetEvery` 后执行一次 `ListState.get()` 并遍历，制造稀疏但更大的 RocksDB `get`
- 达到 `largeStateClearEvery` 后清理当前 key，用于控制峰值状态规模

该设计不是通过降低状态侧 Kryo/Map 计算量来提高 RocksDB 相对占比，而是通过扩大 key 空间和单 key list 保留长度来增加 RocksDB 工作集。

## 运行环境

| 项目 | 值 |
|------|----|
| JDK | Corretto 11.0.26 |
| Flink | 1.16.3 |
| async-profiler | 4.0, `event=itimer` |
| Docker compose | `docker-compose-baseline.yml` |
| Docker backend | RocksDB, `config/flink-conf-rocksdb.yaml` |
| Docker topology | 1 JobManager container, 2 TaskManager containers, 4 `TaskManagerRunner` per TM container |

## 运行配置

MiniCluster JMH：

```bash
java -jar target/benchmarks.jar "KryoStateBenchmark.keyedListStateLargeStateMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=1000 \
  -p mapEntries=20 \
  -p largeStateGetEvery=1000 \
  -p largeStateClearEvery=1000 \
  -f 1 -wi 0 -i 1
```

Docker/Flink job：

```bash
docker compose -f docker-compose-baseline.yml exec -T jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode large-state \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 10000000 \
  --eventsPerSecond 0 \
  --keys 10000 \
  --mapEntries 20 \
  --largeStateGetEvery 1000 \
  --largeStateClearEvery 1000
```

Docker 这轮使用 10M 事件和 10K keys 做受控验证，峰值状态规模接近 `10000 * 1000 = 10M` 条 `MapRecord`。100M 事件、100K keys 的压力配置尚未在本轮执行。

## 产物

| 场景 | HTML | Collapsed |
|------|------|-----------|
| MiniCluster | `docs/flamegraphs/kryo-large-state/minicluster/keyedListStateLargeStateMapPojo_ROCKS_keys1000_map20_get1000_clear1000.html` | `docs/flamegraphs/kryo-large-state/minicluster/keyedListStateLargeStateMapPojo_ROCKS_keys1000_map20_get1000_clear1000.collapsed` |
| Docker | `docs/flamegraphs/kryo-large-state/docker/*.html` | `docs/flamegraphs/kryo-large-state/docker/*.collapsed` |

Docker 产物每个 TaskManager JVM 一组文件，本次共 8 组。

## 吞吐结果

| 场景 | 参数 | 结果 |
|------|------|------|
| MiniCluster collapsed run | `numberOfKeys=1000`, `mapEntries=20`, `largeStateGetEvery=1000`, `largeStateClearEvery=1000` | `56.245 ops/ms` |
| MiniCluster HTML run | 同上 | `61.266 ops/ms` |
| Docker/Flink job | `events=10000000`, `keys=10000`, `mapEntries=20`, `largeStateGetEvery=1000`, `largeStateClearEvery=1000` | `17.213 s`, 约 `581.0 K events/s` |

## 火焰图统计

以下统计来自 collapsed stack。`总样本` 表示端到端 profile，`processElement 内` 表示排除 source、network、JIT 和容器后台线程后的 operator 内占比。

| 指标 | MiniCluster 总样本 | MiniCluster processElement 内 | Docker 总样本 | Docker processElement 内 |
|------|--------------------|-------------------------------|---------------|--------------------------|
| `processElement` | `47.74%` | `100.00%` | `78.01%` | `100.00%` |
| `RocksDBListState.get` | `25.59%` | `53.62%` | `41.49%` | `53.19%` |
| `RocksDBListState.add` | `21.76%` | `45.59%` | `35.73%` | `45.80%` |
| `RocksDBListState.get+add` | `47.36%` | `99.21%` | `77.23%` | `98.99%` |
| RocksDB native `get` | `2.21%` | `4.62%` | `4.46%` | `5.72%` |
| RocksDB native `merge` | `8.44%` | `17.69%` | `14.57%` | `18.66%` |
| RocksDB native `get+merge` | `10.65%` | `22.31%` | `19.03%` | `24.38%` |
| RocksDB native lib | `10.19%` | `9.18%` | `21.89%` | `21.53%` |
| `serializeValue` | `13.23%` | `27.72%` | `20.97%` | `26.88%` |
| `deserializeList` | `23.39%` | `49.00%` | `37.03%` | `47.47%` |
| `MapSerializer.write` | `10.24%` | `21.46%` | `17.97%` | `23.04%` |
| `MapSerializer.read` | `20.34%` | `42.61%` | `32.34%` | `41.45%` |
| network deserialize | `1.77%` | `0.00%` | `3.50%` | `0.00%` |
| source | `0.00%` | `0.00%` | `0.91%` | `0.00%` |

## 与 state-payload 对比

| 指标 | state-payload Docker max3 | large-state Docker |
|------|---------------------------|--------------------|
| `RocksDBListState.get+add`, 总样本 | `88.00%` | `77.23%` |
| `RocksDBListState.get+add`, processElement 内 | `98.65%` | `98.99%` |
| RocksDB native `get+merge`, 总样本 | `8.49%` | `19.03%` |
| RocksDB native `get+merge`, processElement 内 | `9.51%` | `24.38%` |
| `MapSerializer.read+write`, processElement 内 | `85.16%` | `64.49%` |

结果说明：

- `large-state` 仍然让 `processElement` 内 `RocksDBListState.get+add` 保持在 `98.99%`，状态访问路径没有被 source 或 network 主导。
- RocksDB native `get+merge` 在 Docker `processElement` 内从 `9.51%` 提升到 `24.38%`，端到端总样本从 `8.49%` 提升到 `19.03%`。
- `MapSerializer.read+write` 仍然是主要成本之一，说明新 benchmark 没有通过去掉 `MapRecord` Kryo payload 来提高 RocksDB 比例。
- Docker 总样本里的 `RocksDBListState.get+add` 从 `88.00%` 降到 `77.23%`，主要是 large-state 场景引入了更多 RocksDB native、JIT、容器后台和 runtime 样本；operator 内部状态路径占比仍然接近满占比。

## 结论

本轮结果符合预期：`large-state` benchmark 在保留 `ListState<MapRecord>` 和 Kryo Map payload 的前提下，通过扩大状态规模把 RocksDB native `get/merge` 占比显著拉高，更适合用于观察大状态下 RocksDB/ListState 优化对吞吐的影响。

建议后续使用：

- 想最大化状态访问路径占比：继续用 `get-add-state-payload`
- 想观察大状态 RocksDB native `get/merge` 压力：使用 `large-state`
- 想进一步加压：把 Docker 参数提高到 `--events 100000000 --keys 100000 --largeStateGetEvery 1000 --largeStateClearEvery 1000`
- 想制造更强的长期状态堆积：可尝试 `--largeStateClearEvery 0`，但需要单独评估磁盘、运行时间和 checkpoint 配置风险
