# Kryo get+add state-payload benchmark report

## 目的

提高端到端 Kryo/RocksDB mixed benchmark 中 `RocksDBListState.get/add` 的火焰图占比，让未来 `ListState` 优化在 throughput 和 flamegraph 上更容易观察。

之前的 balanced mixed benchmark 使用 `MapRecord` 作为 source 和 `keyBy` 输入，虽然 `processElement` 内 get/add 接近均衡，但 Docker collapsed stack 的总样本里 `RocksDBListState.get+add` 只有 `51.47%`，`keyBy` network 反序列化约 `25.75%`，source 约 `9.03%`。这会稀释 `ListState` 优化对端到端吞吐的影响。

## 新入口

新增 `KryoStateBenchmark.keyedListStateGetAddStatePayloadMapPojo` 和 `KryoStateJob --mode get-add-state-payload`。

核心设计：

- source 和 `keyBy` 使用轻量 `SimpleRecord`
- `KeyedProcessFunction` 内部状态仍是 `ListState<MapRecord>`
- operator `open()` 中为每个 key 预构造 `MapRecord` payload
- 每条输入执行 `ListState.get()` 并遍历，再执行 `ListState.add(MapRecord)`
- `mapEntries` 继续控制状态 payload 的 Kryo `MapSerializer` 权重
- `maxValuesPerKey` 控制每个 key 的 list 长度，`3` 更均衡，`10` 更偏读且 `ListState` 总占比更高

这个设计保留状态侧 `PojoSerializer -> KryoSerializer -> MapSerializer`，但避免 `MapRecord` 在网络反序列化里重复贡献 Kryo/MapSerializer 样本。

## 关联文档

运行命令不放在实验报告里维护，统一见 `docs/kryo-state-benchmark-usage.md`。

## MiniCluster 实验

配置与结果：

| 项目 | 值 |
|------|----|
| JMH 入口 | `KryoStateBenchmark.keyedListStateGetAddStatePayloadMapPojo` |
| backendMode | `ROCKS` |
| numberOfKeys | `1000` |
| mapEntries | `100` |
| balancedGetAddStateMaxValuesPerKey | `3` |
| async-profiler | `event=itimer` |
| collapsed run | `14.214 ops/ms` |
| HTML run | `14.620 ops/ms` |
| 火焰图 | `docs/flamegraphs/kryo-get-add-state-payload/minicluster/keyedListStateGetAddStatePayloadMapPojo_ROCKS_map100.html` |
| collapsed stack | `docs/flamegraphs/kryo-get-add-state-payload/minicluster/keyedListStateGetAddStatePayloadMapPojo_ROCKS_map100.collapsed` |

## Docker 实验

环境：

- `docker-compose-baseline.yml`
- `flink-baseline:latest`
- 1 个 JobManager container
- 2 个 TaskManager containers
- 每个 TaskManager container 4 个 `TaskManagerRunner`
- job parallelism `16`

结果：

| 运行 | 参数 | 结果 | 火焰图目录 |
|------|------|------|------------|
| balanced state-payload | `events=5000000`, `mapEntries=100`, `maxValuesPerKey=3` | `30.500 s`, 约 `163.93 K events/s` | `docs/flamegraphs/kryo-get-add-state-payload/docker-max3` |
| read-heavy state-payload | `events=10000000`, `mapEntries=100`, `maxValuesPerKey=10` | `154.681 s`, 约 `64.65 K events/s` | `docs/flamegraphs/kryo-get-add-state-payload/docker` |

## 火焰图统计

以下统计来自 collapsed stack。`总样本` 表示端到端 profile，`processElement 内` 表示排除 source、network、JIT 和容器后台线程后的 operator 内占比。

| 指标 | MiniCluster max3 总样本 | MiniCluster max3 processElement 内 | Docker max3 总样本 | Docker max3 processElement 内 | Docker max10 总样本 | Docker max10 processElement 内 |
|------|--------------------------|------------------------------------|--------------------|-------------------------------|---------------------|--------------------------------|
| `processElement` | `80.78%` | `100.00%` | `89.19%` | `100.00%` | `96.65%` | `100.00%` |
| `RocksDBListState.get` | `45.85%` | `56.76%` | `54.98%` | `61.64%` | `83.39%` | `86.28%` |
| `RocksDBListState.add` | `33.96%` | `42.04%` | `33.02%` | `37.01%` | `13.00%` | `13.45%` |
| `RocksDBListState.get+add` | `79.82%` | `98.80%` | `88.00%` | `98.65%` | `96.39%` | `99.73%` |
| RocksDB native `get` | `4.32%` | `5.35%` | `3.59%` | `4.02%` | `2.64%` | `2.73%` |
| RocksDB native `merge` | `4.24%` | `5.25%` | `4.90%` | `5.49%` | `1.97%` | `2.04%` |
| `deserializeList` | `41.51%` | `51.38%` | `51.35%` | `57.57%` | `80.74%` | `83.53%` |
| `serializeValue` | `29.69%` | `36.75%` | `28.04%` | `31.42%` | `10.95%` | `11.33%` |
| `MapSerializer.read` | `39.47%` | `48.86%` | `49.07%` | `55.02%` | `77.58%` | `80.27%` |
| `MapSerializer.write` | `27.85%` | `34.47%` | `26.88%` | `30.14%` | `10.53%` | `10.89%` |
| network deserialize | `0.96%` | `0.00%` | `1.08%` | `0.00%` | `0.39%` | `0.00%` |
| source | `0.00%` | `0.00%` | `0.37%` | `0.00%` | `0.09%` | `0.00%` |

## 结论

`state-payload` 入口能显著提高端到端 profile 中 `ListState` 操作占比：

- 旧 balanced Docker：`RocksDBListState.get+add = 51.47%`
- 新 state-payload Docker max3：`RocksDBListState.get+add = 88.00%`
- 新 state-payload Docker max10：`RocksDBListState.get+add = 96.39%`

结果解读：

- `maxValuesPerKey=3` 更适合作为 get/add 相对均衡的端到端 profile。
- `maxValuesPerKey=10` 更适合最大化端到端火焰图里的 `ListState` 总占比，但结果更偏读。
- pure state throughput gate 仍应看 direct `KryoListStateBenchmark`。
- 生产 window/timer 调用栈复现仍应看 `windowProcessMapPojo` 或 `KryoStateJob --mode map`。

取舍：`state-payload` 不再让网络传输 `MapRecord`，因此它比 `get-add-balanced` 少了网络 Kryo Map 反序列化噪声。这个取舍是有意的，目标是让 future `ListState` 优化的收益在端到端 benchmark 中更明显。
