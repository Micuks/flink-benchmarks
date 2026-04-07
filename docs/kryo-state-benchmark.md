# KryoStateBenchmark 设计文档

## 概述

`KryoStateBenchmark` 是一个基于 JMH 的 Flink benchmark，用于复现并量化这样一类热点：在 RocksDB 状态后端上，窗口算子内部使用 `ListState` 保存带 `Map` 字段的 POJO 元素时，元素序列化会走 `PojoSerializer -> KryoSerializer -> MapSerializer` 路径，并在每条记录写入时触发 RocksDB `merge()`。

生产复现 benchmark 的实现位于 `src/main/java/org/apache/flink/benchmark/KryoStateBenchmark.java`。面向 `ListState` 优化收益评估的 direct benchmark 位于 `src/main/java/org/apache/flink/state/benchmark/KryoListStateBenchmark.java`。

当前版本使用 **Processing Time Windows**（5 秒窗口），以便 timer 回调周期性触发 `onProcessingTime`，产生清晰的读路径火焰图。这与生产环境火焰图中的入口一致。

## 相关文档

| 文档 | 内容 |
|------|------|
| `docs/kryo-state-benchmark-usage.md` | MiniCluster JMH、direct state JMH、Docker/Flink job 的推荐运行方式 |
| `docs/kryo-state-job.md` | 外部 Flink 集群 `KryoStateJob` 的详细使用说明 |
| `docs/kryo-window-process-map-pojo-exp-results.md` | `windowProcessMapPojo` 的原始火焰图实验结果 |
| `docs/kryo-get-add-state-payload-benchmark-report.md` | 高 `ListState` 占比 state-payload benchmark 实验报告 |

## 设计目标

- 复现 flame graph 中 `WindowOperator + RocksDB ListState + Kryo MapSerializer` 的热点路径。
- 同时产生**写路径**（merge）和**读路径**（get）两个可区分的调用栈。
- 测量带 `Map` 字段的 POJO 元素写入窗口 `ListState` 时的开销。
- 比较 Kryo 默认模式和注册类型模式的性能差异。
- 提供一个不带 `Map` 字段的 `ListState` 控制组。
- 提供一个 `ReducingState` 路径作为对照。
- 提供一个绕开 MiniCluster/source/keyBy shuffle/window trigger 的 direct `ListState<MapRecord>` benchmark，用于更敏感地评估 `ListState` 优化对吞吐的影响。

## Benchmark 结构

`KryoStateBenchmark` 定义了三个 window benchmark 方法：

### `windowProcessMapPojo`

主 benchmark 路径：

- 输入类型：`MapRecord`（含 `Map<String, Long> counters`，20 entries）
- 窗口 API：`.process(ProcessWindowFunction)`
- 窗口类型：`TumblingProcessingTimeWindows.of(Time.seconds(5))`
- 窗口内部状态：`ListState`
- 状态后端：`RocksDBStateBackend`

设计意图：强制窗口内部使用 `ListState`，让每条 `MapRecord` 都经历 `ListState.add()` → `merge()`，并通过 processing time timer 周期性触发 `onProcessingTime` → `ListState.get()` → `RocksDB.get()`。

### `windowProcessSimple`

控制组路径：

- 输入类型：`SimpleRecord`（无 Map 字段）
- 其余与主路径相同

用于回答：在同样的 `ListState + merge` 路径上，去掉 Map 字段后开销降低多少。

### `windowReduceMapPojo`

对照路径：

- 输入类型：`MapRecord`
- 窗口 API：`.reduce(ReduceFunction)`
- 窗口内部状态：`ReducingState`（read-modify-write: get + reduce + put）

用于比较 `ListState(merge)` 与 `ReducingState(get+put)` 的差异。

### `keyedListStateGetAddMapPojo`

新增的 MiniCluster mixed state benchmark：

- 输入类型：`MapRecord`
- 结构：`source -> keyBy -> KeyedProcessFunction -> discard sink`
- 状态类型：`ListState<MapRecord>`
- 状态访问：每条输入先执行 `ListState.get()` 并遍历，再执行 `ListState.add(MapRecord)`
- 状态规模控制：`getAddStateMaxValuesPerKey` 达到阈值后清理当前 key 的 list，避免读侧状态无限增长

设计意图：在同一个 `processElement` 调用栈里同时包含 `RocksDBListState.get` 和 `RocksDBListState.add`，用于生成一张同时覆盖读写路径的火焰图。它比 `windowProcessMapPojo` 少了 window/timer 触发因素，比 direct `KryoListStateBenchmark` 多保留了 MiniCluster、`keyBy` network 和 task runtime 开销。

### `keyedListStateGetAddBalancedMapPojo`

新增的 MiniCluster balanced mixed state benchmark：

- 输入和拓扑与 `keyedListStateGetAddMapPojo` 相同
- 状态访问仍是每条输入一次 `ListState.get()` 和一次 `ListState.add(MapRecord)`
- 状态规模控制改用 `balancedGetAddStateMaxValuesPerKey`，默认值为 `3`

设计意图：保留旧 mixed benchmark 的同时，把每个 key 的 list 长度上限从 `10` 调低到 `3`，使 `get` 平均只反序列化约 1 个元素，从而让火焰图中的 `RocksDBListState.get` 与 `RocksDBListState.add` 更接近 1:1。该入口适合同时观察读写路径优化收益，旧 `keyedListStateGetAddMapPojo` 继续用于偏读侧的 mixed workload。

### `keyedListStateGetAddStatePayloadMapPojo`

新增的 MiniCluster state-payload mixed state benchmark：

- 输入类型：`SimpleRecord`
- 结构：`source -> keyBy -> KeyedProcessFunction -> discard sink`
- 状态类型：`ListState<MapRecord>`
- 状态访问：每条输入先执行 `ListState.get()` 并遍历，再执行 `ListState.add(MapRecord)`
- 状态 payload：在 operator `open()` 中为每个 key 预构造一条 `MapRecord`，`processElement()` 内写入对应 key 的 payload
- 状态规模控制：复用 `balancedGetAddStateMaxValuesPerKey`，默认值为 `3`

设计意图：保留 RocksDB `ListState<MapRecord>` 的 `PojoSerializer -> KryoSerializer -> MapSerializer` 状态序列化路径，但让 source 和 `keyBy` 网络只处理轻量 `SimpleRecord`。这样可以显著减少 `MapRecord` 在网络反序列化中的 Kryo/MapSerializer 噪声，把火焰图样本更多集中到 `RocksDBListState.get/add`。如果目标是让未来 `ListState` 优化在端到端 Flink job 吞吐中更明显，优先使用这个入口。

### `keyedListStateLargeStateMapPojo`

新增的 MiniCluster large-state benchmark：

- 输入类型：`SimpleRecord`
- 结构：`source -> keyBy -> KeyedProcessFunction -> discard sink`
- 状态类型：`ListState<MapRecord>`
- 状态 payload：仍为带 `Map<String, Long>` 字段的 `MapRecord`，不降低状态侧 Kryo Map 计算量
- 写路径：每条输入都执行 `ListState.add(MapRecord)`，持续制造 RocksDB `merge`
- 读路径：每个 key 累积到 `largeStateGetEvery` 条后执行一次 `ListState.get()` 并遍历，制造较大的 RocksDB `get`
- 状态规模控制：`largeStateClearEvery` 达到阈值后清理当前 key；设置为 `0` 时不主动清理

设计意图：在保留当前 state-payload benchmark 的基础上，通过更大的 key 空间、更高的 `largeStateClearEvery` 和稀疏大读来增加 RocksDB 工作集，而不是通过降低 `MapRecord` 序列化成本来提高 `RocksDB.get/merge` 相对占比。推荐大状态场景使用更多 keys，例如 Docker 100M 事件下使用 `--keys 100000 --largeStateGetEvery 1000 --largeStateClearEvery 1000`，峰值状态量接近 `keys * largeStateClearEvery` 条 MapRecord。

### `KryoListStateBenchmark`

新增的 direct state benchmark：

- 状态类型：`ListState<MapRecord>`
- 状态后端：继承 `StateBenchmarkBase`，通过 `-p backendType=ROCKSDB` 选择 RocksDB
- 写路径：`listAddMapPojo` 直接执行 `setCurrentKey` → `ListState.add(MapRecord)`
- 读路径：`listGetAndIterateMapPojo` 直接执行 `setCurrentKey` → `ListState.get()` → 遍历返回值
- 混合路径：`listGetAndAddMapPojo` 先执行 `ListState.get()` 并遍历，再对另一组 key 执行一批 `ListState.add(MapRecord)`，用于在一张火焰图里同时观察 get 和 add
- 默认参数：`numberOfKeys=10`, `mapEntries=100`, `valuesPerKey=1000`, `recordPoolSize=1024`, `addsPerGet=100`

设计意图：保留 `MapRecord` 的 `PojoSerializer -> KryoSerializer -> MapSerializer` 状态序列化路径，但去掉现有火焰图中 30%+ 的 `keyBy` network 序列化反序列化、7%~10% 的 source `HashMap` 构造，以及 window/timer/mailbox 框架开销。这个 benchmark 更适合回答“优化 `RocksDBListState` 后吞吐能提升多少”；`windowProcessMapPojo` 更适合回答“生产火焰图能否复现”。

## 数据模型

### `MapRecord`

```java
public static class MapRecord implements Serializable {
    public int key;
    public long value;
    public Map<String, Long> counters;  // 20 entries → Kryo MapSerializer
}
```

`counters` 字段是设计关键。Flink `TypeExtractor` 推断 `Map` 字段为 `GenericTypeInfo` → 回退到 `KryoSerializer` → `MapSerializer`。每条记录的 Map 包含 20 个 `"field_0"` ~ `"field_19"` 的 entry，确保足够的序列化权重。

### `SimpleRecord`

```java
public static class SimpleRecord implements Serializable {
    public int key;
    public long value;
}
```

不包含嵌套 Map，不触发 Kryo 回退。

## 执行配置

| 参数 | 值 | 来源 |
|------|------|------|
| `RECORDS_PER_INVOCATION` | 1,000,000 | 类常量 |
| `numberOfKeys` | 1,000 | `KryoStateContext` JMH `@Param` |
| Window | `TumblingProcessingTimeWindows.of(Time.seconds(5))` | benchmark 方法 |
| `mapEntries` | 20 | `KryoStateContext` JMH `@Param` |
| `reuseSourceRecords` | false | `KryoStateContext` JMH `@Param` |
| `getAddStateMaxValuesPerKey` | 10 | mixed get/add benchmark 的每 key list 长度上限 |
| `balancedGetAddStateMaxValuesPerKey` | 3 | balanced 和 state-payload mixed get/add benchmark 的每 key list 长度上限 |
| `largeStateGetEvery` | 1000 | large-state benchmark 每个 key 多少次 add 后执行一次 get |
| `largeStateClearEvery` | 1000 | large-state benchmark 每个 key 的清理阈值，`0` 表示不主动清理 |
| `backendMode` | `ROCKS`, `ROCKS_KRYO_REG`, `FALCON`, `FALCON_CACHE` | JMH `@Param` |
| Parallelism | 1 | `FlinkEnvironmentContext` |

`reuseSourceRecords=true` 时，`MapRecordSource` 会为每个 key 预构造一条 `MapRecord` 并反复发送，减少 source 侧 `HashMap` 和字符串构造成本。该开关用于提高 window benchmark 中 `ListState` 路径的相对占比；如果目标是严格复现每条输入都新建 payload 的生产形态，应保持默认 `false`。

## BackendMode 参数

| 模式 | 说明 |
|------|------|
| `ROCKS` | 默认 RocksDB，Kryo 未注册（reference tracking on）|
| `ROCKS_KRYO_REG` | RocksDB + `registerKryoType(HashMap, MapRecord, SimpleRecord)` |
| `FALCON` | OmniStateStore (Falcon)，需要 `flink-alg-falcon.jar` |
| `FALCON_CACHE` | Falcon + ValueState cache，需要 `libfalcon.so` |

## 运行方法

运行命令集中维护在 `docs/kryo-state-benchmark-usage.md`，外部 Flink 集群的详细操作集中维护在 `docs/kryo-state-job.md`。

## 环境要求

- **JDK 11**（Flink 1.16 不兼容 JDK 24；async-profiler 3.0 不兼容 JDK 24）
- **async-profiler 4.0+**（3.0 在 JDK 24 上 SIGSEGV；在 JDK 11 上建议用 4.0）
- `perf_event_paranoid=4` 时使用 `event=itimer`（不支持硬件 perf events）

## 已知局限

### 被测内容是混合开销

最终 throughput 混合了 stream graph 执行、source、windowing、process/reduce 函数逻辑、backend、序列化等多类成本。适合复现真实热点，不适合精确分离单一组件成本。

已有 collapsed stack 结果显示，`windowProcessMapPojo` 会混入较多 source、network 和 window runtime 开销。因此继续只调大 `mapEntries` 或调小 `numberOfKeys`，很难让 window benchmark 对 `ListState` 优化保持线性敏感。具体实验数值见 `docs/flamegraph-tuning-report.md` 和 `docs/kryo-window-process-map-pojo-exp-results.md`。

如果目标是验证 `ListState` 内部优化的吞吐收益，优先使用 `KryoListStateBenchmark`。如果目标是端到端 Flink job 中尽量放大 `ListState` 优化收益，使用 `KryoStateBenchmark.keyedListStateGetAddStatePayloadMapPojo`。如果目标是验证生产调用栈和 timer 触发路径，继续使用 `KryoStateBenchmark.windowProcessMapPojo`。

### 环境复用风险

`KryoStateContext` 在 `@Setup` 中构造 `StreamExecutionEnvironment`，benchmark 方法在其上追加算子并调用 `env.execute()`。如果后续需要更严谨的测量，应在每次 invocation 中独立构造 job graph。

### JMH 生成类缺失

如果使用 JDK 24 构建，可能看不到 JMH 生成类和 `META-INF/BenchmarkList`、`META-INF/CompilerHints`。优先切换到 JDK 11 重新构建；只有在 shaded jar 仍缺少这些资源时，才需要按上面的手工注入步骤修复。

## 总结

`KryoStateBenchmark` 通过 Processing Time 5s Window + `ProcessWindowFunction` 强制使用 `ListState`，成功复现了两条独立的火焰图调用栈：

1. **写路径**：`processInput → WindowOperator.processElement → serializeValue(PojoSer→KryoSer→MapSerializer.write) → RocksDB.merge`
2. **读路径**：`processMailsNonBlocking → onProcessingTime → RocksDBListState.getInternal → RocksDB.get(DBImpl::Get→BlockBasedTable::Get→snappy) + deserializeList(PojoSer→KryoSer→MapSerializer.read)`

具体火焰图比例和吞吐结果不在设计文档中维护，见 `docs/kryo-window-process-map-pojo-exp-results.md`、`docs/kryo-list-state-benchmark-report.md`、`docs/kryo-get-add-benchmark-report.md`、`docs/kryo-get-add-balanced-benchmark-report.md` 和 `docs/kryo-get-add-state-payload-benchmark-report.md`。
