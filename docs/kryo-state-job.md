# KryoStateJob 使用说明

`KryoStateJob` 是 `KryoStateBenchmark` 的普通 Flink job 入口，用于在外部 Flink 集群上运行同一类 `WindowOperator + RocksDB ListState + Kryo MapSerializer` workload。

它和 `KryoStateBenchmark` 的区别是：`KryoStateBenchmark` 是 JMH 入口，会在进程内启动 MiniCluster；`KryoStateJob` 使用 `StreamExecutionEnvironment.getExecutionEnvironment()`，应通过 `flink run` 提交到已经启动的 `flink-baseline` 或 `flink-falcon` 集群。

## 默认 workload

默认参数参考 Nexmark 100M workload：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `events` | `100000000` | 全局总事件数，不是每个 source subtask 的事件数 |
| `eventsPerSecond` | `10000000` | 全局 source 速率，按 source 并行度均分 |
| `parallelism` | `16` | job 默认并行度 |
| `sourceParallelism` | `16` | source 并行度，默认等于 `parallelism` |
| `keys` | `1000` | key 数量 |
| `mapEntries` | `20` | 每条 `MapRecord.counters` 的 entry 数 |
| `windowSeconds` | `5` | Processing Time tumbling window 大小 |
| `mode` | `map` | 主路径，等价于 JMH 的 `windowProcessMapPojo` |

source 会在运行时按实际 source 并行度切分事件范围。例如 `events=100000000` 且 `sourceParallelism=16` 时，每个 source subtask 约发 `6250000` 条记录；`eventsPerSecond=10000000` 会被均分为每个 source subtask 约 `625000 events/s`。

## 集群资源要求

在当前 docker-compose 环境中，如果每个 TaskManager container 启动 4 个 TaskManager 进程，并且 `taskmanager.numberOfTaskSlots: 2`，则总 slot 数为：

```text
2 taskmanager containers * 4 TaskManager processes/container * 2 slots/process = 16 slots
```

此时可以使用 `parallelism=16`。如果实际环境只是每个 TaskManager container 总共 4 个 slot，则总 slot 数只有 8，需要降低并行度或增加 slot 数。

## 构建

在 `flink-benchmarks` 目录构建：

```bash
mvn -DskipTests package
```

产物为：

```bash
target/benchmarks.jar
```

## 提交到 baseline 集群

baseline 集群使用普通 RocksDB 配置。把 jar 放到能执行 `flink run` 的环境后提交：

```bash
flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  target/benchmarks.jar \
  --mode map \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 10000000 \
  --keys 1000 \
  --mapEntries 20
```

如果在 host 上通过 docker-compose 管理集群，可先把 jar 拷贝到 JobManager container，再在 JobManager container 内运行上述 `flink run`。也可以使用与现有 Nexmark 脚本相同的提交方式，只要 class 指向 `org.apache.flink.benchmark.KryoStateJob`。

## 提交到 Falcon 集群

Falcon 集群侧应通过 `flink-conf.yaml` 或容器镜像启用 Falcon 相关配置和 native library。job 本身不强制设置 backend，避免把 client 侧临时路径或 JMH MiniCluster 配置带入外部集群。

提交命令与 baseline 相同：

```bash
flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  target/benchmarks.jar \
  --mode map \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 10000000 \
  --keys 1000 \
  --mapEntries 20
```

如果需要显式注册 Kryo 类型：

```bash
flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  target/benchmarks.jar \
  --mode map \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 10000000 \
  --registerKryo true
```

## 模式

`--mode map`：主 workload。输入为带 `Map<String, Long>` 字段的 `MapRecord`，窗口使用 `.process()`，内部走 `ListState`，用于观察 `PojoSerializer -> KryoSerializer -> MapSerializer` 和 RocksDB `merge/get` 路径。

`--mode simple`：控制组。输入为不带 `Map` 字段的 `SimpleRecord`，仍使用 `.process()` 和 `ListState`，用于对比去掉 Kryo Map fallback 之后的开销。

`--mode reduce`：对照组。输入仍为 `MapRecord`，窗口使用 `.reduce()`，内部走 `ReducingState`，用于和 `ListState(merge)` 路径对比。

## 常用调参

提高记录数：

```bash
--events 200000000
```

关闭 source 限速，让 source 尽快发完：

```bash
--eventsPerSecond 0
```

增加单条记录的 Kryo Map 序列化权重：

```bash
--mapEntries 100
```

改变 key 数：

```bash
--keys 10000
```

使用可复用 source 对象来减少 source 侧对象构造成本：

```bash
--reuseSourceRecords true
```

## 火焰图注意事项

分布式模式下，`flink run` client 进程不是主要被测进程。要观察 RocksDB/Kryo 热点，应 attach async-profiler 到 TaskManager JVM，或在 TaskManager JVM 参数中预置 async-profiler agent。

如果只在提交命令上加 `-jvmArgs` 或 profile client 进程，通常只能看到 job graph 构建和提交开销，看不到主要的 `WindowOperator`、`RocksDBListState`、`KryoSerializer` 路径。
