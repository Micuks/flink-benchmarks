# KryoStateJob 使用说明

`KryoStateJob` 是 `KryoStateBenchmark` 的普通 Flink job 入口，用于在外部 Flink 集群上运行同一类 `WindowOperator + RocksDB ListState + Kryo MapSerializer` workload。

它和 `KryoStateBenchmark` 的区别是：`KryoStateBenchmark` 是 JMH 入口，会在进程内启动 MiniCluster；`KryoStateJob` 使用 `StreamExecutionEnvironment.getExecutionEnvironment()`，应通过 `flink run` 提交到已经启动的 `flink-baseline` 或 `flink-falcon` 集群。

本文件只维护外部 Flink 集群的使用流程。实验结果见对应 `*-benchmark-report.md` 或 `*-exp-results.md` 文档；推荐入口和常用命令汇总见 `docs/kryo-state-benchmark-usage.md`。

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
| `maxValuesPerKey` | `10` | `--mode get-add` 和 `--mode get-add-state-payload` 下每个 key 的 `ListState` 长度上限；`--mode get-add-balanced` 默认使用 `3` |
| `largeStateGetEvery` | `1000` | `--mode large-state` 下每个 key 累积多少次 add 后执行一次 `ListState.get()` |
| `largeStateClearEvery` | `1000` | `--mode large-state` 下每个 key 的清理阈值，`0` 表示不主动清理 |
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

在 `flink-cluster` 根目录构建：

```bash
cd /mnt/data2/wuql/flink-cluster
cd flink-benchmarks
mvn -DskipTests package
cd ..
```

产物为：

```bash
target/benchmarks.jar
```

## 从启动 Docker 到运行 RocksDB

以下命令从 `flink-cluster` 根目录执行。baseline 集群使用 `flink-baseline:latest` 镜像和普通 RocksDB 配置。

```bash
cd /mnt/data2/wuql/flink-cluster

# 1. 选择 RocksDB 配置。docker-compose 会把 config/flink-conf.yaml 挂载到容器内。
cp config/flink-conf-rocksdb.yaml config/flink-conf.yaml

# 2. 启动 baseline 集群：1 个 JobManager container + 2 个 TaskManager container。
docker compose -f docker-compose-baseline.yml down --remove-orphans -t 0
docker compose -f docker-compose-baseline.yml up -d --remove-orphans

# 3. 等待 TaskManager 注册。当前 entrypoint 每个 TM container 启动 4 个 TM 进程，
#    配置里每个 TM 进程 2 slots，所以期望看到 8 个 TaskManagers / 16 slots。
curl -s http://localhost:8481/overview

# 4. 构建并拷贝 job jar 到 JobManager container。
cd flink-benchmarks
mvn -DskipTests package
cd ..
docker compose -f docker-compose-baseline.yml cp \
  flink-benchmarks/target/benchmarks.jar \
  jobmanager:/opt/benchmarks.jar

# 5. 提交 benchmark。
docker compose -f docker-compose-baseline.yml exec jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode map \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 10000000 \
  --keys 1000 \
  --mapEntries 20
```

Dashboard: `http://localhost:8481`。

## 从启动 Docker 到运行 OmniStateStore

OmniStateStore/Falcon 由集群侧配置启用。`KryoStateJob` 不在代码里强制设置 backend，避免把 client 侧临时路径或 JMH MiniCluster 配置带入外部集群。

### OmniStateStore no-cache

```bash
cd /mnt/data2/wuql/flink-cluster

# 1. 选择 Falcon no-cache 配置。
cp config/flink-conf-falcon-nocache.yaml config/flink-conf.yaml

# 2. 启动 Falcon 集群。这里以 unit23 为例；unit1/unit4 同理，只是端口不同。
docker compose -f docker-compose-unit23.yml down --remove-orphans -t 0
docker compose -f docker-compose-unit23.yml up -d --remove-orphans

# 3. 检查注册情况。unit23 的 REST 端口映射为 8281。
curl -s http://localhost:8281/overview

# 4. 构建并拷贝 job jar。
cd flink-benchmarks
mvn -DskipTests package
cd ..
docker compose -f docker-compose-unit23.yml cp \
  flink-benchmarks/target/benchmarks.jar \
  jobmanager:/opt/benchmarks.jar

# 5. 提交 benchmark。--registerKryo true 对齐 JMH 中非 baseline 模式的注册行为。
docker compose -f docker-compose-unit23.yml exec jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode map \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 10000000 \
  --keys 1000 \
  --mapEntries 20 \
  --registerKryo true
```

Dashboard: `http://localhost:8281`。

### OmniStateStore cache

```bash
cd /mnt/data2/wuql/flink-cluster

# 1. 选择 Falcon cache 配置。
cp config/flink-conf-falcon-cache.yaml config/flink-conf.yaml

# 2. 重启 Falcon 集群，让新配置生效。
docker compose -f docker-compose-unit23.yml down --remove-orphans -t 0
docker compose -f docker-compose-unit23.yml up -d --remove-orphans
curl -s http://localhost:8281/overview

# 3. jar 如果已经拷贝过且未重新构建，可以跳过 cp；否则重新拷贝。
docker compose -f docker-compose-unit23.yml cp \
  flink-benchmarks/target/benchmarks.jar \
  jobmanager:/opt/benchmarks.jar

# 4. 提交 benchmark。
docker compose -f docker-compose-unit23.yml exec jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode map \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 10000000 \
  --registerKryo true
```

确认是否启用 OmniStateStore 的最直接方式是查看容器内 `flink-conf.yaml`：

```bash
docker compose -f docker-compose-unit23.yml exec jobmanager \
  grep -E 'state.backend.rocksdb.options-factory|state.backend.rocksdb.falcon.use-state-cache' \
  /opt/flink/conf/flink-conf.yaml
```

期望看到 `com.huawei.falcon.state.RocksDBOptOptionsFactory`。cache 模式下 `state.backend.rocksdb.falcon.use-state-cache` 应为 `true`，no-cache 模式下应为 `false`。

## 模式

`--mode map`：主 workload。输入为带 `Map<String, Long>` 字段的 `MapRecord`，窗口使用 `.process()`，内部走 `ListState`，用于观察 `PojoSerializer -> KryoSerializer -> MapSerializer` 和 RocksDB `merge/get` 路径。

`--mode simple`：控制组。输入为不带 `Map` 字段的 `SimpleRecord`，仍使用 `.process()` 和 `ListState`，用于对比去掉 Kryo Map fallback 之后的开销。

`--mode reduce`：对照组。输入仍为 `MapRecord`，窗口使用 `.reduce()`，内部走 `ReducingState`，用于和 `ListState(merge)` 路径对比。

`--mode get-add`：混合读写组。输入为 `MapRecord`，使用 `keyBy -> KeyedProcessFunction`，每条输入在同一个 `processElement` 里先执行 `ListState.get()` 并遍历，再执行 `ListState.add(MapRecord)`。`--maxValuesPerKey` 控制每个 key 的 list 长度，适合生成同一张火焰图里同时包含 `RocksDBListState.get` 和 `RocksDBListState.add` 的 profile。

`--mode get-add-balanced`：balanced 混合读写组。拓扑与 `get-add` 相同，但默认 `--maxValuesPerKey` 为 `3`，让 get 平均只反序列化约 1 个元素，从而使火焰图中的 `RocksDBListState.get` 与 `RocksDBListState.add` 更接近 1:1。如果显式传入 `--maxValuesPerKey`，会覆盖该默认值。

`--mode get-add-state-payload`：高 `ListState` 占比混合读写组。输入和 `keyBy` 网络使用轻量 `SimpleRecord`，但 `KeyedProcessFunction` 内部的状态仍是 `ListState<MapRecord>`，并在 operator `open()` 中为每个 key 预构造 `MapRecord` payload。该模式保留状态侧 `PojoSerializer -> KryoSerializer -> MapSerializer`，同时避免 `MapRecord` 在网络反序列化中抢占火焰图样本。推荐用 `--mapEntries 100` 放大状态 payload；用 `--maxValuesPerKey 3` 获取相对均衡的 get/add，占比更高但偏读时可用默认 `10`。

`--mode large-state`：大状态 RocksDB mixed 组。输入和 `keyBy` 网络仍使用轻量 `SimpleRecord`，但状态 payload 是 `ListState<MapRecord>`，不降低状态侧 Kryo Map 计算量。每条输入都执行 `ListState.add(MapRecord)`；每个 key 累积到 `--largeStateGetEvery` 后执行一次 `ListState.get()` 并遍历；达到 `--largeStateClearEvery` 后清理当前 key。该模式用于通过更大的 key 空间和更高的状态保留阈值增加 RocksDB 工作集，从而提高 `RocksDB.get/merge` 本身耗时，而不是通过削弱其他状态侧计算来提高占比。

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

调整 mixed get/add benchmark 的每 key list 长度上限：

```bash
--maxValuesPerKey 10
```

运行 large-state benchmark，让 100M 事件形成接近 `100000 * 1000` 条 `MapRecord` 的峰值状态规模：

```bash
--mode large-state \
--events 100000000 \
--eventsPerSecond 0 \
--keys 100000 \
--mapEntries 20 \
--largeStateGetEvery 1000 \
--largeStateClearEvery 1000
```

## 火焰图注意事项

分布式模式下，`flink run` client 进程不是主要被测进程。要观察 RocksDB/Kryo 热点，应 attach async-profiler 到 TaskManager JVM，或在 TaskManager JVM 参数中预置 async-profiler agent。

如果只在提交命令上加 `-jvmArgs` 或 profile client 进程，通常只能看到 job graph 构建和提交开销，看不到主要的 `WindowOperator`、`RocksDBListState`、`KryoSerializer` 路径。

## 抓取 TaskManager 火焰图

镜像里已安装 async-profiler 4.0，路径为 `/opt/async-profiler/bin/asprof`。下面示例从 `flink-cluster` 根目录执行，对所有 TaskManager JVM 采集 CPU 火焰图。

如果宿主机/容器没有 perf 权限，可把 `PROFILE_EVENT=cpu` 改成 `PROFILE_EVENT=itimer` 或 `PROFILE_EVENT=wall`。

### 1. 启动 profiler

在提交 job 前启动 profiler：

```bash
cd /mnt/data2/wuql/flink-cluster

# baseline 使用 docker-compose-baseline.yml；OmniStateStore/Falcon 使用 docker-compose-unit23.yml。
COMPOSE=docker-compose-unit23.yml
PROFILE_EVENT=cpu

for container in $(docker compose -f "$COMPOSE" ps -q taskmanager1 taskmanager2); do
  for pid in $(docker exec "$container" jps -l | awk '/TaskManagerRunner/ {print $1}'); do
    docker exec "$container" /opt/async-profiler/bin/asprof start -e "$PROFILE_EVENT" "$pid"
  done
done
```

### 2. 运行 benchmark

在 profiler 运行期间提交 job，例如 OmniStateStore no-cache：

```bash
docker compose -f "$COMPOSE" exec jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode map \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 10000000 \
  --keys 1000 \
  --mapEntries 20 \
  --registerKryo true
```

如果 job 太快结束导致样本不足，可增大 `--events`，降低 `--eventsPerSecond`，或在更重配置下运行，例如 `--mapEntries 100`。

### 3. 导出 html 与 collapsed stacks

job 完成或采样时间足够后导出结果：

async-profiler 4.0 可按 `-f` 文件后缀推断输出格式，下面命令不额外传 `-o`，避免不同版本对 `-o html` / `-o collapsed` 的兼容性差异。

```bash
mkdir -p flamegraphs/kryo-state

for container in $(docker compose -f "$COMPOSE" ps -q taskmanager1 taskmanager2); do
  container_name=$(docker inspect --format '{{.Name}}' "$container" | tr -d '/')
  docker exec "$container" mkdir -p /opt/flamegraphs

  for pid in $(docker exec "$container" jps -l | awk '/TaskManagerRunner/ {print $1}'); do
    base="kryo-state-${PROFILE_EVENT}-${container_name}-pid${pid}"

    docker exec "$container" /opt/async-profiler/bin/asprof dump \
      -f "/opt/flamegraphs/${base}.html" \
      "$pid"

    docker exec "$container" /opt/async-profiler/bin/asprof dump \
      -f "/opt/flamegraphs/${base}.collapsed" \
      "$pid"

    docker exec "$container" /opt/async-profiler/bin/asprof stop "$pid"

    docker cp "$container:/opt/flamegraphs/${base}.html" \
      "flamegraphs/kryo-state/${base}.html"

    docker cp "$container:/opt/flamegraphs/${base}.collapsed" \
      "flamegraphs/kryo-state/${base}.collapsed"
  done
done
```

输出会落到：

```text
flamegraphs/kryo-state/
```

### 4. 常见问题

如果 `asprof start -e cpu` 报 perf 权限问题，改用：

```bash
PROFILE_EVENT=itimer
```

如果没有找到 `TaskManagerRunner` PID，先确认 TaskManager container 内实际进程：

```bash
docker compose -f "$COMPOSE" exec taskmanager1 jps -l
docker compose -f "$COMPOSE" exec taskmanager2 jps -l
```

如果 `dump` 失败但 profiler 已启动，先停止会话再重新采集：

```bash
for container in $(docker compose -f "$COMPOSE" ps -q taskmanager1 taskmanager2); do
  for pid in $(docker exec "$container" jps -l | awk '/TaskManagerRunner/ {print $1}'); do
    docker exec "$container" /opt/async-profiler/bin/asprof stop "$pid" || true
  done
done
```
