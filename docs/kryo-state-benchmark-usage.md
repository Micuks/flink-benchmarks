# Kryo state benchmark usage

## 选择入口

| 目标 | 推荐入口 |
|------|----------|
| 复现生产 window/timer 调用栈 | `KryoStateBenchmark.windowProcessMapPojo` 或 `KryoStateJob --mode map` |
| 端到端 Flink job 中放大 `ListState` 优化收益 | `KryoStateBenchmark.keyedListStateGetAddStatePayloadMapPojo` 或 `KryoStateJob --mode get-add-state-payload` |
| 大状态场景中提高 RocksDB native get/merge 耗时 | `KryoStateBenchmark.keyedListStateLargeStateMapPojo` 或 `KryoStateJob --mode large-state` |
| 同一张火焰图里同时观察 get/add 且保持相对均衡 | `--mode get-add-state-payload --mapEntries 100 --maxValuesPerKey 3` |
| 最大化端到端火焰图里的 `ListState.get/add` 占比 | `--mode get-add-state-payload --mapEntries 100 --maxValuesPerKey 10` |
| 纯状态访问 throughput gate | `KryoListStateBenchmark.listAddMapPojo`、`listGetAndIterateMapPojo`、`listGetAndAddMapPojo` |

## 环境要求

- JDK 11。Flink 1.16 不兼容 JDK 24；JDK 24 下也可能缺少 JMH annotation processor 生成资源。
- async-profiler 4.0+。如果宿主机 perf 权限不足，使用 `event=itimer`。
- 外部 Flink 集群的完整启动、提交和 TaskManager profiler 操作见 `docs/kryo-state-job.md`。

## 构建

```bash
export JAVA_HOME=/mnt/data1/wuql/.sdkman/candidates/java/11.0.26-amzn
export PATH="${JAVA_HOME}/bin:${PATH}"

mvn -DskipTests package
java -jar target/benchmarks.jar -l | rg 'Kryo(State|ListState)Benchmark'
```

如果 `target/benchmarks.jar -l` 看不到 JMH 入口，优先确认使用的是 JDK 11 重新构建。

## MiniCluster JMH

复现生产 window/timer 路径：

```bash
java -jar target/benchmarks.jar "KryoStateBenchmark.windowProcessMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p reuseSourceRecords=true \
  -f 1 -wi 1 -i 3
```

端到端 MiniCluster state-payload 路径：

```bash
java -jar target/benchmarks.jar "KryoStateBenchmark.keyedListStateGetAddStatePayloadMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=1000 \
  -p mapEntries=100 \
  -p balancedGetAddStateMaxValuesPerKey=3 \
  -f 1 -wi 1 -i 3
```

端到端 MiniCluster large-state 路径：

```bash
java -jar target/benchmarks.jar "KryoStateBenchmark.keyedListStateLargeStateMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=1000 \
  -p mapEntries=20 \
  -p largeStateGetEvery=1000 \
  -p largeStateClearEvery=1000 \
  -f 1 -wi 1 -i 3
```

direct `ListState` 写/读路径：

```bash
java -jar target/benchmarks.jar "KryoListStateBenchmark.listAddMapPojo|KryoListStateBenchmark.listGetAndIterateMapPojo" \
  -p backendType=ROCKSDB \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p valuesPerKey=1000 \
  -f 1 -wi 1 -i 3
```

direct `ListState` get+add 混合路径：

```bash
java -jar target/benchmarks.jar "KryoListStateBenchmark.listGetAndAddMapPojo" \
  -p backendType=ROCKSDB \
  -p numberOfKeys=10 \
  -p mapEntries=100 \
  -p valuesPerKey=100 \
  -p addsPerGet=100 \
  -f 1 -wi 1 -i 3
```

## MiniCluster 火焰图

生成 HTML：

```bash
AP_LIB=/mnt/data1/wuql/dev/FlinkLargeStateTuning/flink-cluster/async-profiler-4.0-linux-x64/lib/libasyncProfiler.so
OUT=docs/flamegraphs/kryo-get-add-state-payload/minicluster/keyedListStateGetAddStatePayloadMapPojo_ROCKS_map100.html

java -jar target/benchmarks.jar "KryoStateBenchmark.keyedListStateGetAddStatePayloadMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=1000 \
  -p mapEntries=100 \
  -p balancedGetAddStateMaxValuesPerKey=3 \
  -f 1 -wi 0 -i 1 \
  -jvmArgs "-agentpath:${AP_LIB}=start,event=itimer,file=${OUT}"
```

生成 collapsed stack：

```bash
AP_LIB=/mnt/data1/wuql/dev/FlinkLargeStateTuning/flink-cluster/async-profiler-4.0-linux-x64/lib/libasyncProfiler.so
OUT=docs/flamegraphs/kryo-get-add-state-payload/minicluster/keyedListStateGetAddStatePayloadMapPojo_ROCKS_map100.collapsed

java -jar target/benchmarks.jar "KryoStateBenchmark.keyedListStateGetAddStatePayloadMapPojo" \
  -p backendMode=ROCKS \
  -p numberOfKeys=1000 \
  -p mapEntries=100 \
  -p balancedGetAddStateMaxValuesPerKey=3 \
  -f 1 -wi 0 -i 1 \
  -jvmArgs "-agentpath:${AP_LIB}=start,event=itimer,collapsed,file=${OUT}"
```

## Docker/Flink Job

外部 Flink 集群只通过 `KryoStateJob` 入口运行。完整启动、拷贝 jar、抓 TaskManager 火焰图流程见 `docs/kryo-state-job.md`。

baseline RocksDB state-payload，get/add 相对均衡：

```bash
docker compose -f docker-compose-baseline.yml exec -T jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode get-add-state-payload \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 5000000 \
  --eventsPerSecond 0 \
  --keys 1000 \
  --mapEntries 100 \
  --maxValuesPerKey 3
```

baseline RocksDB state-payload，最大化 `ListState` 总占比：

```bash
docker compose -f docker-compose-baseline.yml exec -T jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode get-add-state-payload \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 10000000 \
  --eventsPerSecond 0 \
  --keys 1000 \
  --mapEntries 100 \
  --maxValuesPerKey 10
```

baseline RocksDB large-state，扩大状态规模并提高 RocksDB native get/merge 耗时：

```bash
docker compose -f docker-compose-baseline.yml exec -T jobmanager flink run \
  -c org.apache.flink.benchmark.KryoStateJob \
  /opt/benchmarks.jar \
  --mode large-state \
  --parallelism 16 \
  --sourceParallelism 16 \
  --events 100000000 \
  --eventsPerSecond 0 \
  --keys 100000 \
  --mapEntries 20 \
  --largeStateGetEvery 1000 \
  --largeStateClearEvery 1000
```

生产 window/timer 路径：

```bash
docker compose -f docker-compose-baseline.yml exec -T jobmanager flink run \
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
