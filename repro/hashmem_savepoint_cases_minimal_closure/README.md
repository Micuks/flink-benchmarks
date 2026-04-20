# HashMemTable Savepoint Cases Minimal Closure

这个最小闭包用于在 OmniStateStore `origin/falcon` 基线上执行 3 条 savepoint/recovery 用例，便于故障排查与结果复现。

## 内容

- `job/hashmem-savepoint-cases.jar`
  - 可直接提交到 Flink session cluster 的 thin jar
- `src/`
  - 3 条 workload 与公共支撑类的源码快照
- `scripts/submit_case.sh`
  - 提交 `load` 或 `verify` 作业
- `scripts/trigger_savepoint.sh`
  - 对运行中作业触发 savepoint
- `scripts/check_output.sh`
  - 统计 mismatch 文件行数并打印前几行

## 用例

- `org.apache.flink.benchmark.HashMemtableSavepointRiskJob`
  - 长字符串 `ValueState<String>` savepoint/recovery 风险用例
- `org.apache.flink.benchmark.HashMemtableSavepointRmwRiskJob`
  - `ValueState<String>` 的 `read -> modify -> write` 用例
- `org.apache.flink.benchmark.HashMemtableSavepointMapStateRiskJob`
  - 长字符串 outer key + 长字符串 map key 的 `MapState` 用例

## 基线建议

推荐从以下运行时配置开始：

- `state.backend=rocksdb`
- `state.backend.rocksdb.memory.managed=false`
- `state.backend.rocksdb.falcon.use-hash-memtable=true`
- `state.backend.rocksdb.falcon.prefix-extractor.length=13`
- `state.backend.rocksdb.falcon.use-state-cache=false`

## 快速使用

先准备环境变量：

```bash
export FLINK_BIN=/opt/flink/bin/flink
export JOBMANAGER_REST=http://127.0.0.1:8081
export CLOSURE_DIR=/path/to/hashmem_savepoint_cases_minimal_closure
```

## 编译 Jar

源码仓库地址：

```text
https://github.com/Micuks/flink-benchmarks
```

如果需要从 `flink-benchmarks` 源码重新编译闭包里的 jar，可以在仓库根目录执行：

```bash
cd /path/to/flink-benchmarks
mvn -q -DskipTests package
```

产物位置：

```bash
target/benchmark-0.1.jar
```

闭包默认使用的文件名是：

```bash
job/hashmem-savepoint-cases.jar
```

如需替换，只要把新编出来的 `target/benchmark-0.1.jar` 覆盖到这个路径即可。

### 1. 启动 load

RiskJob：

```bash
CLASS_NAME=org.apache.flink.benchmark.HashMemtableSavepointRiskJob \
MODE=load \
OUTPUT_DIR=/tmp/hashmem_cases/risk/load \
KEYS=50000 \
bash "$CLOSURE_DIR/scripts/submit_case.sh"
```

RmwRiskJob：

```bash
CLASS_NAME=org.apache.flink.benchmark.HashMemtableSavepointRmwRiskJob \
MODE=load \
OUTPUT_DIR=/tmp/hashmem_cases/rmw/load \
KEYS=250000 \
UPDATES_PER_KEY=2 \
bash "$CLOSURE_DIR/scripts/submit_case.sh"
```

MapStateRiskJob：

```bash
CLASS_NAME=org.apache.flink.benchmark.HashMemtableSavepointMapStateRiskJob \
MODE=load \
OUTPUT_DIR=/tmp/hashmem_cases/mapstate/load \
STATE_KEYS=100000 \
ENTRIES_PER_KEY=5 \
bash "$CLOSURE_DIR/scripts/submit_case.sh"
```

脚本会输出 `JOB_ID=...`。

### 2. 触发 savepoint

```bash
JOB_ID=<load-job-id> \
TARGET_DIR=file:///opt/flink/log/hashmem_case_results/risk/savepoints \
bash "$CLOSURE_DIR/scripts/trigger_savepoint.sh"
```

脚本会输出 `SAVEPOINT_PATH=...`。

### 3. 取消 load 作业

```bash
curl -X PATCH "$JOBMANAGER_REST/jobs/<load-job-id>"
```

### 4. 从 savepoint 启动 verify

RiskJob：

```bash
CLASS_NAME=org.apache.flink.benchmark.HashMemtableSavepointRiskJob \
MODE=verify \
OUTPUT_DIR=/tmp/hashmem_cases/risk/verify \
KEYS=50000 \
SAVEPOINT_PATH=<savepoint-path> \
bash "$CLOSURE_DIR/scripts/submit_case.sh"
```

RmwRiskJob：

```bash
CLASS_NAME=org.apache.flink.benchmark.HashMemtableSavepointRmwRiskJob \
MODE=verify \
OUTPUT_DIR=/tmp/hashmem_cases/rmw/verify \
KEYS=250000 \
UPDATES_PER_KEY=2 \
SAVEPOINT_PATH=<savepoint-path> \
bash "$CLOSURE_DIR/scripts/submit_case.sh"
```

MapStateRiskJob：

```bash
CLASS_NAME=org.apache.flink.benchmark.HashMemtableSavepointMapStateRiskJob \
MODE=verify \
OUTPUT_DIR=/tmp/hashmem_cases/mapstate/verify \
STATE_KEYS=100000 \
ENTRIES_PER_KEY=5 \
SAVEPOINT_PATH=<savepoint-path> \
bash "$CLOSURE_DIR/scripts/submit_case.sh"
```

### 5. 检查结果

```bash
bash "$CLOSURE_DIR/scripts/check_output.sh" /tmp/hashmem_cases/risk/verify
```

## 说明

- `submit_case.sh` 默认提交到 `job/hashmem-savepoint-cases.jar`
- `checkpointIntervalMs` 默认是 `3600000`
- `parallelism` 默认是 `1`
- `sourceParallelism` 默认跟随 `parallelism`
- `eventsPerSecond` 默认是 `50000`
