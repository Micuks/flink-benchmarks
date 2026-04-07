# windowProcessMapPojo flamegraph experiment results

## 实验配置

| 参数 | 值 |
|------|------|
| Benchmark | `windowProcessMapPojo` |
| backendMode | `ROCKS` |
| JDK | OpenJDK 11.0.26 (Corretto) |
| async-profiler | 4.0, event=itimer |
| JMH | fork=1, warmup=1, measurement=3 |
| RECORDS_PER_INVOCATION | 1,000,000 |
| numberOfKeys | 1,000 |
| Window | TumblingProcessingTimeWindows(5s) |
| MAP_ENTRIES | 20 |
| Throughput | ~36.8 ops/ms |

## 火焰图文件

- HTML 交互式火焰图：`docs/flamegraphs/windowProcessMapPojo_ROCKS.html`
- Collapsed stacks：`docs/flamegraphs/windowProcessMapPojo_ROCKS.collapsed`

## 总样本分布

| 入口 | 样本数 | 占比 |
|------|--------|------|
| 总样本 | 17,743 | 100% |
| `StreamInputProcessor.processInput` 写路径入口 | 9,527 | 53.6% |
| `MailboxProcessor.processMailsNonBlocking` 读路径入口 | 3,202 | 18.0% |

写路径:读路径约为 `1.5:1`。这里仅计入与 `WindowOperator` 直接相关的部分，未包含 source、调度等框架开销。

## 写路径

从 `StreamInputProcessor.processInput` 进入：

```text
WindowOperator.processElement
  -> RocksDBListState.add
    -> AbstractRocksDBState.serializeValue              16.1%
      -> PojoSerializer.serialize
        -> KryoSerializer.serialize                     15.6%
          -> Kryo.writeClassAndObject
            -> MapSerializer.write                      14.1%
              -> MapReferenceResolver.getWrittenId
                -> IdentityObjectIntMap.get
                  -> System.identityHashCode
    -> RocksDB.merge (native)                            8.8%
```

关键发现：序列化分支 `serializeValue 16.1%` 与 RocksDB native `merge 8.8%` 清晰分离。`MapSerializer.write` 是序列化分支的主要热点，约占 `14.1 / 16.1 = 87.6%`，说明 Map 字段的 Kryo 回退几乎占满整个序列化开销。

## 读路径

从 `MailboxProcessor.processMailsNonBlocking` 进入：

```text
MailboxProcessor.processMailsNonBlocking
  -> Mail.run
    -> StreamTask.invokeProcessingTimeCallback
      -> InternalTimerServiceImpl.onProcessingTime
        -> WindowOperator.onProcessingTime
          -> RocksDBListState.get
            -> RocksDBListState.getInternal
              -> RocksDB.get (native)                    2.3%
                -> DBImpl::Get
                  -> DBImpl::GetImpl
                    -> Version::Get
                      -> TableCache::Get
                        -> BlockBasedTable::Get
                          -> RetrieveBlock -> MaybeReadBlockAndLoadToCache
                            -> BlockFetcher::ReadBlockContents
                              -> UncompressBlockContents
                                -> snappy::RawUncompress  1.3%
              -> ListDelimitedSerializer.deserializeList 15.3%
                -> PojoSerializer.deserialize
                  -> KryoSerializer.deserialize          14.9%
                    -> Kryo.readClassAndObject
                      -> MapSerializer.read              13.7%
```

关键发现：

- 读路径入口为 `MailboxProcessor.processMailsNonBlocking -> onProcessingTime`，与生产环境火焰图一致。
- RocksDB 读路径细节可见：`DBImpl::Get -> Version::Get -> TableCache::Get -> BlockBasedTable::Get`，以及 snappy 解压 `snappy::RawUncompress`。
- 反序列化分支 `15.3%` 远大于 native get `2.3%`，说明反序列化是读路径的主要开销。
- `MapSerializer.read` 占反序列化分支约 `13.7 / 15.3 = 89.6%`。

## merge vs get

| 指标 | 旧版本 Event Time 10000s window | Processing Time 5s window |
|------|----------------------------------|---------------------------|
| 写路径入口占比 | ~90%+ | 53.6% |
| 读路径入口占比 | <5% | 18.0% |
| RocksDB.merge | 压倒性 | 8.8% |
| RocksDB.get | 几乎不可见 | 2.3% |
| 反序列化可见性 | 几乎不可见 | 15.3% |

切换到 Processing Time 5s window 后，读路径占比从几乎不可见提升到 18%，反序列化调用栈完全可见。
