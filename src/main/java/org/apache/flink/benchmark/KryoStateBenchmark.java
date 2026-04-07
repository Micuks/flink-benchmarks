/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Benchmark reproducing the flame graph hotspot: WindowOperator + RocksDB ListState
 * with POJO values containing Map fields that trigger Kryo serialization.
 *
 * <h3>Flame graph call stack reproduced:</h3>
 * <pre>
 *   WindowOperator.processElement
 *     → ListState.add(element)                         // each incoming record
 *       → RocksDBListState.add()
 *         → AbstractRocksDBState.serializeValue()      // serialize branch
 *           → PojoSerializer.serialize()
 *             → KryoSerializer.serialize() (for Map field)
 *               → Kryo.writeClassAndObject()
 *                 → MapSerializer.write()
 *                   → MapReferenceResolver / identityHashCode
 *         → RocksDB.merge()                            // write branch
 *
 *     → (on window fire) ListState.get()               // read branch
 *       → RocksDB.get() + full deserialization
 * </pre>
 *
 * <h3>Two distinct flame graph stacks (matching production profile):</h3>
 * <p><b>Stack 1 — write path</b> (from {@code StreamInputProcessor.processInput}):
 * <br>{@code WindowOperator.processElement → ListState.add → serializeValue + RocksDB.merge}
 * <p><b>Stack 2 — read path</b> (from {@code MailboxProcessor.processMailsNonBlocking}):
 * <br>{@code WindowOperator.onProcessingTime → RocksDBListState.getInternal → RocksDB.get
 *   + ListDelimitedSerializer.deserializeList → PojoSerializer.deserialize → Kryo}
 *
 * <p>Using <b>Processing Time Windows</b> (not Event Time) so that window triggers
 * fire via internal timer callbacks ({@code onProcessingTime}), exactly matching
 * the production flame graph. 5-second windows ensure multiple firings per run.
 *
 * <p>All benchmarks run at parallelism=1 for single-core measurement.
 */
@OperationsPerInvocation(value = KryoStateBenchmark.RECORDS_PER_INVOCATION)
public class KryoStateBenchmark extends BenchmarkBase {

    public static final int RECORDS_PER_INVOCATION = 1_000_000;
    public static final int BALANCED_GET_ADD_MAX_VALUES_PER_KEY = 3;
    public static final int LARGE_STATE_GET_EVERY = 1_000;
    public static final int LARGE_STATE_CLEAR_EVERY = 1_000;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + KryoStateBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    // -------------------------------------------------------------------------
    //  Benchmarks
    // -------------------------------------------------------------------------

    /**
     * PRIMARY BENCHMARK: Window + ProcessWindowFunction → ListState internally.
     *
     * <p>Each incoming MapRecord is serialized via PojoSerializer→Kryo and written
     * to RocksDB via merge(). This is the exact path shown in the flame graph.
     */
    @Benchmark
    public void windowProcessMapPojo(KryoStateContext context) throws Exception {
        context.mapSource
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new SumProcessFunction<MapRecord>() {
                    @Override
                    protected long extractValue(MapRecord r) {
                        return r.value;
                    }
                })
                .addSink(new DiscardingSink<>());

        context.env.execute();
    }

    /**
     * CONTROL: Window + ProcessWindowFunction → ListState, but with SimpleRecord
     * (no Map field, no Kryo fallback). Shows the cost without Kryo overhead.
     */
    @Benchmark
    public void windowProcessSimple(KryoStateContext context) throws Exception {
        context.simpleSource
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new SumProcessFunction<SimpleRecord>() {
                    @Override
                    protected long extractValue(SimpleRecord r) {
                        return r.value;
                    }
                })
                .addSink(new DiscardingSink<>());

        context.env.execute();
    }

    /**
     * COMPARISON: Window + reduce() → ReducingState internally.
     * Uses read-modify-write (get + reduce + put) instead of merge().
     * Included to measure the difference between ListState(merge) vs ReducingState(get+put).
     */
    @Benchmark
    public void windowReduceMapPojo(KryoStateContext context) throws Exception {
        context.mapSource
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new MapRecordReduceFunction())
                .addSink(new DiscardingSink<>());

        context.env.execute();
    }

    /**
     * MIXED BENCHMARK: KeyedProcessFunction with one explicit ListState.get() and one
     * ListState.add() on each input record. This keeps get and add in the same
     * processElement stack.
     */
    @Benchmark
    public void keyedListStateGetAddMapPojo(KryoStateContext context) throws Exception {
        context.mapSource
                .keyBy(r -> r.key)
                .process(
                        new ListStateGetAddProcessFunction(
                                context.numberOfKeys, context.getAddStateMaxValuesPerKey))
                .name("keyed-list-state-get-add-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");

        context.env.execute();
    }

    /**
     * BALANCED MIXED BENCHMARK: same explicit ListState get+add path, but keeps the per-key list
     * short so the average get work is close to one deserialized value per add.
     */
    @Benchmark
    public void keyedListStateGetAddBalancedMapPojo(KryoStateContext context) throws Exception {
        context.mapSource
                .keyBy(r -> r.key)
                .process(
                        new ListStateGetAddProcessFunction(
                                context.numberOfKeys, context.balancedGetAddStateMaxValuesPerKey))
                .name("keyed-list-state-get-add-balanced-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");

        context.env.execute();
    }

    /**
     * STATE-PAYLOAD BENCHMARK: keyBy uses SimpleRecord to avoid network Kryo Map overhead, while
     * the keyed operator still stores MapRecord values in ListState.
     */
    @Benchmark
    public void keyedListStateGetAddStatePayloadMapPojo(KryoStateContext context)
            throws Exception {
        context.simpleSource
                .keyBy(r -> r.key)
                .process(
                        new ListStateGetAddStatePayloadProcessFunction(
                                context.numberOfKeys,
                                context.balancedGetAddStateMaxValuesPerKey,
                                context.mapEntries))
                .name("keyed-list-state-get-add-state-payload-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");

        context.env.execute();
    }

    /**
     * LARGE-STATE BENCHMARK: keep the MapRecord ListState payload while increasing the retained
     * state working set and using sparse large reads to make RocksDB get/merge more visible.
     */
    @Benchmark
    public void keyedListStateLargeStateMapPojo(KryoStateContext context) throws Exception {
        context.simpleSource
                .keyBy(r -> r.key)
                .process(
                        new ListStateLargeStateProcessFunction(
                                context.numberOfKeys,
                                context.mapEntries,
                                context.largeStateGetEvery,
                                context.largeStateClearEvery))
                .name("keyed-list-state-large-state-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");

        context.env.execute();
    }

    // -------------------------------------------------------------------------
    //  Data types
    // -------------------------------------------------------------------------

    /** Record with a Map field — triggers Kryo MapSerializer when serialized as state. */
    public static class MapRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public int key;
        public long value;
        public Map<String, Long> counters;

        public MapRecord() {
            this.counters = new HashMap<>();
        }

        public MapRecord(int key, long value, Map<String, Long> counters) {
            this.key = key;
            this.value = value;
            this.counters = counters;
        }
    }

    /** Simple record without Map — no Kryo fallback, used as control. */
    public static class SimpleRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public int key;
        public long value;

        public SimpleRecord() {}

        public SimpleRecord(int key, long value) {
            this.key = key;
            this.value = value;
        }
    }

    // -------------------------------------------------------------------------
    //  Functions
    // -------------------------------------------------------------------------

    /**
     * ProcessWindowFunction that sums values. Using process() forces WindowOperator
     * to use ListState (collects all elements, fires on window trigger).
     */
    public abstract static class SumProcessFunction<T>
            extends ProcessWindowFunction<T, Long, Integer, TimeWindow> {

        protected abstract long extractValue(T element);

        @Override
        public void process(
                Integer key,
                ProcessWindowFunction<T, Long, Integer, TimeWindow>.Context context,
                Iterable<T> elements,
                Collector<Long> out) {
            long sum = 0;
            for (T element : elements) {
                sum += extractValue(element);
            }
            out.collect(sum);
        }
    }

    /** Reduce function for MapRecord — for the ReducingState comparison benchmark. */
    public static class MapRecordReduceFunction implements ReduceFunction<MapRecord> {
        @Override
        public MapRecord reduce(MapRecord v1, MapRecord v2) {
            Map<String, Long> merged = new HashMap<>(v1.counters);
            v2.counters.forEach((k, v) -> merged.merge(k, v, Long::sum));
            return new MapRecord(v1.key, v1.value + v2.value, merged);
        }
    }

    /**
     * Explicit ListState get+add path. The bounded clear keeps the read-side list size stable while
     * still keeping get and add visible in one flame graph.
     */
    public static class ListStateGetAddProcessFunction
            extends KeyedProcessFunction<Integer, MapRecord, Long> {
        private static final long serialVersionUID = 1L;
        private static final String STATE_NAME = "kryo-get-add-list-state";

        private final int numberOfKeys;
        private final int maxValuesPerKey;
        private transient ListState<MapRecord> listState;
        private transient int[] valuesPerKey;

        public ListStateGetAddProcessFunction(int numberOfKeys, int maxValuesPerKey) {
            this.numberOfKeys = numberOfKeys;
            this.maxValuesPerKey = maxValuesPerKey;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<MapRecord> descriptor =
                    new ListStateDescriptor<>(STATE_NAME, TypeInformation.of(MapRecord.class));
            listState = getRuntimeContext().getListState(descriptor);
            valuesPerKey = new int[numberOfKeys];
        }

        @Override
        public void processElement(MapRecord value, Context ctx, Collector<Long> out)
                throws Exception {
            long sum = 0L;
            for (MapRecord stateValue : listState.get()) {
                sum += stateValue.value;
            }

            listState.add(value);

            int keyIndex = Math.floorMod(value.key, numberOfKeys);
            valuesPerKey[keyIndex]++;
            if (valuesPerKey[keyIndex] >= maxValuesPerKey) {
                listState.clear();
                valuesPerKey[keyIndex] = 0;
            }

            out.collect(sum);
        }
    }

    /**
     * Reads a lightweight SimpleRecord from the network, then writes a prebuilt MapRecord payload to
     * ListState. This keeps state serialization on the Kryo Map path while reducing non-state
     * network serialization in the flame graph.
     */
    public static class ListStateGetAddStatePayloadProcessFunction
            extends KeyedProcessFunction<Integer, SimpleRecord, Long> {
        private static final long serialVersionUID = 1L;
        private static final String STATE_NAME = "kryo-get-add-state-payload-list-state";

        private final int numberOfKeys;
        private final int maxValuesPerKey;
        private final int mapEntries;
        private transient ListState<MapRecord> listState;
        private transient int[] valuesPerKey;
        private transient MapRecord[] payloads;

        public ListStateGetAddStatePayloadProcessFunction(
                int numberOfKeys, int maxValuesPerKey, int mapEntries) {
            this.numberOfKeys = numberOfKeys;
            this.maxValuesPerKey = maxValuesPerKey;
            this.mapEntries = mapEntries;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<MapRecord> descriptor =
                    new ListStateDescriptor<>(STATE_NAME, TypeInformation.of(MapRecord.class));
            listState = getRuntimeContext().getListState(descriptor);
            valuesPerKey = new int[numberOfKeys];
            payloads = new MapRecord[numberOfKeys];
            for (int key = 0; key < numberOfKeys; key++) {
                payloads[key] = createMapRecord(key, 10_000L + key, mapEntries);
            }
        }

        @Override
        public void processElement(SimpleRecord value, Context ctx, Collector<Long> out)
                throws Exception {
            long sum = 0L;
            for (MapRecord stateValue : listState.get()) {
                sum += stateValue.value;
            }

            int keyIndex = Math.floorMod(value.key, numberOfKeys);
            listState.add(payloads[keyIndex]);

            valuesPerKey[keyIndex]++;
            if (valuesPerKey[keyIndex] >= maxValuesPerKey) {
                listState.clear();
                valuesPerKey[keyIndex] = 0;
            }

            out.collect(sum);
        }
    }

    /**
     * Writes a MapRecord payload to ListState for every input and performs sparse reads when each
     * key reaches a configured count. With a high key count and high clear threshold, this creates a
     * large RocksDB working set without making every input deserialize a long list.
     */
    public static class ListStateLargeStateProcessFunction
            extends KeyedProcessFunction<Integer, SimpleRecord, Long> {
        private static final long serialVersionUID = 1L;
        private static final String STATE_NAME = "large-list-state-map";

        private final int numberOfKeys;
        private final int mapEntries;
        private final int getEvery;
        private final int clearEvery;
        private transient ListState<MapRecord> listState;
        private transient int[] valuesPerKey;
        private transient MapRecord payload;

        public ListStateLargeStateProcessFunction(
                int numberOfKeys, int mapEntries, int getEvery, int clearEvery) {
            this.numberOfKeys = numberOfKeys;
            this.mapEntries = mapEntries;
            this.getEvery = Math.max(1, getEvery);
            this.clearEvery = clearEvery;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<MapRecord> descriptor =
                    new ListStateDescriptor<>(STATE_NAME, TypeInformation.of(MapRecord.class));
            listState = getRuntimeContext().getListState(descriptor);
            valuesPerKey = new int[numberOfKeys];
            payload = createMapRecord(0, 10_000L, mapEntries);
        }

        @Override
        public void processElement(SimpleRecord value, Context ctx, Collector<Long> out)
                throws Exception {
            int keyIndex = Math.floorMod(value.key, numberOfKeys);
            listState.add(payload);
            int count = ++valuesPerKey[keyIndex];

            long sum = 0L;
            if (count % getEvery == 0) {
                for (MapRecord stateValue : listState.get()) {
                    sum += stateValue.value;
                }
            }

            if (clearEvery > 0 && count >= clearEvery) {
                listState.clear();
                valuesPerKey[keyIndex] = 0;
            }

            out.collect(sum);
        }
    }

    // -------------------------------------------------------------------------
    //  Sources
    // -------------------------------------------------------------------------

    /** Source emitting MapRecord for processing-time windows. */
    public static class MapRecordSource extends RichParallelSourceFunction<MapRecord> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private final int numKeys;
        private final long numEvents;

        private final int mapEntries;
        private final boolean reuseRecords;
        private transient MapRecord[] reusableRecords;

        public MapRecordSource(
                int numKeys, long numEvents, int mapEntries, boolean reuseRecords) {
            this.numKeys = numKeys;
            this.numEvents = numEvents;
            this.mapEntries = mapEntries;
            this.reuseRecords = reuseRecords;
        }

        @Override
        public void run(SourceContext<MapRecord> ctx) {
            long counter = 0;
            while (running && counter < numEvents) {
                int keyId = (int) (counter % numKeys);
                MapRecord record =
                        reuseRecords
                                ? reusableRecords[keyId]
                                : createMapRecord(keyId, counter, mapEntries);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(record);
                }
                counter++;
            }
        }

        @Override
        public void open(Configuration parameters) {
            if (reuseRecords) {
                reusableRecords = new MapRecord[numKeys];
                for (int i = 0; i < numKeys; i++) {
                    reusableRecords[i] = createMapRecord(i, 10_000L + i, mapEntries);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static MapRecord createMapRecord(int key, long value, int mapEntries) {
        Map<String, Long> counters = new HashMap<>(mapEntries * 2);
        for (int i = 0; i < mapEntries; i++) {
            counters.put("field_" + i, value + i);
        }
        return new MapRecord(key, value, counters);
    }

    /** Source emitting SimpleRecord for processing-time windows. */
    public static class SimpleRecordSource extends RichParallelSourceFunction<SimpleRecord> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private final int numKeys;
        private final long numEvents;

        public SimpleRecordSource(int numKeys, long numEvents) {
            this.numKeys = numKeys;
            this.numEvents = numEvents;
        }

        @Override
        public void run(SourceContext<SimpleRecord> ctx) {
            long counter = 0;
            while (running && counter < numEvents) {
                int keyId = (int) (counter % numKeys);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new SimpleRecord(keyId, counter));
                }
                counter++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // -------------------------------------------------------------------------
    //  Context
    // -------------------------------------------------------------------------

    public enum BackendMode {
        /** Vanilla RocksDB with default Kryo (unregistered, reference tracking on). */
        ROCKS,
        /** Vanilla RocksDB with Kryo types registered. */
        ROCKS_KRYO_REG,
        /**
         * OmniStateStore (Falcon) with Kryo types registered + RocksDB write buffer tuning.
         * Requires flink-alg-falcon.jar on classpath.
         */
        FALCON,
        /** Falcon + ValueState cache enabled. Requires libfalcon.so via java.library.path. */
        FALCON_CACHE
    }

    @State(Thread)
    public static class KryoStateContext extends FlinkEnvironmentContext {

        @Param({"ROCKS", "ROCKS_KRYO_REG", "FALCON", "FALCON_CACHE"})
        public BackendMode backendMode = BackendMode.ROCKS;

        @Param({"1000"})
        public int numberOfKeys = 1000;

        @Param({"20"})
        public int mapEntries = 20;

        @Param({"false"})
        public boolean reuseSourceRecords = false;

        @Param({"10"})
        public int getAddStateMaxValuesPerKey = 10;

        @Param({"3"})
        public int balancedGetAddStateMaxValuesPerKey = BALANCED_GET_ADD_MAX_VALUES_PER_KEY;

        @Param({"1000"})
        public int largeStateGetEvery = LARGE_STATE_GET_EVERY;

        @Param({"1000"})
        public int largeStateClearEvery = LARGE_STATE_CLEAR_EVERY;

        public DataStreamSource<MapRecord> mapSource;
        public DataStreamSource<SimpleRecord> simpleSource;
        private File checkpointDir;

        @Override
        public void setUp() throws Exception {
            checkpointDir = Files.createTempDirectory("kryo-bench-").toFile();
            super.setUp();

            String checkpointUri = "file://" + checkpointDir.getAbsolutePath();
            RocksDBStateBackend rocksBackend = new RocksDBStateBackend(checkpointUri, false);

            if (backendMode == BackendMode.FALCON || backendMode == BackendMode.FALCON_CACHE) {
                rocksBackend.setRocksDBOptions(
                        new com.huawei.falcon.state.RocksDBOptOptionsFactory());
            }

            env.setStateBackend(rocksBackend);

            // Kryo type registration for non-baseline modes
            if (backendMode != BackendMode.ROCKS) {
                env.getConfig().registerKryoType(HashMap.class);
                env.getConfig().registerKryoType(MapRecord.class);
                env.getConfig().registerKryoType(SimpleRecord.class);
            }

            mapSource =
                    env.addSource(
                            new MapRecordSource(
                                    numberOfKeys,
                                    RECORDS_PER_INVOCATION,
                                    mapEntries,
                                    reuseSourceRecords));
            simpleSource =
                    env.addSource(new SimpleRecordSource(numberOfKeys, RECORDS_PER_INVOCATION));
        }

        @Override
        protected Configuration createConfiguration() {
            Configuration configuration = super.createConfiguration();
            configuration.set(
                    RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE, MemorySize.parse("322122552b"));

            if (backendMode == BackendMode.FALCON || backendMode == BackendMode.FALCON_CACHE) {
                configuration.setString(
                        "state.backend.rocksdb.options-factory",
                        "com.huawei.falcon.state.RocksDBOptOptionsFactory");
                configuration.setString(
                        "state.backend.rocksdb.falcon.use-partition-filter", "true");
                configuration.setString(
                        "state.backend.rocksdb.falcon.use-range-filter", "true");
                configuration.setString(
                        "state.backend.rocksdb.falcon.use-hash-memtable", "true");
                configuration.setString(
                        "state.backend.rocksdb.falcon.use-merge", "true");
                configuration.setString(
                        "state.backend.rocksdb.falcon.prefix-extractor.length", "13");
            }
            if (backendMode == BackendMode.FALCON_CACHE) {
                configuration.setString(
                        "state.backend.rocksdb.falcon.use-state-cache", "true");
                configuration.setString(
                        "state.backend.rocksdb.falcon.state-cache-sizeLimit", "20000");
                configuration.setString(
                        "state.backend.rocksdb.falcon.state-cache-bypass-hitRatio", "0.2");
            } else if (backendMode == BackendMode.FALCON) {
                configuration.setString(
                        "state.backend.rocksdb.falcon.use-state-cache", "false");
            }

            return configuration;
        }

        @Override
        public void tearDown() throws Exception {
            super.tearDown();
            if (checkpointDir != null) {
                FileUtils.deleteDirectory(checkpointDir);
            }
        }
    }
}
