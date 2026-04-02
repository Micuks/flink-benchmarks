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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
 *         → AbstractRocksDBState.serializeValue()      // LEFT branch (~55% CPU)
 *           → PojoSerializer.serialize()
 *             → KryoSerializer.serialize() (for Map field)
 *               → Kryo.writeClassAndObject()
 *                 → MapSerializer.write()
 *                   → MapReferenceResolver / identityHashCode
 *         → RocksDB.merge()                            // RIGHT branch (~25% CPU)
 *           → DBImpl::WriteImpl → MemTable::Add
 * </pre>
 *
 * <p>Key insight: {@code .process(ProcessWindowFunction)} makes WindowOperator use
 * <b>ListState</b> internally (not ReducingState or AggregatingState). ListState.add()
 * calls {@code RocksDB.merge()} after serializing each element, which is exactly what
 * the flame graph shows.
 *
 * <p>Benchmarks:
 * <ul>
 *   <li>{@code windowProcessMapPojo}: ListState + MapRecord (triggers Kryo) — reproduces hotspot</li>
 *   <li>{@code windowProcessSimple}: ListState + SimpleRecord (no Kryo) — control</li>
 *   <li>{@code windowReduceMapPojo}: ReducingState + MapRecord — comparison (read-modify-write, no merge)</li>
 * </ul>
 *
 * <p>All benchmarks run at parallelism=1 for single-core measurement.
 */
@OperationsPerInvocation(value = KryoStateBenchmark.RECORDS_PER_INVOCATION)
public class KryoStateBenchmark extends BenchmarkBase {

    public static final int RECORDS_PER_INVOCATION = 1_000_000;

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
                .window(TumblingEventTimeWindows.of(Time.seconds(10_000)))
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10_000)))
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10_000)))
                .reduce(new MapRecordReduceFunction())
                .addSink(new DiscardingSink<>());

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

    // -------------------------------------------------------------------------
    //  Sources
    // -------------------------------------------------------------------------

    /** Source emitting MapRecord with timestamps for event-time windows. */
    public static class MapRecordSource extends RichParallelSourceFunction<MapRecord> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;
        private final int numKeys;
        private final long numEvents;

        public MapRecordSource(int numKeys, long numEvents) {
            this.numKeys = numKeys;
            this.numEvents = numEvents;
        }

        @Override
        public void run(SourceContext<MapRecord> ctx) {
            long counter = 0;
            while (running && counter < numEvents) {
                int keyId = (int) (counter % numKeys);
                Map<String, Long> counters = new HashMap<>(4);
                counters.put("a", (long) keyId);
                counters.put("b", (long) keyId * 2);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collectWithTimestamp(
                            new MapRecord(keyId, counter, counters), counter);
                }
                counter++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Source emitting SimpleRecord with timestamps for event-time windows. */
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
                    ctx.collectWithTimestamp(
                            new SimpleRecord(keyId, counter), counter);
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

    public enum SerMode {
        /** Default Kryo: unregistered types, reference tracking enabled. */
        KRYO_DEFAULT,
        /** Kryo with types registered: avoids class name serialization. */
        KRYO_REGISTERED
    }

    @State(Thread)
    public static class KryoStateContext extends FlinkEnvironmentContext {

        @Param({"KRYO_DEFAULT", "KRYO_REGISTERED"})
        public SerMode serMode = SerMode.KRYO_DEFAULT;

        public final int numberOfKeys = 1000;
        public DataStreamSource<MapRecord> mapSource;
        public DataStreamSource<SimpleRecord> simpleSource;
        private File checkpointDir;

        @Override
        public void setUp() throws Exception {
            checkpointDir = Files.createTempDirectory("kryo-bench-").toFile();
            super.setUp();

            String checkpointUri = "file://" + checkpointDir.getAbsolutePath();
            env.setStateBackend(new RocksDBStateBackend(checkpointUri, false));
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            if (serMode == SerMode.KRYO_REGISTERED) {
                env.getConfig().registerKryoType(HashMap.class);
                env.getConfig().registerKryoType(MapRecord.class);
                env.getConfig().registerKryoType(SimpleRecord.class);
            }

            mapSource = env.addSource(new MapRecordSource(numberOfKeys, RECORDS_PER_INVOCATION));
            simpleSource = env.addSource(new SimpleRecordSource(numberOfKeys, RECORDS_PER_INVOCATION));
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
