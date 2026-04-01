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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Benchmark for Window + RocksDB with POJO state containing Map fields.
 *
 * <p>This reproduces the flame graph hotspot where Kryo's MapSerializer dominates CPU
 * (~55%) due to PojoSerializer falling back to KryoSerializer for java.util.Map fields.
 *
 * <p>The benchmark compares three serialization modes:
 * <ul>
 *   <li>KRYO_DEFAULT: unregistered Kryo (writes full class names + reference tracking)</li>
 *   <li>KRYO_REGISTERED: registered types (avoids class name overhead)</li>
 *   <li>REDUCE_SIMPLE: same workload but simple value type (no Map), as a control</li>
 * </ul>
 *
 * <p>All modes run single-threaded (parallelism=1) on a MiniCluster for single-core measurement.
 * Use {@code taskset -c 0} for CPU pinning if needed.
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
     * Window aggregate with POJO accumulator containing Map field.
     * This exercises the PojoSerializer -> KryoSerializer -> MapSerializer path.
     */
    @Benchmark
    public void windowAggregateWithMapState(KryoStateContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;

        context.mapSource
                .keyBy(r -> r.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(10_000)))
                .aggregate(new MapAccumulatorAggregateFunction())
                .addSink(new DiscardingSink<>());

        env.execute();
    }

    /**
     * Window reduce with simple POJO (int + long). Control benchmark without Kryo.
     */
    @Benchmark
    public void windowReduceSimple(KryoStateContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;

        context.simpleSource
                .keyBy(r -> r.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(10_000)))
                .reduce(new SimpleReduceFunction())
                .addSink(new DiscardingSink<>());

        env.execute();
    }

    // -------------------------------------------------------------------------
    //  Data types
    // -------------------------------------------------------------------------

    /** Record with a Map field — triggers Kryo MapSerializer when used as state. */
    public static class MapRecord implements Serializable {
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

    /** Simple record — no Kryo fallback, used as control. */
    public static class SimpleRecord implements Serializable {
        public int key;
        public long value;

        public SimpleRecord() {}

        public SimpleRecord(int key, long value) {
            this.key = key;
            this.value = value;
        }
    }

    /** Accumulator POJO with a Map field — the core of the Kryo hotspot. */
    public static class MapAccumulator implements Serializable {
        public long count;
        public double sum;
        public Map<String, Long> groupCounts;

        public MapAccumulator() {
            this.groupCounts = new HashMap<>();
        }
    }

    // -------------------------------------------------------------------------
    //  Functions
    // -------------------------------------------------------------------------

    /** Aggregate function whose accumulator is a POJO with Map — triggers Kryo. */
    public static class MapAccumulatorAggregateFunction
            implements AggregateFunction<MapRecord, MapAccumulator, Long> {

        @Override
        public MapAccumulator createAccumulator() {
            return new MapAccumulator();
        }

        @Override
        public MapAccumulator add(MapRecord value, MapAccumulator acc) {
            acc.count++;
            acc.sum += value.value;
            String group = "g" + (value.key % 10);
            acc.groupCounts.merge(group, 1L, Long::sum);
            return acc;
        }

        @Override
        public Long getResult(MapAccumulator acc) {
            return acc.count;
        }

        @Override
        public MapAccumulator merge(MapAccumulator a, MapAccumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            b.groupCounts.forEach((k, v) -> a.groupCounts.merge(k, v, Long::sum));
            return a;
        }
    }

    public static class SimpleReduceFunction implements ReduceFunction<SimpleRecord> {
        @Override
        public SimpleRecord reduce(SimpleRecord v1, SimpleRecord v2) {
            return new SimpleRecord(v1.key, v1.value + v2.value);
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
                env.getConfig().registerKryoType(MapAccumulator.class);
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
