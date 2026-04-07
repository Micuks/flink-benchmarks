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

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.benchmark.KryoStateBenchmark;
import org.apache.flink.benchmark.KryoStateBenchmark.MapRecord;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.compactState;

/**
 * Focused benchmark for ListState with POJO values that contain a Map field and therefore exercise
 * the PojoSerializer -> KryoSerializer -> MapSerializer state serialization path.
 *
 * <p>This benchmark bypasses the MiniCluster, source, keyBy shuffle and window trigger machinery in
 * {@link KryoStateBenchmark}. Use it to measure the throughput impact of ListState-specific
 * optimizations. Keep {@code KryoStateBenchmark.windowProcessMapPojo} for production flame-graph
 * reproduction.
 */
public class KryoListStateBenchmark extends StateBenchmarkBase {

    private static final String STATE_NAME = "kryoMapPojoListState";

    @Param({"10"})
    public int numberOfKeys = 10;

    @Param({"100"})
    public int mapEntries = 100;

    @Param({"1000"})
    public int valuesPerKey = 1000;

    @Param({"1024"})
    public int recordPoolSize = 1024;

    @Param({"100"})
    public int addsPerGet = 100;

    private ListState<MapRecord> listState;
    private ListStateDescriptor<MapRecord> stateDescriptor;
    private MapRecord[] records;
    private int keyCursor;
    private int recordCursor;

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + KryoListStateBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {
        keyedStateBackend = createKeyedStateBackend();
        stateDescriptor =
                new ListStateDescriptor<>(STATE_NAME, TypeInformation.of(MapRecord.class));
        listState =
                keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
        records = createRecordPool();
        keyCursor = 0;
        recordCursor = 0;
    }

    @Setup(Level.Iteration)
    public void setUpPerIteration() throws Exception {
        for (int key = 0; key < numberOfKeys; key++) {
            keyedStateBackend.setCurrentKey((long) key);
            for (int valueIndex = 0; valueIndex < valuesPerKey; valueIndex++) {
                listState.add(records[(key * valuesPerKey + valueIndex) % records.length]);
            }
        }
        compactRocksDBState();
        keyCursor = 0;
        recordCursor = 0;
    }

    @TearDown(Level.Iteration)
    public void tearDownPerIteration() throws Exception {
        for (int key = 0; key < numberOfKeys * 2; key++) {
            keyedStateBackend.setCurrentKey((long) key);
            listState.clear();
        }
        compactRocksDBState();
    }

    @Benchmark
    public void listAddMapPojo() throws Exception {
        int index = nextRecordIndex();
        keyedStateBackend.setCurrentKey((long) (index % numberOfKeys));
        listState.add(records[index % records.length]);
    }

    @Benchmark
    public void listGetAndIterateMapPojo(Blackhole blackhole) throws Exception {
        keyedStateBackend.setCurrentKey((long) nextKey());
        Iterable<MapRecord> values = listState.get();
        for (MapRecord value : values) {
            blackhole.consume(value.key);
            blackhole.consume(value.value);
            blackhole.consume(value.counters);
        }
    }

    @Benchmark
    public void listGetAndAddMapPojo(Blackhole blackhole) throws Exception {
        keyedStateBackend.setCurrentKey((long) nextKey());
        Iterable<MapRecord> values = listState.get();
        for (MapRecord value : values) {
            blackhole.consume(value.key);
            blackhole.consume(value.value);
            blackhole.consume(value.counters);
        }

        for (int i = 0; i < addsPerGet; i++) {
            int index = nextRecordIndex();
            // Keep writes on a disjoint key range so reads remain stable across an iteration.
            keyedStateBackend.setCurrentKey((long) (numberOfKeys + (index % numberOfKeys)));
            listState.add(records[index % records.length]);
        }
    }

    private MapRecord[] createRecordPool() {
        MapRecord[] pool = new MapRecord[recordPoolSize];
        for (int i = 0; i < pool.length; i++) {
            pool[i] =
                    KryoStateBenchmark.createMapRecord(
                            i % numberOfKeys, 100_000L + i, mapEntries);
        }
        return pool;
    }

    private int nextKey() {
        int index = keyCursor;
        keyCursor = keyCursor == Integer.MAX_VALUE ? 0 : keyCursor + 1;
        return index % numberOfKeys;
    }

    private int nextRecordIndex() {
        int index = recordCursor;
        recordCursor = recordCursor == Integer.MAX_VALUE ? 0 : recordCursor + 1;
        return index;
    }

    private void compactRocksDBState() throws Exception {
        if (keyedStateBackend instanceof RocksDBKeyedStateBackend) {
            @SuppressWarnings("unchecked")
            RocksDBKeyedStateBackend<Long> rocksDBKeyedStateBackend =
                    (RocksDBKeyedStateBackend<Long>) keyedStateBackend;
            compactState(rocksDBKeyedStateBackend, stateDescriptor);
        } else {
            System.gc();
        }
    }
}
