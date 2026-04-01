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

import org.apache.flink.benchmark.functions.IntLongApplications;
import org.apache.flink.benchmark.functions.IntegerLongSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
import java.nio.file.Files;

import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Benchmark comparing vanilla RocksDB state backend vs OmniStateStore-optimized RocksDB.
 *
 * <p>Tests window reduce operations with tumbling windows, exercising:
 * - Serialization (PojoSerializer / KryoSerializer path)
 * - RocksDB merge writes
 * - State read-modify-write cycles
 *
 * <p>The "OMNI" variant enables OmniStateStore optimizations:
 * - Auto Kryo type registration (eliminates class name writes)
 * - RocksDB write buffer tuning (reduces flush frequency)
 */
@OperationsPerInvocation(value = OmniStateBackendBenchmark.RECORDS_PER_INVOCATION)
public class OmniStateBackendBenchmark extends BenchmarkBase {
    public static final int RECORDS_PER_INVOCATION = 2_000_000;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + OmniStateBackendBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void tumblingWindowReduce(OmniStateBackendContext context) throws Exception {
        IntLongApplications.reduceWithWindow(
                context.source, TumblingEventTimeWindows.of(Time.seconds(10_000)));
        context.execute();
    }

    public enum BackendMode {
        ROCKS_BASELINE,
        ROCKS_OMNI
    }

    @State(Thread)
    public static class OmniStateBackendContext extends FlinkEnvironmentContext {
        @Param({"ROCKS_BASELINE", "ROCKS_OMNI"})
        public BackendMode backendMode = BackendMode.ROCKS_BASELINE;

        public final int numberOfElements = 1000;
        public DataStreamSource<IntegerLongSource.Record> source;
        private File checkpointDir;

        @Override
        public void setUp() throws Exception {
            try {
                checkpointDir = Files.createTempDirectory("bench-").toFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            super.setUp();

            String checkpointDataUri = "file://" + checkpointDir.getAbsolutePath();
            RocksDBStateBackend rocksBackend = new RocksDBStateBackend(checkpointDataUri, false);

            if (backendMode == BackendMode.ROCKS_OMNI) {
                // Enable OmniStateStore RocksDB options factory
                rocksBackend.setRocksDBOptions(
                        new com.huawei.falcon.state.RocksDBOptOptionsFactory());
            }

            env.setStateBackend(rocksBackend);

            if (backendMode == BackendMode.ROCKS_OMNI) {
                // Kryo optimization: register common collection types to avoid class name writes
                env.getConfig().registerKryoType(java.util.HashMap.class);
                env.getConfig().registerKryoType(java.util.ArrayList.class);
                env.getConfig().registerKryoType(java.util.LinkedHashMap.class);
                env.getConfig().registerKryoType(java.util.LinkedList.class);
                env.getConfig().registerKryoType(java.util.HashSet.class);
                env.getConfig().registerKryoType(java.util.TreeMap.class);
                env.getConfig().registerKryoType(java.util.TreeSet.class);
            }

            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            source = env.addSource(new IntegerLongSource(numberOfElements, RECORDS_PER_INVOCATION));
        }

        @Override
        protected Configuration createConfiguration() {
            Configuration configuration = super.createConfiguration();
            configuration.set(
                    RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE, MemorySize.parse("322122552b"));

            if (backendMode == BackendMode.ROCKS_OMNI) {
                // OmniStateStore Falcon config
                configuration.setString(
                        "state.backend.rocksdb.falcon.optimize-kryo-serialization", "true");
                configuration.setString(
                        "state.backend.rocksdb.falcon.write-buffer-size", "128mb");
                configuration.setString(
                        "state.backend.rocksdb.falcon.max-write-buffer-number", "4");
                configuration.setString(
                        "state.backend.rocksdb.falcon.min-write-buffer-number-to-merge", "2");
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
