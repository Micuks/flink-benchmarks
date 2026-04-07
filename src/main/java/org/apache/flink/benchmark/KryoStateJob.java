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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/**
 * Standalone Flink job entry for the Kryo state workload.
 *
 * <p>This is intentionally separate from {@link KryoStateBenchmark}, which is a JMH entry point
 * backed by an in-process MiniCluster. Submit this class with {@code flink run} to exercise an
 * external cluster such as the baseline/Falcon docker-compose environments.
 */
public final class KryoStateJob {

    private static final Logger LOG = LoggerFactory.getLogger(KryoStateJob.class);

    private static final long DEFAULT_EVENTS_NUM = 100_000_000L;
    private static final long DEFAULT_EVENTS_PER_SECOND = 10_000_000L;
    private static final int DEFAULT_PARALLELISM = 16;
    private static final int DEFAULT_NUMBER_OF_KEYS = 1_000;
    private static final int DEFAULT_MAP_ENTRIES = 20;
    private static final int DEFAULT_WINDOW_SECONDS = 5;
    private static final int DEFAULT_THROTTLE_BATCH_SIZE = 4_096;
    private static final int DEFAULT_MAX_VALUES_PER_KEY = 10;
    private static final int DEFAULT_LARGE_STATE_GET_EVERY =
            KryoStateBenchmark.LARGE_STATE_GET_EVERY;
    private static final int DEFAULT_LARGE_STATE_CLEAR_EVERY =
            KryoStateBenchmark.LARGE_STATE_CLEAR_EVERY;

    private KryoStateJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        JobMode mode = JobMode.fromString(params.get("mode", "map"));
        int parallelism = params.getInt("parallelism", DEFAULT_PARALLELISM);
        int sourceParallelism = params.getInt("sourceParallelism", parallelism);
        long eventsNum = params.getLong("events", DEFAULT_EVENTS_NUM);
        long eventsPerSecond = params.getLong("eventsPerSecond", DEFAULT_EVENTS_PER_SECOND);
        int numberOfKeys = params.getInt("keys", DEFAULT_NUMBER_OF_KEYS);
        int mapEntries = params.getInt("mapEntries", DEFAULT_MAP_ENTRIES);
        int windowSeconds = params.getInt("windowSeconds", DEFAULT_WINDOW_SECONDS);
        int throttleBatchSize = params.getInt("throttleBatchSize", DEFAULT_THROTTLE_BATCH_SIZE);
        int maxValuesPerKey =
                params.getInt(
                        "maxValuesPerKey",
                        mode == JobMode.KEYED_LIST_GET_ADD_BALANCED_MAP
                                ? KryoStateBenchmark.BALANCED_GET_ADD_MAX_VALUES_PER_KEY
                                : DEFAULT_MAX_VALUES_PER_KEY);
        int largeStateGetEvery = params.getInt("largeStateGetEvery", DEFAULT_LARGE_STATE_GET_EVERY);
        int largeStateClearEvery =
                params.getInt("largeStateClearEvery", DEFAULT_LARGE_STATE_CLEAR_EVERY);
        boolean reuseSourceRecords = params.getBoolean("reuseSourceRecords", false);
        boolean registerKryo = params.getBoolean("registerKryo", false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(parallelism);

        if (registerKryo) {
            env.getConfig().registerKryoType(HashMap.class);
            env.getConfig().registerKryoType(KryoStateBenchmark.MapRecord.class);
            env.getConfig().registerKryoType(KryoStateBenchmark.SimpleRecord.class);
        }

        LOG.info(
                "Starting KryoStateJob mode={}, parallelism={}, sourceParallelism={}, events={}, "
                        + "eventsPerSecond={}, keys={}, mapEntries={}, windowSeconds={}, "
                        + "maxValuesPerKey={}, largeStateGetEvery={}, largeStateClearEvery={}, "
                        + "reuseSourceRecords={}, registerKryo={}",
                mode,
                parallelism,
                sourceParallelism,
                eventsNum,
                eventsPerSecond,
                numberOfKeys,
                mapEntries,
                windowSeconds,
                maxValuesPerKey,
                largeStateGetEvery,
                largeStateClearEvery,
                reuseSourceRecords,
                registerKryo);

        switch (mode) {
            case WINDOW_PROCESS_MAP:
                runWindowProcessMap(
                        env,
                        sourceParallelism,
                        eventsNum,
                        eventsPerSecond,
                        numberOfKeys,
                        mapEntries,
                        windowSeconds,
                        throttleBatchSize,
                        reuseSourceRecords);
                break;
            case WINDOW_PROCESS_SIMPLE:
                runWindowProcessSimple(
                        env,
                        sourceParallelism,
                        eventsNum,
                        eventsPerSecond,
                        numberOfKeys,
                        windowSeconds,
                        throttleBatchSize);
                break;
            case WINDOW_REDUCE_MAP:
                runWindowReduceMap(
                        env,
                        sourceParallelism,
                        eventsNum,
                        eventsPerSecond,
                        numberOfKeys,
                        mapEntries,
                        windowSeconds,
                        throttleBatchSize,
                        reuseSourceRecords);
                break;
            case KEYED_LIST_GET_ADD_MAP:
                runKeyedListStateGetAddMap(
                        env,
                        sourceParallelism,
                        eventsNum,
                        eventsPerSecond,
                        numberOfKeys,
                        mapEntries,
                        throttleBatchSize,
                        reuseSourceRecords,
                        maxValuesPerKey);
                break;
            case KEYED_LIST_GET_ADD_BALANCED_MAP:
                runKeyedListStateGetAddMap(
                        env,
                        sourceParallelism,
                        eventsNum,
                        eventsPerSecond,
                        numberOfKeys,
                        mapEntries,
                        throttleBatchSize,
                        reuseSourceRecords,
                        maxValuesPerKey,
                        "keyed-list-state-get-add-balanced-map-pojo");
                break;
            case KEYED_LIST_GET_ADD_STATE_PAYLOAD_MAP:
                runKeyedListStateGetAddStatePayloadMap(
                        env,
                        sourceParallelism,
                        eventsNum,
                        eventsPerSecond,
                        numberOfKeys,
                        mapEntries,
                        throttleBatchSize,
                        maxValuesPerKey);
                break;
            case KEYED_LIST_LARGE_STATE_MAP:
                runKeyedListStateLargeStateMap(
                        env,
                        sourceParallelism,
                        eventsNum,
                        eventsPerSecond,
                        numberOfKeys,
                        mapEntries,
                        throttleBatchSize,
                        largeStateGetEvery,
                        largeStateClearEvery);
                break;
            default:
                throw new IllegalArgumentException("Unsupported mode: " + mode);
        }

        env.execute("KryoStateJob-" + mode.name().toLowerCase(Locale.ROOT));
    }

    private static void runWindowProcessMap(
            StreamExecutionEnvironment env,
            int sourceParallelism,
            long eventsNum,
            long eventsPerSecond,
            int numberOfKeys,
            int mapEntries,
            int windowSeconds,
            int throttleBatchSize,
            boolean reuseSourceRecords) {
        DataStream<KryoStateBenchmark.MapRecord> source =
                env.addSource(
                                new RateLimitedMapRecordSource(
                                        eventsNum,
                                        eventsPerSecond,
                                        throttleBatchSize,
                                        numberOfKeys,
                                        mapEntries,
                                        reuseSourceRecords))
                        .name("rate-limited-map-record-source")
                        .setParallelism(sourceParallelism);

        source.keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
                .process(
                        new KryoStateBenchmark.SumProcessFunction<KryoStateBenchmark.MapRecord>() {
                            @Override
                            protected long extractValue(KryoStateBenchmark.MapRecord r) {
                                return r.value;
                            }
                        })
                .name("window-process-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");
    }

    private static void runWindowProcessSimple(
            StreamExecutionEnvironment env,
            int sourceParallelism,
            long eventsNum,
            long eventsPerSecond,
            int numberOfKeys,
            int windowSeconds,
            int throttleBatchSize) {
        DataStream<KryoStateBenchmark.SimpleRecord> source =
                env.addSource(
                                new RateLimitedSimpleRecordSource(
                                        eventsNum,
                                        eventsPerSecond,
                                        throttleBatchSize,
                                        numberOfKeys))
                        .name("rate-limited-simple-record-source")
                        .setParallelism(sourceParallelism);

        source.keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
                .process(
                        new KryoStateBenchmark.SumProcessFunction<
                                KryoStateBenchmark.SimpleRecord>() {
                            @Override
                            protected long extractValue(KryoStateBenchmark.SimpleRecord r) {
                                return r.value;
                            }
                        })
                .name("window-process-simple")
                .addSink(new DiscardingSink<>())
                .name("discard");
    }

    private static void runWindowReduceMap(
            StreamExecutionEnvironment env,
            int sourceParallelism,
            long eventsNum,
            long eventsPerSecond,
            int numberOfKeys,
            int mapEntries,
            int windowSeconds,
            int throttleBatchSize,
            boolean reuseSourceRecords) {
        DataStream<KryoStateBenchmark.MapRecord> source =
                env.addSource(
                                new RateLimitedMapRecordSource(
                                        eventsNum,
                                        eventsPerSecond,
                                        throttleBatchSize,
                                        numberOfKeys,
                                        mapEntries,
                                        reuseSourceRecords))
                        .name("rate-limited-map-record-source")
                        .setParallelism(sourceParallelism);

        source.keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
                .reduce(new KryoStateBenchmark.MapRecordReduceFunction())
                .name("window-reduce-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");
    }

    private static void runKeyedListStateGetAddMap(
            StreamExecutionEnvironment env,
            int sourceParallelism,
            long eventsNum,
            long eventsPerSecond,
            int numberOfKeys,
            int mapEntries,
            int throttleBatchSize,
            boolean reuseSourceRecords,
            int maxValuesPerKey) {
        runKeyedListStateGetAddMap(
                env,
                sourceParallelism,
                eventsNum,
                eventsPerSecond,
                numberOfKeys,
                mapEntries,
                throttleBatchSize,
                reuseSourceRecords,
                maxValuesPerKey,
                "keyed-list-state-get-add-map-pojo");
    }

    private static void runKeyedListStateGetAddMap(
            StreamExecutionEnvironment env,
            int sourceParallelism,
            long eventsNum,
            long eventsPerSecond,
            int numberOfKeys,
            int mapEntries,
            int throttleBatchSize,
            boolean reuseSourceRecords,
            int maxValuesPerKey,
            String operatorName) {
        DataStream<KryoStateBenchmark.MapRecord> source =
                env.addSource(
                                new RateLimitedMapRecordSource(
                                        eventsNum,
                                        eventsPerSecond,
                                        throttleBatchSize,
                                        numberOfKeys,
                                        mapEntries,
                                        reuseSourceRecords))
                        .name("rate-limited-map-record-source")
                        .setParallelism(sourceParallelism);

        source.keyBy(r -> r.key)
                .process(
                        new KryoStateBenchmark.ListStateGetAddProcessFunction(
                                numberOfKeys, maxValuesPerKey))
                .name(operatorName)
                .addSink(new DiscardingSink<>())
                .name("discard");
    }

    private static void runKeyedListStateGetAddStatePayloadMap(
            StreamExecutionEnvironment env,
            int sourceParallelism,
            long eventsNum,
            long eventsPerSecond,
            int numberOfKeys,
            int mapEntries,
            int throttleBatchSize,
            int maxValuesPerKey) {
        DataStream<KryoStateBenchmark.SimpleRecord> source =
                env.addSource(
                                new RateLimitedSimpleRecordSource(
                                        eventsNum,
                                        eventsPerSecond,
                                        throttleBatchSize,
                                        numberOfKeys))
                        .name("rate-limited-simple-record-source")
                        .setParallelism(sourceParallelism);

        source.keyBy(r -> r.key)
                .process(
                        new KryoStateBenchmark.ListStateGetAddStatePayloadProcessFunction(
                                numberOfKeys, maxValuesPerKey, mapEntries))
                .name("keyed-list-state-get-add-state-payload-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");
    }

    private static void runKeyedListStateLargeStateMap(
            StreamExecutionEnvironment env,
            int sourceParallelism,
            long eventsNum,
            long eventsPerSecond,
            int numberOfKeys,
            int mapEntries,
            int throttleBatchSize,
            int largeStateGetEvery,
            int largeStateClearEvery) {
        DataStream<KryoStateBenchmark.SimpleRecord> source =
                env.addSource(
                                new RateLimitedSimpleRecordSource(
                                        eventsNum,
                                        eventsPerSecond,
                                        throttleBatchSize,
                                        numberOfKeys))
                        .name("rate-limited-simple-record-source")
                        .setParallelism(sourceParallelism);

        source.keyBy(r -> r.key)
                .process(
                        new KryoStateBenchmark.ListStateLargeStateProcessFunction(
                                numberOfKeys,
                                mapEntries,
                                largeStateGetEvery,
                                largeStateClearEvery))
                .name("keyed-list-state-large-state-map-pojo")
                .addSink(new DiscardingSink<>())
                .name("discard");
    }

    private enum JobMode {
        WINDOW_PROCESS_MAP,
        WINDOW_PROCESS_SIMPLE,
        WINDOW_REDUCE_MAP,
        KEYED_LIST_GET_ADD_MAP,
        KEYED_LIST_GET_ADD_BALANCED_MAP,
        KEYED_LIST_GET_ADD_STATE_PAYLOAD_MAP,
        KEYED_LIST_LARGE_STATE_MAP;

        private static JobMode fromString(String value) {
            String normalized = value.trim().replace('-', '_').toUpperCase(Locale.ROOT);
            switch (normalized) {
                case "MAP":
                case "WINDOW_PROCESS_MAP":
                case "WINDOW_PROCESS_MAP_POJO":
                    return WINDOW_PROCESS_MAP;
                case "SIMPLE":
                case "WINDOW_PROCESS_SIMPLE":
                    return WINDOW_PROCESS_SIMPLE;
                case "REDUCE":
                case "WINDOW_REDUCE_MAP":
                case "WINDOW_REDUCE_MAP_POJO":
                    return WINDOW_REDUCE_MAP;
                case "GET_ADD":
                case "KEYED_GET_ADD":
                case "KEYED_LIST_GET_ADD":
                case "KEYED_LIST_GET_ADD_MAP":
                case "KEYED_LIST_GET_ADD_MAP_POJO":
                    return KEYED_LIST_GET_ADD_MAP;
                case "GET_ADD_BALANCED":
                case "BALANCED_GET_ADD":
                case "KEYED_GET_ADD_BALANCED":
                case "KEYED_LIST_GET_ADD_BALANCED":
                case "KEYED_LIST_GET_ADD_BALANCED_MAP":
                case "KEYED_LIST_GET_ADD_BALANCED_MAP_POJO":
                    return KEYED_LIST_GET_ADD_BALANCED_MAP;
                case "GET_ADD_STATE_PAYLOAD":
                case "STATE_PAYLOAD":
                case "KEYED_GET_ADD_STATE_PAYLOAD":
                case "KEYED_LIST_GET_ADD_STATE_PAYLOAD":
                case "KEYED_LIST_GET_ADD_STATE_PAYLOAD_MAP":
                case "KEYED_LIST_GET_ADD_STATE_PAYLOAD_MAP_POJO":
                    return KEYED_LIST_GET_ADD_STATE_PAYLOAD_MAP;
                case "LARGE_STATE":
                case "LARGE_STATE_MAP":
                case "LARGE_STATE_MAP_POJO":
                case "KEYED_LARGE_STATE":
                case "KEYED_LIST_LARGE_STATE":
                case "KEYED_LIST_LARGE_STATE_MAP":
                case "KEYED_LIST_LARGE_STATE_MAP_POJO":
                case "GET_ADD_LARGE_STATE":
                    return KEYED_LIST_LARGE_STATE_MAP;
                default:
                    throw new IllegalArgumentException(
                            "Unknown mode '"
                                    + value
                                    + "'. Supported values: map, simple, reduce, get-add, get-add-balanced, get-add-state-payload, large-state.");
            }
        }
    }

    private abstract static class BoundedRateLimitedSource<T> extends RichParallelSourceFunction<T> {
        private static final long serialVersionUID = 1L;

        private final long totalEvents;
        private final long totalEventsPerSecond;
        private final int throttleBatchSize;

        private volatile boolean running = true;
        private transient long assignedEvents;
        private transient long firstEventId;
        private transient double subtaskEventsPerSecond;

        private BoundedRateLimitedSource(
                long totalEvents, long totalEventsPerSecond, int throttleBatchSize) {
            this.totalEvents = totalEvents;
            this.totalEventsPerSecond = totalEventsPerSecond;
            this.throttleBatchSize = Math.max(1, throttleBatchSize);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            long baseEvents = totalEvents / parallelism;
            long remainder = totalEvents % parallelism;

            assignedEvents = baseEvents + (subtask < remainder ? 1 : 0);
            firstEventId = baseEvents * subtask + Math.min(subtask, remainder);
            subtaskEventsPerSecond =
                    totalEventsPerSecond <= 0 ? 0.0d : totalEventsPerSecond / (double) parallelism;

            LOG.info(
                    "{} assigned firstEventId={}, events={}, ratePerSecond={}",
                    getRuntimeContext().getTaskNameWithSubtasks(),
                    firstEventId,
                    assignedEvents,
                    subtaskEventsPerSecond);
        }

        @Override
        public void run(SourceContext<T> ctx) {
            long emitted = 0L;
            long startNanos = System.nanoTime();

            while (running && emitted < assignedEvents) {
                long eventId = firstEventId + emitted;
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(createRecord(eventId));
                }
                emitted++;
                throttle(emitted, startNanos);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        protected abstract T createRecord(long eventId);

        private void throttle(long emitted, long startNanos) {
            if (subtaskEventsPerSecond <= 0.0d || emitted % throttleBatchSize != 0) {
                return;
            }

            long targetElapsedNanos = (long) ((emitted / subtaskEventsPerSecond) * 1_000_000_000L);
            long sleepNanos = startNanos + targetElapsedNanos - System.nanoTime();
            if (sleepNanos > 0L) {
                LockSupport.parkNanos(sleepNanos);
            }
        }
    }

    private static final class RateLimitedMapRecordSource
            extends BoundedRateLimitedSource<KryoStateBenchmark.MapRecord> {
        private static final long serialVersionUID = 1L;

        private final int numKeys;
        private final int mapEntries;
        private final boolean reuseRecords;
        private transient KryoStateBenchmark.MapRecord[] reusableRecords;

        private RateLimitedMapRecordSource(
                long totalEvents,
                long totalEventsPerSecond,
                int throttleBatchSize,
                int numKeys,
                int mapEntries,
                boolean reuseRecords) {
            super(totalEvents, totalEventsPerSecond, throttleBatchSize);
            this.numKeys = numKeys;
            this.mapEntries = mapEntries;
            this.reuseRecords = reuseRecords;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (reuseRecords) {
                reusableRecords = new KryoStateBenchmark.MapRecord[numKeys];
                for (int i = 0; i < numKeys; i++) {
                    reusableRecords[i] = createMapRecord(i, 10_000L + i, mapEntries);
                }
            }
        }

        @Override
        protected KryoStateBenchmark.MapRecord createRecord(long eventId) {
            int key = (int) (eventId % numKeys);
            return reuseRecords ? reusableRecords[key] : createMapRecord(key, eventId, mapEntries);
        }
    }

    private static final class RateLimitedSimpleRecordSource
            extends BoundedRateLimitedSource<KryoStateBenchmark.SimpleRecord> {
        private static final long serialVersionUID = 1L;

        private final int numKeys;

        private RateLimitedSimpleRecordSource(
                long totalEvents, long totalEventsPerSecond, int throttleBatchSize, int numKeys) {
            super(totalEvents, totalEventsPerSecond, throttleBatchSize);
            this.numKeys = numKeys;
        }

        @Override
        protected KryoStateBenchmark.SimpleRecord createRecord(long eventId) {
            return new KryoStateBenchmark.SimpleRecord((int) (eventId % numKeys), eventId);
        }
    }

    private static KryoStateBenchmark.MapRecord createMapRecord(
            int key, long value, int mapEntries) {
        Map<String, Long> counters = new HashMap<>(mapEntries * 2);
        for (int i = 0; i < mapEntries; i++) {
            counters.put("field_" + i, value + i);
        }
        return new KryoStateBenchmark.MapRecord(key, value, counters);
    }
}
