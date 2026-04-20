package org.apache.flink.benchmark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class HashMemtableSavepointMapStateRiskJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String mode = params.get("mode", "load").toLowerCase(Locale.ROOT);
        int parallelism = params.getInt("parallelism", 1);
        int sourceParallelism = params.getInt("sourceParallelism", parallelism);
        long stateKeys = params.getLong("stateKeys", 100000L);
        int entriesPerKey = params.getInt("entriesPerKey", 5);
        long eventsPerSecond = params.getLong("eventsPerSecond", 50000L);
        long checkpointIntervalMs = params.getLong("checkpointIntervalMs", 3600_000L);
        String outputDir = params.getRequired("output");

        StreamExecutionEnvironment env =
                HashMemtableSavepointJobSupport.createEnvironment(
                        params, parallelism, checkpointIntervalMs);

        DataStream<MapEvent> stream =
                env.addSource(new MapStateSource(mode, stateKeys, entriesPerKey, eventsPerSecond))
                        .name("hashmem-mapstate-source")
                        .uid("hashmem-mapstate-source")
                        .setParallelism(sourceParallelism);

        stream.keyBy(event -> event.stateKey)
                .process(new MapStateVerifier(entriesPerKey))
                .name("hashmem-mapstate-process")
                .uid("hashmem-mapstate-process")
                .addSink(new HashMemtableSavepointJobSupport.AppendingSink(outputDir))
                .name("hashmem-mapstate-sink")
                .uid("hashmem-mapstate-sink")
                .setParallelism(1);

        env.execute("HashMemtableSavepointMapStateRiskJob-" + mode);
    }

    public static final class MapEvent implements Serializable {
        public long seq;
        public long stateKeyId;
        public String stateKey;
        public int entryId;
        public boolean verify;

        public MapEvent() {}

        MapEvent(long seq, long stateKeyId, String stateKey, int entryId, boolean verify) {
            this.seq = seq;
            this.stateKeyId = stateKeyId;
            this.stateKey = stateKey;
            this.entryId = entryId;
            this.verify = verify;
        }
    }

    public static final class MapStateSource extends RichParallelSourceFunction<MapEvent>
            implements CheckpointedFunction {
        private final String mode;
        private final long stateKeys;
        private final int entriesPerKey;
        private final long eventsPerSecond;

        private volatile boolean running = true;
        private long nextSeq;
        private transient ListState<Long> checkpointState;

        MapStateSource(String mode, long stateKeys, int entriesPerKey, long eventsPerSecond) {
            this.mode = mode;
            this.stateKeys = stateKeys;
            this.entriesPerKey = entriesPerKey;
            this.eventsPerSecond = eventsPerSecond;
        }

        @Override
        public void run(SourceContext<MapEvent> ctx) throws Exception {
            long totalEvents = "verify".equals(mode) ? stateKeys : stateKeys * entriesPerKey;
            long intervalNs = eventsPerSecond > 0 ? 1_000_000_000L / eventsPerSecond : 0L;
            long nextDeadline = System.nanoTime();

            while (running && nextSeq < totalEvents) {
                MapEvent event = buildEvent(nextSeq);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                    nextSeq++;
                }
                if (intervalNs > 0L) {
                    nextDeadline += intervalNs;
                    long sleepNs = nextDeadline - System.nanoTime();
                    if (sleepNs > 0L) {
                        LockSupport.parkNanos(sleepNs);
                    }
                }
            }

            if ("load".equals(mode)) {
                while (running) {
                    LockSupport.parkNanos(100_000_000L);
                }
            }
        }

        private MapEvent buildEvent(long seq) {
            if ("verify".equals(mode)) {
                long stateKeyId = seq;
                return new MapEvent(
                        seq,
                        stateKeyId,
                        HashMemtableSavepointJobSupport.buildStateKey(stateKeyId),
                        -1,
                        true);
            }

            long stateKeyId = seq / entriesPerKey;
            int entryId = (int) (seq % entriesPerKey);
            return new MapEvent(
                    seq,
                    stateKeyId,
                    HashMemtableSavepointJobSupport.buildStateKey(stateKeyId),
                    entryId,
                    false);
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointState.clear();
            checkpointState.add(nextSeq);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            checkpointState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("mapstate-next-seq", Long.class));
            if ("verify".equals(mode)) {
                nextSeq = 0L;
                return;
            }
            if (context.isRestored()) {
                Iterator<Long> iterator = checkpointState.get().iterator();
                nextSeq = iterator.hasNext() ? iterator.next() : 0L;
            } else {
                nextSeq = 0L;
            }
        }
    }

    public static final class MapStateVerifier
            extends KeyedProcessFunction<String, MapEvent, String> {
        private final int entriesPerKey;
        private transient MapState<String, String> mapState;

        MapStateVerifier(int entriesPerKey) {
            this.entriesPerKey = entriesPerKey;
        }

        @Override
        public void open(Configuration parameters) {
            mapState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "risk-map-state", String.class, String.class));
        }

        @Override
        public void processElement(MapEvent value, Context ctx, Collector<String> out) throws Exception {
            if (!value.verify) {
                String mapKey =
                        HashMemtableSavepointJobSupport.buildMapKey(value.stateKeyId, value.entryId);
                String payload =
                        HashMemtableSavepointJobSupport.buildPayload(value.stateKeyId, value.entryId);
                mapState.put(mapKey, payload);
                return;
            }

            int actualCount = 0;
            Iterable<Map.Entry<String, String>> entries = mapState.entries();
            for (Map.Entry<String, String> ignored : entries) {
                actualCount++;
            }
            if (actualCount != entriesPerKey) {
                out.collect(
                        value.seq
                                + "\t"
                                + value.stateKey
                                + "\t<size>\t"
                                + actualCount
                                + "\t"
                                + entriesPerKey);
            }

            for (int entryId = 0; entryId < entriesPerKey; entryId++) {
                String mapKey =
                        HashMemtableSavepointJobSupport.buildMapKey(value.stateKeyId, entryId);
                String expected =
                        HashMemtableSavepointJobSupport.buildPayload(value.stateKeyId, entryId);
                String actual = mapState.get(mapKey);
                if (!expected.equals(actual)) {
                    out.collect(
                            value.seq
                                    + "\t"
                                    + value.stateKey
                                    + "\t"
                                    + mapKey
                                    + "\t"
                                    + HashMemtableSavepointJobSupport.safe(actual)
                                    + "\t"
                                    + expected);
                }
            }
        }
    }
}
