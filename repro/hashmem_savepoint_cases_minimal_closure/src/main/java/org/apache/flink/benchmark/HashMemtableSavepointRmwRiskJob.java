package org.apache.flink.benchmark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import java.util.concurrent.locks.LockSupport;

public class HashMemtableSavepointRmwRiskJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String mode = params.get("mode", "load").toLowerCase(Locale.ROOT);
        int parallelism = params.getInt("parallelism", 1);
        int sourceParallelism = params.getInt("sourceParallelism", parallelism);
        long keys = params.getLong("keys", 250000L);
        int updatesPerKey = params.getInt("updatesPerKey", 2);
        long eventsPerSecond = params.getLong("eventsPerSecond", 50000L);
        long checkpointIntervalMs = params.getLong("checkpointIntervalMs", 3600_000L);
        String outputDir = params.getRequired("output");

        StreamExecutionEnvironment env =
                HashMemtableSavepointJobSupport.createEnvironment(
                        params, parallelism, checkpointIntervalMs);

        DataStream<RmwEvent> stream =
                env.addSource(new RmwSource(mode, keys, updatesPerKey, eventsPerSecond))
                        .name("hashmem-rmw-source")
                        .uid("hashmem-rmw-source")
                        .setParallelism(sourceParallelism);

        stream.keyBy(event -> event.key)
                .process(new RmwVerifier())
                .name("hashmem-rmw-process")
                .uid("hashmem-rmw-process")
                .addSink(new HashMemtableSavepointJobSupport.AppendingSink(outputDir))
                .name("hashmem-rmw-sink")
                .uid("hashmem-rmw-sink")
                .setParallelism(1);

        env.execute("HashMemtableSavepointRmwRiskJob-" + mode);
    }

    public static final class RmwEvent implements Serializable {
        public long seq;
        public long keyId;
        public String key;
        public String payload;
        public boolean verify;

        public RmwEvent() {}

        RmwEvent(long seq, long keyId, String key, String payload, boolean verify) {
            this.seq = seq;
            this.keyId = keyId;
            this.key = key;
            this.payload = payload;
            this.verify = verify;
        }
    }

    public static final class RmwSource extends RichParallelSourceFunction<RmwEvent>
            implements CheckpointedFunction {
        private final String mode;
        private final long keys;
        private final int updatesPerKey;
        private final long eventsPerSecond;

        private volatile boolean running = true;
        private long nextSeq;
        private transient ListState<Long> checkpointState;

        RmwSource(String mode, long keys, int updatesPerKey, long eventsPerSecond) {
            this.mode = mode;
            this.keys = keys;
            this.updatesPerKey = updatesPerKey;
            this.eventsPerSecond = eventsPerSecond;
        }

        @Override
        public void run(SourceContext<RmwEvent> ctx) throws Exception {
            long totalEvents = "verify".equals(mode) ? keys : keys * updatesPerKey;
            long intervalNs = eventsPerSecond > 0 ? 1_000_000_000L / eventsPerSecond : 0L;
            long nextDeadline = System.nanoTime();

            while (running && nextSeq < totalEvents) {
                RmwEvent event = buildEvent(nextSeq);
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

        private RmwEvent buildEvent(long seq) {
            if ("verify".equals(mode)) {
                long keyId = seq;
                return new RmwEvent(
                        seq,
                        keyId,
                        HashMemtableSavepointJobSupport.buildStateKey(keyId),
                        HashMemtableSavepointJobSupport.buildExpectedRmwValue(keyId, updatesPerKey),
                        true);
            }

            int updateId = (int) (seq / keys);
            long keyId = seq % keys;
            return new RmwEvent(
                    seq,
                    keyId,
                    HashMemtableSavepointJobSupport.buildStateKey(keyId),
                    HashMemtableSavepointJobSupport.buildRmwFragment(keyId, updateId),
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
                            .getListState(new ListStateDescriptor<>("rmw-next-seq", Long.class));
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

    public static final class RmwVerifier
            extends KeyedProcessFunction<String, RmwEvent, String> {
        private transient ValueState<String> state;

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("rmw-state", String.class));
        }

        @Override
        public void processElement(RmwEvent value, Context ctx, Collector<String> out) throws Exception {
            if (!value.verify) {
                String current = state.value();
                String next = current == null ? value.payload : current + '|' + value.payload;
                state.update(next);
                return;
            }

            String actual = state.value();
            if (!value.payload.equals(actual)) {
                out.collect(
                        value.seq
                                + "\t"
                                + value.key
                                + "\t"
                                + HashMemtableSavepointJobSupport.safe(actual)
                                + "\t"
                                + value.payload);
            }
        }
    }
}
