package org.apache.flink.benchmark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.locks.LockSupport;

public class HashMemtableSavepointRiskJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String mode = params.get("mode", "load").toLowerCase(Locale.ROOT);
        int parallelism = params.getInt("parallelism", 1);
        int sourceParallelism = params.getInt("sourceParallelism", parallelism);
        long keys = params.getLong("keys", 50000L);
        long eventsPerSecond = params.getLong("eventsPerSecond", 50000L);
        long checkpointIntervalMs = params.getLong("checkpointIntervalMs", 3600_000L);
        String outputDir = params.getRequired("output");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> stream = env
                .addSource(new PhaseSource(mode, keys, eventsPerSecond))
                .name("risk-source")
                .uid("risk-source")
                .setParallelism(sourceParallelism);

        stream
                .keyBy(event -> event.key)
                .process(new ValueStateVerifier())
                .name("risk-process")
                .uid("risk-process")
                .addSink(new AppendingSink(outputDir))
                .name("risk-sink")
                .uid("risk-sink")
                .setParallelism(1);

        env.execute("HashMemtableSavepointRiskJob-" + mode);
    }

    public static final class Event implements Serializable {
        public long seq;
        public String key;
        public String payload;
        public boolean verify;

        public Event() {}

        Event(long seq, String key, String payload, boolean verify) {
            this.seq = seq;
            this.key = key;
            this.payload = payload;
            this.verify = verify;
        }
    }

    public static final class PhaseSource extends RichParallelSourceFunction<Event>
            implements CheckpointedFunction {
        private final String mode;
        private final long keys;
        private final long eventsPerSecond;

        private volatile boolean running = true;
        private long nextSeq;
        private transient ListState<Long> checkpointState;

        PhaseSource(String mode, long keys, long eventsPerSecond) {
            this.mode = mode;
            this.keys = keys;
            this.eventsPerSecond = eventsPerSecond;
        }

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            final long intervalNs = eventsPerSecond > 0 ? 1_000_000_000L / eventsPerSecond : 0L;
            long nextDeadline = System.nanoTime();

            while (running && nextSeq < keys) {
                Event event = buildEvent(nextSeq, "verify".equals(mode));
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

        private static Event buildEvent(long seq, boolean verify) {
            String head = String.format(Locale.ROOT, "%08x", seq);
            String tail = String.format(Locale.ROOT, "%08x", Integer.reverse((int) seq));
            String key = head + "-state-key-" + tail + "-abcdefghijklmnop";
            String payload = "payload-" + head + '-' + tail + "-abcdefghijklmnopqrstuvwxyz-0123456789";
            return new Event(seq, key, payload, verify);
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
            checkpointState = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("risk-next-seq", Long.class));
            if ("verify".equals(mode)) {
                nextSeq = 0L;
                return;
            }
            if (context.isRestored()) {
                Iterator<Long> it = checkpointState.get().iterator();
                nextSeq = it.hasNext() ? it.next() : 0L;
            } else {
                nextSeq = 0L;
            }
        }
    }

    public static final class ValueStateVerifier
            extends KeyedProcessFunction<String, Event, String> {
        private transient ValueState<String> state;

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("risk-payload", String.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            if (!value.verify) {
                state.update(value.payload);
                return;
            }
            String actual = state.value();
            if (!value.payload.equals(actual)) {
                out.collect(value.seq
                        + "\t" + value.key
                        + "\t" + safe(actual)
                        + "\t" + value.payload);
            }
        }

        private static String safe(String value) {
            return value == null ? "<null>" : value;
        }
    }

    public static final class AppendingSink extends RichSinkFunction<String> {
        private final String outputDir;
        private transient BufferedWriter writer;
        private transient int buffered;

        AppendingSink(String outputDir) {
            this.outputDir = outputDir;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            File dir = new File(outputDir);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IOException("Failed to create output dir: " + outputDir);
            }
            File file = new File(dir, "part-" + getRuntimeContext().getIndexOfThisSubtask() + ".tsv");
            writer = new BufferedWriter(new FileWriter(file, StandardCharsets.UTF_8, true));
            buffered = 0;
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            writer.write(value);
            writer.newLine();
            buffered++;
            if (buffered >= 128) {
                writer.flush();
                buffered = 0;
            }
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }
}
