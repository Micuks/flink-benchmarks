package org.apache.flink.benchmark;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
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
import java.util.concurrent.locks.LockSupport;

public class HashMemtableSavepointProbeJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        int parallelism = params.getInt("parallelism", 1);
        int sourceParallelism = params.getInt("sourceParallelism", parallelism);
        long events = params.getLong("events", 40000L);
        long eventsPerSecond = params.getLong("eventsPerSecond", 400L);
        int keys = params.getInt("keys", 8);
        long checkpointIntervalMs = params.getLong("checkpointIntervalMs", 5000L);
        String outputDir = params.getRequired("output");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> stream = env
                .addSource(new DeterministicSource(events, eventsPerSecond, keys))
                .name("probe-source")
                .uid("probe-source")
                .setParallelism(sourceParallelism);

        stream
                .keyBy(e -> e.key)
                .process(new ProbeProcessFunction())
                .name("probe-process")
                .uid("probe-process")
                .addSink(new AppendingSink(outputDir))
                .name("probe-sink")
                .uid("probe-sink")
                .setParallelism(1);

        env.execute("HashMemtableSavepointProbeJob");
    }

    public static final class Event implements Serializable {
        public long seq;
        public int key;
        public long delta;

        public Event() {}

        public Event(long seq, int key, long delta) {
            this.seq = seq;
            this.key = key;
            this.delta = delta;
        }
    }

    public static final class DeterministicSource extends RichParallelSourceFunction<Event>
            implements CheckpointedFunction {
        private final long totalEvents;
        private final long eventsPerSecond;
        private final int keys;

        private volatile boolean running = true;
        private long nextSeq;
        private transient ListState<Long> checkpointState;

        public DeterministicSource(long totalEvents, long eventsPerSecond, int keys) {
            this.totalEvents = totalEvents;
            this.eventsPerSecond = eventsPerSecond;
            this.keys = keys;
        }

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            final long intervalNs = eventsPerSecond > 0 ? 1_000_000_000L / eventsPerSecond : 0L;
            long nextDeadline = System.nanoTime();

            while (running && nextSeq < totalEvents) {
                final long current = nextSeq;
                final Event event = new Event(current, (int) (current % keys), current + 1);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                    nextSeq = current + 1;
                }
                if (intervalNs > 0L) {
                    nextDeadline += intervalNs;
                    long sleepNs = nextDeadline - System.nanoTime();
                    if (sleepNs > 0L) {
                        LockSupport.parkNanos(sleepNs);
                    }
                }
            }
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
                    .getListState(new ListStateDescriptor<>("next-seq", Long.class));
            if (context.isRestored()) {
                Iterator<Long> it = checkpointState.get().iterator();
                nextSeq = it.hasNext() ? it.next() : 0L;
            } else {
                nextSeq = 0L;
            }
        }
    }

    public static final class ProbeProcessFunction
            extends KeyedProcessFunction<Integer, Event, String> {
        private transient ValueState<Long> sum;
        private transient ValueState<Long> lastSeq;
        private transient ValueState<Long> xor;

        @Override
        public void open(Configuration parameters) {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum-state", Long.class));
            lastSeq = getRuntimeContext().getState(new ValueStateDescriptor<>("last-seq-state", Long.class));
            xor = getRuntimeContext().getState(new ValueStateDescriptor<>("xor-state", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            Long curSum = sum.value();
            Long curXor = xor.value();
            if (curSum == null) {
                curSum = 0L;
            }
            if (curXor == null) {
                curXor = 0L;
            }
            curSum += value.delta;
            curXor ^= value.delta;
            sum.update(curSum);
            lastSeq.update(value.seq);
            xor.update(curXor);
            out.collect(value.seq + "\t" + value.key + "\t" + curSum + "\t" + value.seq + "\t" + curXor);
        }
    }

    public static final class AppendingSink extends RichSinkFunction<String> {
        private final String outputDir;
        private transient BufferedWriter writer;
        private transient int buffered;

        public AppendingSink(String outputDir) {
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
