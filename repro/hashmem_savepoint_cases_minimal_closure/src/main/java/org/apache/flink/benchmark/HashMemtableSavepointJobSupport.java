package org.apache.flink.benchmark;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

final class HashMemtableSavepointJobSupport {

    private HashMemtableSavepointJobSupport() {}

    static StreamExecutionEnvironment createEnvironment(
            ParameterTool params, int parallelism, long checkpointIntervalMs) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getConfig().setGlobalJobParameters(params);
        return env;
    }

    static String buildStateKey(long id) {
        return hex32(id)
                + "-state-key-"
                + reversedHex32(id)
                + "-abcdefghijklmnop";
    }

    static String buildMapKey(long stateKeyId, int entryId) {
        return "map-"
                + hex32(stateKeyId)
                + '-'
                + String.format(Locale.ROOT, "%02d", entryId)
                + "-sub-key-qrstuvwxyz";
    }

    static String buildPayload(long id, int version) {
        return "payload-"
                + hex32(id)
                + '-'
                + reversedHex32(id)
                + "-v"
                + String.format(Locale.ROOT, "%02d", version)
                + "-abcdefghijklmnopqrstuvwxyz-0123456789";
    }

    static String buildRmwFragment(long id, int updateId) {
        return "rmw-"
                + hex32(id)
                + '-'
                + reversedHex32(id + updateId)
                + "-u"
                + String.format(Locale.ROOT, "%02d", updateId)
                + "-abcdefghijklmnopqrstuvwx";
    }

    static String buildExpectedRmwValue(long id, int updatesPerKey) {
        StringBuilder builder = new StringBuilder();
        for (int updateId = 0; updateId < updatesPerKey; updateId++) {
            if (updateId > 0) {
                builder.append('|');
            }
            builder.append(buildRmwFragment(id, updateId));
        }
        return builder.toString();
    }

    static String buildJoinValue(long id) {
        return "join-dim-"
                + hex32(id)
                + '-'
                + reversedHex32(id)
                + "-abcdefghijklmnopqrstuvwxyz-0123456789";
    }

    static String safe(String value) {
        return value == null ? "<null>" : value;
    }

    private static String hex32(long value) {
        return String.format(Locale.ROOT, "%08x", value);
    }

    private static String reversedHex32(long value) {
        return String.format(Locale.ROOT, "%08x", Integer.reverse((int) value));
    }

    static final class AppendingSink extends RichSinkFunction<String> {
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
