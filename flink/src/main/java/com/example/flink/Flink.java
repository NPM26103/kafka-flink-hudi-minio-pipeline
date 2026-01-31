package com.example.flink;

import com.example.common.Args;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// ✅ Dùng Flink shaded Jackson (KHÔNG dùng com.fasterxml.*)
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Flink {

    public static void main(String[] args) throws Exception {
        Args cfg = Args.parse(args);

        // hỗ trợ cả --bootstrap và --bootstrapServers
        String bootstrap = cfg.get("bootstrap", cfg.get("bootstrapServers", "localhost:9092"));
        String groupId   = cfg.get("groupId", "flink-group");

        boolean enableCsv  = cfg.getBool("ENABLE_CSV", false);
        boolean enableHttp = cfg.getBool("ENABLE_HTTP", false);
        if (!enableCsv && !enableHttp) enableCsv = true;

        String topicArg = cfg.get("topic", "");
        if (topicArg == null || topicArg.isBlank()) {
            throw new IllegalArgumentException("Missing --topic. Example: --topic=raw.student.csv or --topic=raw.student.csv,raw.student.http");
        }

        List<String> topics = Arrays.stream(topicArg.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());

        String outTopic = cfg.get("outTopic", "output-topic");
        String dlqTopic = cfg.get("dlqTopic", "output-topic-dlq");
        long checkpointMs = cfg.getLong("checkpointMs", 5000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointMs);

        // OutputTag tạo trong main rồi truyền vào function để tránh closure dính outer class
        final OutputTag<String> dlqTag = new OutputTag<>("dlq") {};

        DataStream<String> input = buildInput(env, bootstrap, groupId, enableCsv, enableHttp, topics);

        SingleOutputStreamOperator<String> ok = input.process(new ValidateAndComputePerformance(dlqTag));
        DataStream<String> dlq = ok.getSideOutput(dlqTag);

        KafkaSink<String> outSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(dlqTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        ok.sinkTo(outSink);
        dlq.sinkTo(dlqSink);

        env.execute("kafka -> flink -> kafka (union by ENABLE + --topic)");
    }

    private static DataStream<String> buildInput(
            StreamExecutionEnvironment env,
            String bootstrap,
            String groupId,
            boolean enableCsv,
            boolean enableHttp,
            List<String> topics
    ) {
        DataStream<String> s = null;

        String csvTopic  = topics.get(0);
        String httpTopic = topics.size() >= 2 ? topics.get(1) : topics.get(0);

        if (enableCsv) {
            KafkaSource<String> csvSource = KafkaSource.<String>builder()
                    .setBootstrapServers(bootstrap)
                    .setTopics(csvTopic)
                    .setGroupId(groupId)
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();
            s = env.fromSource(csvSource, WatermarkStrategy.noWatermarks(), "CSV Kafka Source");
        }

        if (enableHttp) {
            KafkaSource<String> httpSource = KafkaSource.<String>builder()
                    .setBootstrapServers(bootstrap)
                    .setTopics(httpTopic)
                    .setGroupId(groupId)
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();
            DataStream<String> s2 = env.fromSource(httpSource, WatermarkStrategy.noWatermarks(), "HTTP Kafka Source");
            s = (s == null) ? s2 : s.union(s2);
        }

        if (s == null) throw new IllegalStateException("No source enabled");
        return s;
    }

    static class ValidateAndComputePerformance extends ProcessFunction<String, String> {

        private final OutputTag<String> dlqTag;

        // ✅ transient để Flink serialize function OK
        private transient ObjectMapper mapper;

        ValidateAndComputePerformance(OutputTag<String> dlqTag) {
            this.dlqTag = dlqTag;
        }

        @Override
        public void open(Configuration parameters) {
            this.mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            ObjectNode n;
            try {
                n = parseObj(value);
            } catch (Exception e) {
                ctx.output(dlqTag, buildDlq(value, "INVALID_JSON", e.getMessage()));
                return;
            }

            // Nếu là record CSV (score) => xử lý đơn giản
            if (n.hasNonNull("score") && !n.hasNonNull("quiz_score")) {
                try {
                    double score = toDouble(n, "score");
                    if (score < 0) score = 0;
                    if (score > 100) score = 100;

                    long ts = System.currentTimeMillis();
                    n.put("performance_index", score);
                    n.put("_pipeline", "kafka->flink->kafka");
                    n.put("_processed_at", Instant.ofEpochMilli(ts).toString());
                    n.put("processed_ts_ms", ts);
                    n.put("ts_ms", ts);

                    out.collect(n.toString());
                } catch (Exception e) {
                    ctx.output(dlqTag, buildDlq(n.toString(), "CSV_VALIDATION_ERROR", e.getMessage()));
                }
                return;
            }

            // HTTP schema required fields
            String[] required = {
                    "student_id","week","study_hours","sleep_hours","stress_level","attendance_rate",
                    "screen_time_hours","caffeine_intake","learning_efficiency","fatigue_index",
                    "quiz_score","assignment_score"
            };
            for (String k : required) {
                if (!n.hasNonNull(k)) {
                    ctx.output(dlqTag, buildDlq(n.toString(), "MISSING_FIELD", "Missing: " + k));
                    return;
                }
            }

            try {
                long studentId = toLong(n, "student_id");
                int week = (int) toLong(n, "week");

                double studyHours = toDouble(n, "study_hours");
                double sleepHours = toDouble(n, "sleep_hours");
                double stress = toDouble(n, "stress_level");
                double attendance = toDouble(n, "attendance_rate");
                double screen = toDouble(n, "screen_time_hours");
                int caffeine = (int) toLong(n, "caffeine_intake");
                double learnEff = toDouble(n, "learning_efficiency");
                double fatigue = toDouble(n, "fatigue_index");
                double quiz = toDouble(n, "quiz_score");
                double assign = toDouble(n, "assignment_score");

                // normalize types
                n.put("student_id", studentId);
                n.put("week", week);
                n.put("study_hours", studyHours);
                n.put("sleep_hours", sleepHours);
                n.put("stress_level", stress);
                n.put("attendance_rate", attendance);
                n.put("screen_time_hours", screen);
                n.put("caffeine_intake", caffeine);
                n.put("learning_efficiency", learnEff);
                n.put("fatigue_index", fatigue);
                n.put("quiz_score", quiz);
                n.put("assignment_score", assign);

                double perf = 0.5 * quiz + 0.5 * assign + attendance * 10.0 + learnEff * 5.0
                        - stress * 1.0 - fatigue * 4.0;
                if (perf < 0) perf = 0;
                if (perf > 100) perf = 100;

                n.put("performance_index", perf);

                long ts = System.currentTimeMillis();
                n.put("_pipeline", "kafka->flink->kafka");
                n.put("_processed_at", Instant.ofEpochMilli(ts).toString());
                n.put("processed_ts_ms", ts);
                n.put("ts_ms", ts);

                out.collect(n.toString());
            } catch (Exception e) {
                ctx.output(dlqTag, buildDlq(n.toString(), "VALIDATION_ERROR", e.getMessage()));
            }
        }

        private ObjectNode parseObj(String s) {
            try {
                JsonNode node = mapper.readTree(s);
                if (node instanceof ObjectNode) return (ObjectNode) node;
                throw new RuntimeException("JSON is not an object");
            } catch (Exception e) {
                throw new RuntimeException("Invalid JSON: " + e.getMessage(), e);
            }
        }

        private String buildDlq(String raw, String code, String msg) {
            ObjectNode err = mapper.createObjectNode();
            err.put("_error_code", code);
            err.put("_error_message", msg);
            err.put("_raw", raw);
            err.put("_pipeline", "kafka->flink->kafka");
            err.put("_failed_at", Instant.now().toString());
            return err.toString();
        }

        private static long toLong(ObjectNode n, String k) {
            if (n.get(k).isNumber()) return n.get(k).asLong();
            return Long.parseLong(n.get(k).asText().trim());
        }

        private static double toDouble(ObjectNode n, String k) {
            if (n.get(k).isNumber()) return n.get(k).asDouble();
            return Double.parseDouble(n.get(k).asText().trim());
        }
    }
}
