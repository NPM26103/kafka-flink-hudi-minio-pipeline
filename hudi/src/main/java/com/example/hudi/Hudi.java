package com.example.hudi;

import com.example.common.Args;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class Hudi {
    public static void main(String[] args) throws Exception {
        Args p = Args.parse(args);

        String bootstrap = p.get("bootstrap", "broker:9092");
        String inTopic   = p.get("topic", "output-topic");
        String groupId   = p.get("groupId", "hudi-writer");

        // accept both --hudiPath and --basePath
        String hudiPathArg = p.get("hudiPath", null);
        String basePathArg = p.get("basePath", null);

        // default to MinIO via s3a (NO local)
        String hudiPath = (basePathArg != null && !basePathArg.isBlank())
                ? basePathArg
                : (hudiPathArg != null && !hudiPathArg.isBlank()
                    ? hudiPathArg
                    : "s3a://hudi/student_perf");

        // only prefix file:// if it's a local absolute path
        if (hudiPath.startsWith("/") && !hudiPath.startsWith("file://")) {
            hudiPath = "file://" + hudiPath;
        }
        // if it's s3a:// or s3:// or file:// ... keep as-is

        String hudiTable = p.get("hudiTable", "student_perf");

        int  parallelism   = (int) p.getLong("parallelism", 1);
        long checkpointMs  = p.getLong("checkpointMs", 30000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().getConfiguration().setString("parallelism.default", String.valueOf(parallelism));
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", checkpointMs + " ms");
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.mode", "EXACTLY_ONCE");

        String createKafkaSource =
                "CREATE TABLE kafka_students (\n" +
                "  student_id BIGINT,\n" +
                "  week INT,\n" +
                "  study_hours DOUBLE,\n" +
                "  sleep_hours DOUBLE,\n" +
                "  stress_level DOUBLE,\n" +
                "  attendance_rate DOUBLE,\n" +
                "  screen_time_hours DOUBLE,\n" +
                "  caffeine_intake INT,\n" +
                "  learning_efficiency DOUBLE,\n" +
                "  fatigue_index DOUBLE,\n" +
                "  quiz_score DOUBLE,\n" +
                "  assignment_score DOUBLE,\n" +
                "  performance_index DOUBLE,\n" +
                "  _source STRING,\n" +
                "  _ingested_at STRING,\n" +
                "  _pipeline STRING,\n" +
                "  _processed_at STRING,\n" +
                "  processed_ts_ms BIGINT,\n" +
                "  ts_ms BIGINT,\n" +
                "  ts_ms_fixed AS COALESCE(processed_ts_ms, ts_ms, CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT))\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + inTopic + "',\n" +
                "  'properties.bootstrap.servers' = '" + bootstrap + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'json.fail-on-missing-field' = 'false'\n" +
                ")";

        String createHudiSink =
                "CREATE TABLE hudi_students (\n" +
                "  student_id BIGINT,\n" +
                "  week INT,\n" +
                "  study_hours DOUBLE,\n" +
                "  sleep_hours DOUBLE,\n" +
                "  stress_level DOUBLE,\n" +
                "  attendance_rate DOUBLE,\n" +
                "  screen_time_hours DOUBLE,\n" +
                "  caffeine_intake INT,\n" +
                "  learning_efficiency DOUBLE,\n" +
                "  fatigue_index DOUBLE,\n" +
                "  quiz_score DOUBLE,\n" +
                "  assignment_score DOUBLE,\n" +
                "  performance_index DOUBLE,\n" +
                "  _source STRING,\n" +
                "  _ingested_at STRING,\n" +
                "  _pipeline STRING,\n" +
                "  _processed_at STRING,\n" +
                "  ts_ms BIGINT,\n" +
                "  PRIMARY KEY (student_id, week) NOT ENFORCED\n" +
                ") PARTITIONED BY (week)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = '" + hudiPath + "',\n" +
                "  'hoodie.table.name' = '" + hudiTable + "',\n" +
                "  'table.type' = 'COPY_ON_WRITE',\n" +
                "  'write.operation' = 'upsert',\n" +
                "  'hoodie.datasource.write.recordkey.field' = 'student_id,week',\n" +
                "  'hoodie.datasource.write.partitionpath.field' = 'week',\n" +
                "  'hoodie.datasource.write.precombine.field' = 'ts_ms',\n" +
                // ✅ FIX: value hợp lệ cho Hudi 1.1.1
                "  'hoodie.filesystem.view.type' = 'MEMORY',\n" +
                // demo đơn giản
                "  'hoodie.metadata.enable' = 'false'\n" +
                ")";

        String insertSql =
                "INSERT INTO hudi_students\n" +
                "SELECT\n" +
                "  student_id, week, study_hours, sleep_hours, stress_level,\n" +
                "  attendance_rate, screen_time_hours, caffeine_intake,\n" +
                "  learning_efficiency, fatigue_index, quiz_score, assignment_score,\n" +
                "  performance_index, _source, _ingested_at, _pipeline, _processed_at,\n" +
                "  ts_ms_fixed AS ts_ms\n" +
                "FROM kafka_students\n" +
                "WHERE student_id IS NOT NULL AND week IS NOT NULL";

        tEnv.executeSql(createKafkaSource);
        tEnv.executeSql(createHudiSink);

        TableResult r = tEnv.executeSql(insertSql);
        r.getJobClient().ifPresent(j -> System.out.println("Job submitted: " + j.getJobID()));

        r.getJobClient().get().getJobExecutionResult().get();
    }
}
