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

        // basePath should be s3a://bucket/path (MinIO)
        String basePathArg = p.get("basePath", p.get("hudiPath", "s3a://hudi/student_perf"));
        String hudiPath = basePathArg;

        String hudiTable = p.get("hudiTable", "student_perf");

        // Hive sync settings
        String hiveDb = p.get("hiveDb", "hudi");
        String hiveTable = p.get("hiveTable", hudiTable);
        String metastoreUris = p.get("hmsUris", "thrift://hive-metastore:9083");

        int  parallelism  = (int) p.getLong("parallelism", 1);
        long checkpointMs = p.getLong("checkpointMs", 30000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().getConfiguration().setString("parallelism.default", String.valueOf(parallelism));
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", checkpointMs + " ms");
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.mode", "EXACTLY_ONCE");

        // Kafka source table
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

        // MinIO + Hive Sync
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
                "\n" +
                "  -- FIX enum for Hudi 1.1.1\n" +
                "  'hoodie.filesystem.view.type' = 'MEMORY',\n" +
                "  'hoodie.metadata.enable' = 'false',\n" +
                "\n" +
                "  -- ========= Hive Sync (HMS) =========\n" +
                "  'hoodie.datasource.hive_sync.enable' = 'true',\n" +
                "  'hoodie.datasource.hive_sync.mode' = 'hms',\n" +
                "  'hoodie.datasource.hive_sync.use_jdbc' = 'false',\n" +
                "  'hoodie.datasource.hive_sync.metastore.uris' = '" + metastoreUris + "',\n" +
                "  'hoodie.datasource.hive_sync.auto_create_database' = 'true',\n" +
                "  'hoodie.datasource.hive_sync.database' = '" + hiveDb + "',\n" +
                "  'hoodie.datasource.hive_sync.table' = '" + hiveTable + "',\n" +
                "  'hoodie.datasource.hive_sync.partition_fields' = 'week',\n" +
                "  'hoodie.datasource.write.hive_style_partitioning' = 'true',\n" +
                "  'hoodie.datasource.hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'\n" +
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

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS default_database"); // safe no-op
        tEnv.executeSql(createKafkaSource);
        tEnv.executeSql(createHudiSink);

        TableResult r = tEnv.executeSql(insertSql);
        r.getJobClient().ifPresent(j -> System.out.println("Job submitted: " + j.getJobID()));
        r.getJobClient().get().getJobExecutionResult().get();
    }
}
