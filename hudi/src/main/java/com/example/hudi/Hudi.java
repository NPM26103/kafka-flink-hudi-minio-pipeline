package com.example.hudi;

import com.example.common.Args;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hudi {

    private static final Logger log = LoggerFactory.getLogger(Hudi.class);

    public static void main(String[] args) throws Exception {
        Args p = Args.parse(args);

        String bootstrap = p.get("bootstrap", "broker:9092");
        String inTopic   = p.get("topic", "output-topic");
        String groupId   = p.get("groupId", "hudi-writer");

        String hudiPath  = p.get("basePath", p.get("hudiPath", "s3a://hudi/student_perf"));
        String hudiTable = p.get("hudiTable", "student_perf");

        String hiveDb        = p.get("hiveDb", "hudi");
        String hiveTable     = p.get("hiveTable", hudiTable);
        String metastoreUris = p.get("hmsUris", "thrift://hive-metastore:9083");

        int  parallelism  = (int) p.getLong("parallelism", 1);
        long checkpointMs = p.getLong("checkpointMs", 30000);

        TableEnvironment tEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        tEnv.getConfig().getConfiguration().setString("parallelism.default", String.valueOf(parallelism));
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.interval", checkpointMs + " ms");
        tEnv.getConfig().getConfiguration().setString("execution.checkpointing.mode", "EXACTLY_ONCE");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS default_database"); // safe no-op

        tEnv.executeSql(kafkaSourceSql(inTopic, bootstrap, groupId));
        tEnv.executeSql(hudiSinkSql(hudiPath, hudiTable, hiveDb, hiveTable, metastoreUris));

        TableResult r = tEnv.executeSql(insertSql());
        r.getJobClient().ifPresent(j -> log.info("Job submitted: {}", j.getJobID()));
        r.getJobClient().get().getJobExecutionResult().get();
    }

    private static String kafkaSourceSql(String inTopic, String bootstrap, String groupId) {
        return """
                CREATE TABLE kafka_students (
                  student_id BIGINT,
                  week INT,
                  study_hours DOUBLE,
                  sleep_hours DOUBLE,
                  stress_level DOUBLE,
                  attendance_rate DOUBLE,
                  screen_time_hours DOUBLE,
                  caffeine_intake INT,
                  learning_efficiency DOUBLE,
                  fatigue_index DOUBLE,
                  quiz_score DOUBLE,
                  assignment_score DOUBLE,
                  performance_index DOUBLE,
                  _source STRING,
                  _ingested_at STRING,
                  _pipeline STRING,
                  _processed_at STRING,
                  processed_ts_ms BIGINT,
                  ts_ms BIGINT,
                  ts_ms_fixed AS COALESCE(processed_ts_ms, ts_ms, CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT))
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = '%s',
                  'properties.bootstrap.servers' = '%s',
                  'properties.group.id' = '%s',
                  'scan.startup.mode' = 'earliest-offset',
                  'format' = 'json',
                  'json.ignore-parse-errors' = 'true',
                  'json.fail-on-missing-field' = 'false'
                )
                """.formatted(inTopic, bootstrap, groupId);
    }

    private static String hudiSinkSql(String hudiPath, String hudiTable, String hiveDb, String hiveTable, String metastoreUris) {
        return """
                CREATE TABLE hudi_students (
                  student_id BIGINT,
                  week INT,
                  study_hours DOUBLE,
                  sleep_hours DOUBLE,
                  stress_level DOUBLE,
                  attendance_rate DOUBLE,
                  screen_time_hours DOUBLE,
                  caffeine_intake INT,
                  learning_efficiency DOUBLE,
                  fatigue_index DOUBLE,
                  quiz_score DOUBLE,
                  assignment_score DOUBLE,
                  performance_index DOUBLE,
                  _source STRING,
                  _ingested_at STRING,
                  _pipeline STRING,
                  _processed_at STRING,
                  ts_ms BIGINT,
                  PRIMARY KEY (student_id, week) NOT ENFORCED
                ) PARTITIONED BY (week)
                WITH (
                  'connector' = 'hudi',
                  'path' = '%s',
                  'hoodie.table.name' = '%s',
                  'table.type' = 'COPY_ON_WRITE',
                  'write.operation' = 'upsert',
                  'hoodie.datasource.write.recordkey.field' = 'student_id,week',
                  'hoodie.datasource.write.partitionpath.field' = 'week',
                  'hoodie.datasource.write.precombine.field' = 'ts_ms',

                  'hoodie.filesystem.view.type' = 'MEMORY',
                  'hoodie.metadata.enable' = 'false',

                  'hoodie.datasource.hive_sync.enable' = 'true',
                  'hoodie.datasource.hive_sync.mode' = 'hms',
                  'hoodie.datasource.hive_sync.use_jdbc' = 'false',
                  'hoodie.datasource.hive_sync.metastore.uris' = '%s',
                  'hoodie.datasource.hive_sync.auto_create_database' = 'true',
                  'hoodie.datasource.hive_sync.database' = '%s',
                  'hoodie.datasource.hive_sync.table' = '%s',
                  'hoodie.datasource.hive_sync.partition_fields' = 'week',
                  'hoodie.datasource.write.hive_style_partitioning' = 'true',
                  'hoodie.datasource.hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
                )
                """.formatted(hudiPath, hudiTable, metastoreUris, hiveDb, hiveTable);
    }

    private static String insertSql() {
        return """
                INSERT INTO hudi_students
                SELECT
                  student_id, week, study_hours, sleep_hours, stress_level,
                  attendance_rate, screen_time_hours, caffeine_intake,
                  learning_efficiency, fatigue_index, quiz_score, assignment_score,
                  performance_index, _source, _ingested_at, _pipeline, _processed_at,
                  ts_ms_fixed AS ts_ms
                FROM kafka_students
                WHERE student_id IS NOT NULL AND week IS NOT NULL
                """;
    }
}
