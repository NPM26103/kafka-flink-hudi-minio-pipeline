package com.example.ingest;

import com.example.common.Args;
import com.example.ingest.KafkaProducerr;
import com.example.ingest.sources.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IngestMain {

    public static void main(String[] args) throws Exception {
        Args cfg = Args.parse(args);
        System.out.println("ARGS=" + cfg);

        String bootstrap = cfg.get("bootstrap", "localhost:9092");

        boolean enableCsv  = cfg.getBool("ENABLE_CSV", false);
        boolean enableHttp = cfg.getBool("ENABLE_HTTP", false);
        boolean enableJdbc = cfg.getBool("ENABLE_JDBC", false);

        if (!enableCsv && !enableHttp && !enableJdbc) {
            enableCsv = true;
        }

        String topic = cfg.get("topic", "");
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Missing --topic. Example: --topic=raw.student.csv");
        }

        try (KafkaProducer<String, String> producer =
                     KafkaProducerr.create(bootstrap, "ingest-" + modeName(enableCsv, enableHttp, enableJdbc))) {

            ExecutorService pool = Executors.newFixedThreadPool(3);

            if (enableCsv) {
                pool.submit(() -> {
                    try {
                        new CsvSource(cfg, producer, topic).runOnce();
                        System.out.println("[INGEST] CSV done -> " + topic);
                    } catch (Exception e) {
                        throw new RuntimeException("[INGEST] CSV failed: " + e.getMessage(), e);
                    }
                });
            }

            if (enableJdbc) {
                pool.submit(() -> {
                    try {
                        new JdbcSource(cfg, producer, topic).runOnce();
                        System.out.println("[INGEST] JDBC done -> " + topic);
                    } catch (Exception e) {
                        throw new RuntimeException("[INGEST] JDBC failed: " + e.getMessage(), e);
                    }
                });
            }

            if (enableHttp) {
                pool.submit(() -> {
                    try {
                        new HttpSource(cfg, producer, topic).runLoop();
                        System.out.println("[INGEST] HTTP loop -> " + topic);
                    } catch (Exception e) {
                        throw new RuntimeException("[INGEST] HTTP failed: " + e.getMessage(), e);
                    }
                });
            }

            Thread.currentThread().join();
        }
    }

    private static String modeName(boolean csv, boolean http, boolean jdbc) {
        StringBuilder sb = new StringBuilder();
        if (csv) sb.append("csv");
        if (http) sb.append(sb.length() == 0 ? "http" : "+http");
        if (jdbc) sb.append(sb.length() == 0 ? "jdbc" : "+jdbc");
        return sb.length() == 0 ? "none" : sb.toString();
    }
}