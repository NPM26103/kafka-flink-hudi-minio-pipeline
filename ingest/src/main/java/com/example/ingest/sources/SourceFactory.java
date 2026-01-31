package com.example.ingest.sources;

import com.example.common.Args;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.ArrayList;
import java.util.List;

public final class SourceFactory {
    private SourceFactory(){}
    public static String modeName(Args cfg) {
        boolean enableCsv  = cfg.getBool("ENABLE_CSV", false);
        boolean enableHttp = cfg.getBool("ENABLE_HTTP", false);
        boolean enableJdbc = cfg.getBool("ENABLE_JDBC", false);

        if (!enableCsv && !enableHttp && !enableJdbc) enableCsv = true;

        StringBuilder sb = new StringBuilder();
        if (enableCsv) sb.append("csv");
        if (enableHttp) sb.append(sb.length() == 0 ? "http" : "+http");
        if (enableJdbc) sb.append(sb.length() == 0 ? "jdbc" : "+jdbc");
        return sb.length() == 0 ? "none" : sb.toString();
    }

    public static List<IngestSource> build(Args cfg, KafkaProducer<String, String> producer, String topic) {
        boolean enableCsv  = cfg.getBool("ENABLE_CSV", false);
        boolean enableHttp = cfg.getBool("ENABLE_HTTP", false);
        boolean enableJdbc = cfg.getBool("ENABLE_JDBC", false);

        if (!enableCsv && !enableHttp && !enableJdbc) enableCsv = true;

        List<IngestSource> list = new ArrayList<>();

        // để ngược sẽ chạy mãi http
        if (enableCsv)  list.add(new CsvSource(cfg, producer, topic));
        if (enableJdbc) list.add(new JdbcSource(cfg, producer, topic));

        if (enableHttp) list.add(new HttpSource(cfg, producer, topic));

        return list;
    }
}
