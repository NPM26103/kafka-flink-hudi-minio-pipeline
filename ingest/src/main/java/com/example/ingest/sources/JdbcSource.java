package com.example.ingest.sources;

import com.example.common.Args;
import com.example.common.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public class JdbcSource implements IngestSource {

    private final Args cfg;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    @Override
    public String name() { return "jdbc"; }

    @Override
    public Mode mode() { return Mode.ONCE; }

    @Override
    public void run() throws Exception {
        runOnce();
    }

    public void runOnce() throws Exception {
        String jdbcUrl   = cfg.must("jdbcUrl");
        String jdbcUser  = cfg.get("jdbcUser", "");
        String jdbcPass  = cfg.get("jdbcPass", "");
        String query     = cfg.must("jdbcQuery");
        String keyCol    = cfg.get("keyCol", "id");

        long count = 0;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
             PreparedStatement ps = conn.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {

            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();

            while (rs.next()) {
                ObjectNode obj = Json.obj();
                for (int i = 1; i <= n; i++) {
                    String col = md.getColumnLabel(i);
                    Object val = rs.getObject(i);
                    if (val == null) obj.putNull(col);
                    else obj.put(col, String.valueOf(val));
                }

                obj.put("_source", "jdbc");
                obj.put("_ingested_at", Instant.now().toString());

                // ✅ compute key per record
                String key = null;
                try {
                    Object o = rs.getObject(keyCol);
                    if (o != null) key = String.valueOf(o);
                } catch (Exception ignore) {}

                final String recordKey = key;
                final String payload = Json.toString(obj);

                producer.send(new ProducerRecord<>(topic, recordKey, payload),
                        (md2, ex) -> {
                            if (ex != null) {
                                log.error("[JDBC] send failed topic={} key={} err={}", topic, recordKey, ex.toString());
                            }
                        });

                count++;
            }

            producer.flush();
        }

        log.info("[JDBC] published {} records -> {}", count, topic);
    }
}
