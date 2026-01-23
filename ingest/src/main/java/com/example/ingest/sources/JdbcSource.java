package com.example.ingest.sources;

import com.example.common.Args;
import com.example.common.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.time.Instant;

public class JdbcSource {
    private final Args cfg;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public JdbcSource(Args cfg, KafkaProducer<String, String> producer, String topic) {
        this.cfg = cfg;
        this.producer = producer;
        this.topic = topic;
    }

    public void runOnce() throws Exception{
        String jdbcUrl   = cfg.must("jdbcUrl");
        String jdbcUser  = cfg.get("jdbcUser", "");
        String jdbcPass  = cfg.get("jdbcPass", "");
        String query     = cfg.must("jdbcQuery");
        String keyCol    = cfg.get("keyCol", "id");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
            PreparedStatement ps = conn.prepareStatement(query);
            ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();

            while (rs.next()){
                ObjectNode obj = Json.MAPPER.createObjectNode();
                for (int i = 1; i <= n; i++){
                    String col = md.getColumnLabel(i);
                    Object val = rs.getObject(i);
                    if (val == null) obj.putNull(col);
                    else obj.put(col, String.valueOf(val));
                }
                obj.put("_source", "jdbc");
                obj.put("_ingested_at", Instant.now().toString());

                String key = null;
                try {
                    Object o = rs.getObject(keyCol);
                    if (o != null) key = String.valueOf(o);
                } catch (Exception ignore) {}

                producer.send(new ProducerRecord<>(topic, key, Json.toString(obj)));
            }
            producer.flush();
        }
    }
}
