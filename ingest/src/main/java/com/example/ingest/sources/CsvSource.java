package com.example.ingest.sources;

import com.example.common.Args;
import com.example.common.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileReader;
import java.time.Instant;
import java.util.Map;

public class CsvSource {
    private final Args cfg;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public CsvSource(Args cfg, KafkaProducer<String, String> producer, String topic) {
        this.cfg = cfg;
        this.producer = producer;
        this.topic = topic;
    }

    public void runOnce() throws Exception {
        String path = cfg.get("csvPath", "/Users/npm/Documents/demo3/src/resources/student.csv");
        String keyCol = cfg.get("keyCol", "id");

        try (FileReader reader = new FileReader(path);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord r : parser) {
                ObjectNode obj = Json.MAPPER.createObjectNode();
                for (Map.Entry<String, String> e : r.toMap().entrySet()) {
                    obj.put(e.getKey(), e.getValue());
                }
                obj.put("_source", "csv");
                obj.put("_ingested_at", Instant.now().toString());

                String key = null;
                if (r.isMapped(keyCol)) key = r.get(keyCol);

                producer.send(new ProducerRecord<>(topic, key, Json.toString(obj)));
            }
            producer.flush();
        }
    }
}
