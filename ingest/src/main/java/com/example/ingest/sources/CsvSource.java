package com.example.ingest.sources;

import com.example.common.Args;
import com.example.common.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileReader;
import java.time.Instant;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class CsvSource implements IngestSource {

    private final Args cfg;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    @Override
    public String name() { return "csv"; }

    @Override
    public Mode mode() { return Mode.ONCE; }

    @Override
    public void run() throws Exception {
        runOnce();
    }

    public void runOnce() throws Exception {
        String path = cfg.get("csvPath", "/Users/npm/Documents/demo3/src/resources/student.csv");
        String keyCol = cfg.get("keyCol", "id");

        long count = 0;
        try (FileReader reader = new FileReader(path);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord r : parser) {
                ObjectNode obj = Json.obj();
                for (Map.Entry<String, String> e : r.toMap().entrySet()) {
                    obj.put(e.getKey(), e.getValue());
                }
                obj.put("_source", "csv");
                obj.put("_ingested_at", Instant.now().toString());

                String key = (r.isMapped(keyCol) ? r.get(keyCol) : null);

                producer.send(new ProducerRecord<>(topic, key, Json.toString(obj)),
                        (md, ex) -> {
                            if (ex != null) {
                                log.error("[CSV] send failed topic={} key={} err={}", topic, key, ex.toString());
                            }
                        });
                count++;
            }
            producer.flush();
        }

        log.info("[CSV] published {} records -> {}", count, topic);
    }
}
