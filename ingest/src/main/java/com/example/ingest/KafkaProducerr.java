package com.example.ingest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public final class KafkaProducerr {

    private KafkaProducerr() {}

    public static KafkaProducer<String, String> create(String bootstrap, String clientId) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrap);
        props.put("client.id", clientId);

        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // reliability
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("retries", "10");

        // throughput (safe)
        props.put("linger.ms", "20");
        props.put("batch.size", String.valueOf(32 * 1024)); // 32KB
        props.put("compression.type", "lz4");

        // idempotence
        props.put("max.in.flight.requests.per.connection", "5");

        // timeouts
        props.put("request.timeout.ms", "30000");
        props.put("delivery.timeout.ms", "120000");

        return new KafkaProducer<>(props);
    }
}
