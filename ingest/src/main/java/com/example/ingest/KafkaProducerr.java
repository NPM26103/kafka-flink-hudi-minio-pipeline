package com.example.ingest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public final class KafkaProducerr {
    private KafkaProducerr(){}

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
        props.put("linger.ms", "20");

        return new KafkaProducer<>(props);
    }
}
