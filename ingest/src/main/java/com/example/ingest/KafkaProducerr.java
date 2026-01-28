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

        props.put("acks", "all");  // chờ xác nhận trước khi báo done -> 0: gửi mess xong là done
        props.put("enable.idempotence", "true");  //no dupli
        props.put("retries", "10"); //error -> retry
        props.put("linger.ms", "20");  // chờ tối đa 20 (ms) -> gom nhiều mess

        return new KafkaProducer<>(props);
    }
}
