package com.example.ingest;

import com.example.common.Args;
import com.example.ingest.sources.IngestSource;
import com.example.ingest.sources.SourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;

@Slf4j
public class IngestMain {

    public static void main(String[] args) throws Exception {
        Args cfg = Args.parse(args);
        log.info("ARGS={}", cfg);

        String bootstrap = cfg.get("bootstrap", "localhost:9092");

        String topic = cfg.get("topic", "");
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Missing --topic. Example: --topic=raw.student.csv");
        }

        String clientId = "ingest-" + SourceFactory.modeName(cfg);

        try (KafkaProducer<String, String> producer = KafkaProducerr.create(bootstrap, clientId)) {
            List<IngestSource> sources = SourceFactory.build(cfg, producer, topic);

            for (IngestSource s : sources) {
                if (s.mode() == IngestSource.Mode.ONCE) {
                    log.info("[INGEST] start source={} topic={}", s.name(), topic);
                    s.run();
                    log.info("[INGEST] done  source={} topic={}", s.name(), topic);
                }
            }

            for (IngestSource s : sources) {
                if (s.mode() == IngestSource.Mode.LOOP) {
                    log.info("[INGEST] loop source={} topic={} starting ...", s.name(), topic);
                    s.run();
                }
            }
        }
    }
}
