package com.example.ingest.sources;

import com.example.common.Args;
import com.example.common.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public class HttpSource implements IngestSource {

    private final Args cfg;
    private final KafkaProducer<String, String> producer;
    private final String topic;

    @Override
    public String name() { return "http"; }

    @Override
    public Mode mode() { return Mode.LOOP; }

    @Override
    public void run() throws Exception {
        runLoop();
    }

    public void runLoop() throws Exception {
        String url = cfg.must("url");
        long intervalMs = cfg.getLong("intervalMs", 5000);
        String authHeader = cfg.get("authHeader", "");

        HttpClient client = HttpClient.newHttpClient();

        while (true) {
            try {
                HttpRequest.Builder b = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .GET()
                        .header("Accept", "application/json");

                if (!authHeader.isBlank()) {
                    int idx = authHeader.indexOf(':');
                    if (idx > 0) {
                        String hk = authHeader.substring(0, idx).trim();
                        String hv = authHeader.substring(idx + 1).trim();
                        b.header(hk, hv);
                    }
                }

                HttpResponse<String> resp = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
                String body = resp.body() == null ? "" : resp.body();

                publishHttpBody(body, url, resp.statusCode());
                producer.flush();
            } catch (Exception e) {
                String err = "http_poll_error: " + e.getClass().getSimpleName() + ": " + e.getMessage();
                log.warn("[HTTP] poll failed: {}", err);

                producer.send(new ProducerRecord<>(topic, null, Json.wrapRaw("http_poll", err)));
                producer.flush();
            }

            Thread.sleep(intervalMs);
        }
    }

    private void publishHttpBody(String body, String url, int status) {
        try {
            JsonNode node = Json.MAPPER.readTree(body);

            // Nếu là array gửi từng item
            if (node != null && node.isArray()) {
                for (JsonNode item : node) {
                    if (item != null && item.isObject()) {
                        ObjectNode obj = (ObjectNode) item;
                        obj.put("_source", "http_poll");
                        obj.put("_url", url);
                        obj.put("_status", status);
                        obj.put("_ingested_at", Instant.now().toString());
                        producer.send(new ProducerRecord<>(topic, null, Json.toString(obj)));
                    } else {
                        ObjectNode wrap = Json.obj();
                        wrap.set("raw_item", item);
                        wrap.put("_source", "http_poll");
                        wrap.put("_url", url);
                        wrap.put("_status", status);
                        wrap.put("_ingested_at", Instant.now().toString());
                        producer.send(new ProducerRecord<>(topic, null, Json.toString(wrap)));
                    }
                }
                return;
            }

            // Nếu là object gửi trực tiếp
            if (node != null && node.isObject()) {
                ObjectNode obj = (ObjectNode) node;
                obj.put("_source", "http_poll");
                obj.put("_url", url);
                obj.put("_status", status);
                obj.put("_ingested_at", Instant.now().toString());
                producer.send(new ProducerRecord<>(topic, null, Json.toString(obj)));
                return;
            }
        } catch (Exception ignore) {}

        // Fallback
        ObjectNode wrap = Json.obj();
        wrap.put("raw", body);
        wrap.put("_source", "http_poll");
        wrap.put("_url", url);
        wrap.put("_status", status);
        wrap.put("_ingested_at", Instant.now().toString());
        producer.send(new ProducerRecord<>(topic, null, Json.toString(wrap)));
    }
}
