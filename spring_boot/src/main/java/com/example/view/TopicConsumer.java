package com.example.view;

import com.example.view.config.SaveToDb;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopicConsumer {

    private final SaveToDb saveToDb;

    @KafkaListener(topics = "${app.topic}")
    public void consume(String message) {
        try {
            saveToDb.save(message);
        } catch (Exception e) {
            // không throw để tránh listener crash/retry vô hạn
            log.error("[VIEW] save failed: {}", e.getMessage(), e);
        }
    }
}
