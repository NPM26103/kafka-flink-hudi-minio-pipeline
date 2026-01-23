package com.example.view;

import com.example.view.config.SaveToDb;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TopicConsumer {

    private final SaveToDb saveToDb;

    public TopicConsumer(SaveToDb saveToDb){
        this.saveToDb = saveToDb;
    }

    @KafkaListener(topics = "${app.topic}")
    public void consume(String message){
        saveToDb.save(message);
    }
}
