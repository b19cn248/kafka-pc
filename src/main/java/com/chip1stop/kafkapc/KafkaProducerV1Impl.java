package com.chip1stop.kafkapc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class KafkaProducerV1Impl implements KafkaProducerV1<String, byte[]> {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    public KafkaProducerV1Impl(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, String key, byte[] message) {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, byte[]>> future = kafkaTemplate.send(topicName, key, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Message sent successfully to topic {} with key {} and partition {}", topicName, key, result.getRecordMetadata().partition());
            } else {
                log.error("Failed to send message to topic {} due to {}", topicName, ex.getMessage());
            }
        });
    }
}

