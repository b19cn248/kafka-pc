package com.chip1stop.kafkapc;

public interface KafkaProducerV1<K, V> {

    void send(K topicName, String key, V message);
}
