package com.resilience.lab.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface RecordProcessor {
    void process(ConsumerRecord<String, String> record) throws Exception;
}