package com.resilience.lab.handlers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class DeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterHandler.class);
    private final ResilientProducerHandler producer;

    public DeadLetterHandler(ResilientProducerHandler producer) {
        this.producer = producer;
    }

    public void sendToDlt(ConsumerRecord<String, String> failedRecord, Exception cause) {
        String dltTopic = failedRecord.topic() + ".DLT";
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("dlt-original-topic", failedRecord.topic());
        headers.put("dlt-original-partition", String.valueOf(failedRecord.partition()));
        headers.put("dlt-original-offset", String.valueOf(failedRecord.offset()));
        headers.put("dlt-error-class", cause.getClass().getSimpleName());
        headers.put("dlt-error-message", cause.getMessage() != null ? cause.getMessage() : "unknown");

        try {
            producer.sendSync(dltTopic, failedRecord.key(), failedRecord.value(), headers);
            log.warn("Routed poison pill from {}:{}@{} to {} ({})",
                    failedRecord.topic(), failedRecord.partition(), failedRecord.offset(),
                    dltTopic, cause.getClass().getSimpleName());
        } catch (Exception dltEx) {
            log.error("Failed to publish record to DLT {}", dltTopic, dltEx);
        }
    }
}