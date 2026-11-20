package com.resilience.lab.handlers;

import com.resilience.lab.configs.KafkaConfigFactory;
import com.resilience.lab.core.ManagedKafkaClient;
import com.resilience.lab.core.RecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ManualCommitConsumerHandler extends ManagedKafkaClient {

    private static final Logger log = LoggerFactory.getLogger(ManualCommitConsumerHandler.class);

    private final KafkaConsumer<String, String> consumer;
    private final DeadLetterHandler deadLetterHandler;
    private final CountDownLatch assignmentLatch = new CountDownLatch(1);
    private Thread workerThread;

    public ManualCommitConsumerHandler(String groupId, DeadLetterHandler deadLetterHandler) {
        this.consumer = new KafkaConsumer<>(KafkaConfigFactory.consumerProps(groupId));
        this.deadLetterHandler = deadLetterHandler;
    }

    public void startAsync(String topicName, RecordProcessor processor) {
        workerThread = new Thread(() -> runLoop(topicName, processor), "kafka-consumer-" + topicName);
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public boolean awaitPartitionAssignment(Duration timeout) throws InterruptedException {
        return assignmentLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void runLoop(String topicName, RecordProcessor processor) {
        try {
            consumer.subscribe(Collections.singletonList(topicName), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!partitions.isEmpty()) {
                        log.info("Assigned partitions {} for topic '{}'", partitions, topicName);
                        assignmentLatch.countDown();
                    }
                }
            });

            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processor.process(record);
                    } catch (Exception ex) {
                        if (deadLetterHandler != null) {
                            deadLetterHandler.sendToDlt(record, ex);
                        } else {
                            log.error("Record processing failed with no DLT configured", ex);
                        }
                    }
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1)));
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            log.info("ManualCommitConsumerHandler closed cleanly for '{}'", topicName);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            consumer.wakeup();
            if (workerThread != null && workerThread != Thread.currentThread()) {
                try {
                    workerThread.join(5000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            removeShutdownHookQuietly();
        }
    }
}