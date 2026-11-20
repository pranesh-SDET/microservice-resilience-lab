package com.resilience.lab.tests;

import com.resilience.lab.handlers.DeadLetterHandler;
import com.resilience.lab.handlers.ManualCommitConsumerHandler;
import com.resilience.lab.handlers.ResilientProducerHandler;
import com.resilience.lab.helper.EventPayloadHelper;
import com.resilience.lab.helper.TopicAdminHelper;
import com.resilience.lab.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaObservabilityTest {

    private ResilientProducerHandler producer;

    @BeforeClass
    public void setUp() {
        producer = new ResilientProducerHandler();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test(priority = 1)
    public void testPartitionKeyStickinessAndOffsetOrdering() throws Exception {
        String topic = "orders-ordering-" + UUID.randomUUID().toString().substring(0, 8);
        TopicAdminHelper.ensureTopicExists(topic, 3, (short) 1);

        List<RecordMetadata> results = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            OrderEvent event = new OrderEvent("evt-ord-" + i, "cust-42", 1000L + i, 49.99 * i, "PLACED");
            results.add(producer.sendSync(topic, event.customerId(), EventPayloadHelper.toJson(event)));
        }

        int expectedPartition = results.get(0).partition();
        long previousOffset = -1L;
        for (RecordMetadata meta : results) {
            Assert.assertEquals(meta.partition(), expectedPartition, "All cust-42 events must land on the same partition");
            Assert.assertTrue(meta.offset() > previousOffset, "Offsets within a partition must strictly increase");
            previousOffset = meta.offset();
        }
    }

    @Test(priority = 2)
    public void testConsumerLagDrainsToZeroAndDeduplicatesEvents() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topic = "orders-dedup-" + suffix;
        String groupId = "group-dedup-" + suffix;
        TopicAdminHelper.ensureTopicExists(topic, 3, (short) 1);

        OrderEvent first = new OrderEvent("evt-dup-1", "cust-10", 2001L, 120.00, "PLACED");
        OrderEvent duplicate = new OrderEvent("evt-dup-1", "cust-10", 2001L, 120.00, "PLACED");
        OrderEvent second = new OrderEvent("evt-unique-2", "cust-20", 2002L, 85.50, "PLACED");

        producer.sendSync(topic, first.customerId(), EventPayloadHelper.toJson(first));
        producer.sendSync(topic, duplicate.customerId(), EventPayloadHelper.toJson(duplicate));
        producer.sendSync(topic, second.customerId(), EventPayloadHelper.toJson(second));

        Set<String> seenEventIds = ConcurrentHashMap.newKeySet();
        List<OrderEvent> processedOrders = new CopyOnWriteArrayList<>();

        try (ManualCommitConsumerHandler consumer = new ManualCommitConsumerHandler(groupId, new DeadLetterHandler(producer))) {
            consumer.startAsync(topic, record -> {
                OrderEvent event = EventPayloadHelper.fromJson(record.value(), OrderEvent.class);
                if (seenEventIds.add(event.eventId())) {
                    processedOrders.add(event);
                }
            });

            Assert.assertTrue(consumer.awaitPartitionAssignment(Duration.ofSeconds(15)), "Consumer failed to join group");

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .untilAsserted(() -> {
                        Assert.assertEquals(processedOrders.size(), 2, "Duplicate eventId must be ignored");
                        long lag = TopicAdminHelper.getConsumerGroupLag(groupId, topic);
                        Assert.assertEquals(lag, 0L, "Consumer group lag must drain to 0 after manual commit");
                    });
        }
    }

    @Test(priority = 3)
    public void testPoisonPillIsolatedToDeadLetterTopic() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String topic = "orders-dlt-" + suffix;
        String dltTopic = topic + ".DLT";
        String mainGroup = "group-main-" + suffix;
        String dltGroup = "group-dlt-" + suffix;

        TopicAdminHelper.ensureTopicExists(topic, 1, (short) 1);
        TopicAdminHelper.ensureTopicExists(dltTopic, 1, (short) 1);

        OrderEvent validBefore = new OrderEvent("evt-ok-1", "cust-7", 3001L, 50.00, "PLACED");
        OrderEvent validAfter = new OrderEvent("evt-ok-2", "cust-7", 3002L, 75.00, "PLACED");

        producer.sendSync(topic, "cust-7", EventPayloadHelper.toJson(validBefore));
        producer.sendSync(topic, "cust-7", "{corrupted-json-poison-pill");
        producer.sendSync(topic, "cust-7", EventPayloadHelper.toJson(validAfter));

        List<OrderEvent> validOrders = new CopyOnWriteArrayList<>();
        List<ConsumerRecord<String, String>> dltRecords = new CopyOnWriteArrayList<>();

        try (ManualCommitConsumerHandler mainConsumer = new ManualCommitConsumerHandler(mainGroup, new DeadLetterHandler(producer));
             ManualCommitConsumerHandler dltConsumer = new ManualCommitConsumerHandler(dltGroup, null)) {

            mainConsumer.startAsync(topic, record -> {
                OrderEvent event = EventPayloadHelper.fromJson(record.value(), OrderEvent.class);
                validOrders.add(event);
            });
            dltConsumer.startAsync(dltTopic, dltRecords::add);

            Assert.assertTrue(mainConsumer.awaitPartitionAssignment(Duration.ofSeconds(15)));
            Assert.assertTrue(dltConsumer.awaitPartitionAssignment(Duration.ofSeconds(15)));

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .untilAsserted(() -> {
                        Assert.assertEquals(validOrders.size(), 2, "Valid orders before and after poison pill must process");
                        Assert.assertEquals(dltRecords.size(), 1, "Poison pill must land in DLT");
                        var errorHeader = dltRecords.get(0).headers().lastHeader("dlt-error-class");
                        Assert.assertNotNull(errorHeader, "DLT record must carry dlt-error-class header");
                        String errorClass = new String(errorHeader.value(), StandardCharsets.UTF_8);
                        Assert.assertTrue(errorClass.contains("Json"), "Expected Jackson parse error header, got: " + errorClass);
                    });
        }
    }
}