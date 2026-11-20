package com.resilience.lab.helper;

import com.resilience.lab.configs.KafkaConfigFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public final class TopicAdminHelper {

    private static final Logger log = LoggerFactory.getLogger(TopicAdminHelper.class);

    private TopicAdminHelper() {}

    public static void ensureTopicExists(String topicName, int partitions, short replicationFactor) throws Exception {
        try (Admin admin = Admin.create(KafkaConfigFactory.adminProps())) {
            Set<String> existing = admin.listTopics().names().get();
            if (existing.contains(topicName)) {
                return;
            }
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            try {
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
                log.info("Created topic '{}' with {} partitions", topicName, partitions);
            } catch (ExecutionException ex) {
                if (!(ex.getCause() instanceof TopicExistsException)) {
                    throw ex;
                }
            }
        }
    }

    public static long getConsumerGroupLag(String groupId, String topicName) throws Exception {
        try (Admin admin = Admin.create(KafkaConfigFactory.adminProps())) {
            TopicDescription description = admin.describeTopics(Collections.singletonList(topicName))
                    .allTopicNames().get().get(topicName);

            Set<TopicPartition> partitions = new HashSet<>();
            Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
            for (TopicPartitionInfo info : description.partitions()) {
                TopicPartition tp = new TopicPartition(topicName, info.partition());
                partitions.add(tp);
                latestSpecs.put(tp, OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(latestSpecs).all().get();
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

            long totalLag = 0L;
            for (TopicPartition tp : partitions) {
                long endOffset = endOffsets.get(tp).offset();
                OffsetAndMetadata committed = committedOffsets.get(tp);
                long currentOffset = (committed != null) ? committed.offset() : 0L;
                totalLag += Math.max(0L, endOffset - currentOffset);
            }
            return totalLag;
        }
    }
}