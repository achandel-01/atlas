/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.Meter;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.utils.KafkaUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;
import static org.apache.atlas.security.SecurityProperties.TRUSTSTORE_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityUtil.getPassword;

final class KafkaMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetrics.class);

    private static final int DEFAULT_OFFSET_COLLECTION_INTERVAL_SECS = 30;

    private static final String ATLAS_KAFKA_METRICS_THREAD_NAME = "atlas-kafka-metrics-thread";

    private static final AttributeKey<String> TOPIC     = AttributeKey.stringKey("topic");
    private static final AttributeKey<Long>   PARTITION = AttributeKey.longKey("partition");
    private static final Meter                METER     = GlobalOpenTelemetry.getMeter("org.apache.atlas.notification.kafka");
    private static final LongGauge            COMMITTED_OFFSET = METER.gaugeBuilder("atlas.kafka.committed.offset")
            .ofLongs()
            .setDescription("Latest Kafka offset committed by Atlas notification consumers")
            .setUnit("{offset}")
            .build();
    private static final LongGauge            LATEST_OFFSET = METER.gaugeBuilder("atlas.kafka.latest.offset")
            .ofLongs()
            .setDescription("Latest Kafka broker log-end offset for Atlas notification topics")
            .setUnit("{offset}")
            .build();

    private static volatile AdminClient adminClient;
    private static volatile ScheduledExecutorService executorService;
    private static volatile Set<TopicPartition> configuredTopicPartitions;

    private KafkaMetrics() {
    }

    static void start() {
        if (!isKafkaLagMetricsEnabled()) {
            return;
        }

        if (executorService == null) {
            synchronized (KafkaMetrics.class) {
                if (executorService == null) {
                    executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
                        Thread thread = new Thread(runnable, ATLAS_KAFKA_METRICS_THREAD_NAME);

                        thread.setDaemon(true);

                        return thread;
                    });
                    executorService.scheduleWithFixedDelay(KafkaMetrics::recordLatestOffsets, 0,
                            DEFAULT_OFFSET_COLLECTION_INTERVAL_SECS, TimeUnit.SECONDS);
                    LOG.info("Started Kafka metrics collector with interval={} seconds",
                            DEFAULT_OFFSET_COLLECTION_INTERVAL_SECS);
                }
            }
        }
    }

    static void stop() {
        synchronized (KafkaMetrics.class) {
            ScheduledExecutorService executor = executorService;

            if (executor != null) {
                executor.shutdownNow();
                executorService = null;
            }

            AdminClient client = adminClient;

            if (client != null) {
                client.close();
                adminClient = null;
            }

            configuredTopicPartitions = null;
            LOG.info("Stopped Kafka metrics collector");
        }
    }

    static void updateProcessedOffset(TopicPartition topicPartition, long committedOffset) {
        if (!isKafkaLagMetricsEnabled()) {
            return;
        }

        if (topicPartition == null || committedOffset < 0) {
            return;
        }

        if (!isConfiguredTopic(topicPartition.topic())) {
            return;
        }

        COMMITTED_OFFSET.set(committedOffset, Attributes.of(TOPIC, topicPartition.topic(), PARTITION,
                (long) topicPartition.partition()));
    }

    private static void recordLatestOffsets() {
        try {
            Set<TopicPartition> topicPartitions = getConfiguredTopicPartitions();

            if (topicPartitions.isEmpty()) {
                return;
            }

            Map<TopicPartition, Long> latestOffsets = getLatestOffsets(topicPartitions);

            for (Map.Entry<TopicPartition, Long> entry : latestOffsets.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Long           latestOffset   = entry.getValue();

                LATEST_OFFSET.set(latestOffset, Attributes.of(TOPIC, topicPartition.topic(), PARTITION,
                        (long) topicPartition.partition()));
            }
        } catch (Exception exception) {
            LOG.warn("Failed to record Kafka latest offset metrics: {}", exception.getMessage());
        }
    }

    private static Set<TopicPartition> getConfiguredTopicPartitions() throws Exception {
        Set<TopicPartition> ret = configuredTopicPartitions;

        if (ret == null) {
            synchronized (KafkaMetrics.class) {
                ret = configuredTopicPartitions;

                if (ret == null) {
                    ret = loadConfiguredTopicPartitions();
                    configuredTopicPartitions = ret;
                }
            }
        }

        return ret;
    }

    private static boolean isConfiguredTopic(String topic) {
        if (topic == null) {
            return false;
        }

        return getConfiguredTopicNames().contains(topic.trim());
    }

    private static Set<String> getConfiguredTopicNames() {
        Set<String> topicNames = new HashSet<>();

        for (String topicName : AtlasConfiguration.NOTIFICATION_HOOK_CONSUMER_TOPIC_NAMES
                .getStringArray(KafkaNotification.ATLAS_HOOK_TOPIC)) {
            if (StringUtils.isNotBlank(topicName)) {
                topicNames.add(topicName.trim());
            }
        }

        if (AtlasConfiguration.ATLAS_PARALLEL_PROCESSING_ENABLED.getBoolean()) {
            for (String topicName : AtlasConfiguration.ATLAS_PARALLEL_PROCESSING_INPUT_TOPICS
                    .getStringArray(KafkaNotification.ATLAS_HOOK_TOPIC)) {
                if (StringUtils.isNotBlank(topicName)) {
                    topicNames.add(topicName.trim());
                }
            }
        }

        return topicNames;
    }

    private static Set<TopicPartition> loadConfiguredTopicPartitions() throws Exception {
        Set<String> topicNames = getConfiguredTopicNames();

        LOG.info("Kafka lag metrics configured topic names: {}", topicNames);

        if (topicNames.isEmpty()) {
            return new HashSet<>();
        }

        DescribeTopicsResult describeTopicsResult = getAdminClient().describeTopics(topicNames);
        Map<String, TopicDescription> descriptions = describeTopicsResult.allTopicNames().get();
        Set<TopicPartition> ret = new HashSet<>();

        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            for (TopicPartitionInfo partition : entry.getValue().partitions()) {
                ret.add(new TopicPartition(entry.getKey(), partition.partition()));
            }
        }

        return ret;
    }

    private static Map<TopicPartition, Long> getLatestOffsets(Iterable<TopicPartition> topicPartitions) throws Exception {
        Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();

        for (TopicPartition topicPartition : topicPartitions) {
            offsetSpecs.put(topicPartition, OffsetSpec.latest());
        }

        ListOffsetsResult result = getAdminClient().listOffsets(offsetSpecs);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = result.all().get();
        Map<TopicPartition, Long> ret = new HashMap<>();

        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsets.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().offset());
        }

        return ret;
    }

    private static boolean isKafkaLagMetricsEnabled() {
        return AtlasConfiguration.NOTIFICATION_CONSUMER_KAFKA_LAG_METRICS_ENABLED.getBoolean();
    }

    private static AdminClient getAdminClient() throws Exception {
        AdminClient ret = adminClient;

        if (ret == null) {
            synchronized (KafkaMetrics.class) {
                ret = adminClient;

                if (ret == null) {
                    Configuration atlasConfiguration = ApplicationProperties.get();
                    Configuration kafkaConfiguration = ApplicationProperties.getSubsetConfiguration(atlasConfiguration,
                            KafkaNotification.PROPERTY_PREFIX);
                    Properties    kafkaProperties    = ConfigurationConverter.getProperties(kafkaConfiguration);

                    if (atlasConfiguration.getBoolean(TLS_ENABLED, false)) {
                        kafkaProperties.put("ssl.truststore.password",
                                getPassword(atlasConfiguration, TRUSTSTORE_PASSWORD_KEY));
                    }

                    KafkaUtils.setKafkaJAASProperties(atlasConfiguration, kafkaProperties);

                    ret = AdminClient.create(kafkaProperties);
                    adminClient = ret;
                }
            }
        }

        return ret;
    }
}
