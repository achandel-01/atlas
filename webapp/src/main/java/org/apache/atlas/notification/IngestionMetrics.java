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
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class IngestionMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(IngestionMetrics.class);

    static final String OTHER_METADATA_SOURCE = "OTHER";

    /**
     * Parallel {@link org.apache.atlas.notification.preprocessor.NotificationPreProcessor} stamps routed
     * messages with {@code {topic}-{offset}-{hookSource}} (or {@code {topic}-{offset}} when hook source
     * is absent) for end-to-end tracing. That composite value must not be used as the {@code metadata_source}
     * metric label because it would explode cardinality. When parallel processing is disabled, the Kafka
     * message source is already the hook source (e.g. {@code hive}) and is returned unchanged.
     */
    private static final Pattern ROUTED_METADATA_SOURCE_PATTERN = Pattern.compile("^(.+)-(\\d+)(?:-(.+))?$");

    private static final String FAILURE_TYPE_GRAPH_LOCKING        = "GRAPH_LOCKING";
    private static final String FAILURE_TYPE_GRAPH_BACKEND        = "GRAPH_BACKEND";
    private static final String FAILURE_TYPE_JANUSGRAPH           = "JANUSGRAPH";
    private static final String FAILURE_TYPE_SCHEMA_VIOLATION     = "SCHEMA_VIOLATION";
    private static final String FAILURE_TYPE_INTERRUPTED          = "INTERRUPTED";
    private static final String FAILURE_TYPE_ATLAS_BASE_EXCEPTION = "ATLAS_BASE_EXCEPTION";
    private static final String FAILURE_TYPE_RUNTIME_EXCEPTION    = "RUNTIME_EXCEPTION";
    private static final String FAILURE_TYPE_OTHER                = "OTHER";

    private static final AttributeKey<String> MESSAGE_TYPE          = AttributeKey.stringKey("message_type");
    private static final AttributeKey<String> METADATA_SOURCE       = AttributeKey.stringKey("metadata_source");
    private static final AttributeKey<String> FAILURE_TYPE    = AttributeKey.stringKey("failure_type");
    private static final Meter                METER           = GlobalOpenTelemetry.getMeter("org.apache.atlas.notification");
    private static final LongCounter          REQUESTS        = METER.counterBuilder("atlas.ingestion.requests")
                                                                .setDescription("Number of ingestion notification messages processed by Atlas")
                                                                .setUnit("{request}")
                                                                .build();
    private static final LongCounter          FAILURES        = METER.counterBuilder("atlas.ingestion.failures")
                                                                .setDescription("Number of failed ingestion notification messages processed by Atlas")
                                                                .setUnit("{failure}")
                                                                .build();
    private static final LongCounter          RETRY_ATTEMPTS  = METER.counterBuilder("atlas.ingestion.retry.attempts")
                                                                .setDescription("Number of retry attempts scheduled for ingestion notification processing by Atlas")
                                                                .setUnit("{attempt}")
                                                                .build();
    private static final LongCounter          ENTITIES        = METER.counterBuilder("atlas.ingestion.entities")
                                                                .setDescription("Number of entities processed by Atlas ingestion notification processing")
                                                                .setUnit("{entity}")
                                                                .build();
    private static final DoubleHistogram      DURATION_MS     = METER.histogramBuilder("atlas.ingestion.duration")
                                                                .setDescription("Duration of successful ingestion notification processing by Atlas")
                                                                .setUnit("ms")
                                                                .build();

    private IngestionMetrics() {
    }

    /**
     * Returns the bounded {@code metadata_source} label for ingestion metrics.
     * Serial ingestion uses the hook source from the Kafka message. Parallel ingestion may carry a
     * composite routed source; in that case the trailing hook source segment is extracted.
     */
    static String normalizeMetadataSource(String metadataSource) {
        if (StringUtils.isBlank(metadataSource)) {
            return OTHER_METADATA_SOURCE;
        }

        String trimmed = metadataSource.trim();

        if (!AtlasConfiguration.ATLAS_PARALLEL_PROCESSING_ENABLED.getBoolean()) {
            return trimmed;
        }

        String normalized = extractHookSourceFromRoutedMetadata(trimmed);

        LOG.info("Ingestion metadata_source (parallel processing): original='{}', extracted='{}'", trimmed, normalized);

        return normalized;
    }

    static void recordRequest(String messageType, String metadataSource) {
        REQUESTS.add(1, Attributes.of(MESSAGE_TYPE, messageType, METADATA_SOURCE, metadataSource));
    }

    static void recordDuration(String messageType, String metadataSource, long elapsedMs) {
        DURATION_MS.record(elapsedMs, Attributes.of(MESSAGE_TYPE, messageType, METADATA_SOURCE, metadataSource));
    }

    static void recordFailure(String messageType, String metadataSource, Throwable exception) {
        String failureType = failureType(exception);

        FAILURES.add(1, Attributes.of(MESSAGE_TYPE, messageType, METADATA_SOURCE, metadataSource, FAILURE_TYPE, failureType));
    }

    static void recordRetryAttempt(String messageType, String metadataSource, Throwable exception) {
        String failureType = failureType(exception);

        RETRY_ATTEMPTS.add(1, Attributes.of(MESSAGE_TYPE, messageType, METADATA_SOURCE, metadataSource, FAILURE_TYPE, failureType));
    }

    static void recordEntities(String metadataSource, int entityCount) {
        if (entityCount > 0) {
            ENTITIES.add(entityCount, Attributes.of(METADATA_SOURCE, metadataSource));
        }
    }

    private static String failureType(Throwable exception) {
        if (hasException(exception, PermanentLockingException.class)
                || hasException(exception, TemporaryLockingException.class)) {
            return FAILURE_TYPE_GRAPH_LOCKING;
        } else if (hasException(exception, TemporaryBackendException.class)) {
            return FAILURE_TYPE_GRAPH_BACKEND;
        } else if (hasException(exception, JanusGraphException.class)) {
            return FAILURE_TYPE_JANUSGRAPH;
        } else if (hasException(exception, AtlasSchemaViolationException.class)) {
            return FAILURE_TYPE_SCHEMA_VIOLATION;
        } else if (hasException(exception, InterruptedException.class)) {
            return FAILURE_TYPE_INTERRUPTED;
        } else if (hasException(exception, AtlasBaseException.class)) {
            return FAILURE_TYPE_ATLAS_BASE_EXCEPTION;
        } else if (exception instanceof RuntimeException) {
            return FAILURE_TYPE_RUNTIME_EXCEPTION;
        }

        return FAILURE_TYPE_OTHER;
    }

    private static boolean hasException(Throwable exception, Class<? extends Throwable> type) {
        Throwable current = exception;

        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }

            current = current.getCause();
        }

        return false;
    }

    private static String extractHookSourceFromRoutedMetadata(String metadataSource) {
        Matcher matcher = ROUTED_METADATA_SOURCE_PATTERN.matcher(metadataSource);

        if (!matcher.matches()) {
            return metadataSource;
        }

        String hookSource = matcher.group(3);

        if (StringUtils.isBlank(hookSource)) {
            return OTHER_METADATA_SOURCE;
        }

        return hookSource.trim();
    }
}
