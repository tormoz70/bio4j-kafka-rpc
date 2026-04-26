package ru.sbrf.uamc.kafkarpc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PooledKafkaRpcChannelTest {

    private static final String REQUEST_TOPIC = "request";
    private static final String REPLY_TOPIC = "reply";

    private MockProducer<String, byte[]> producer;
    private MockConsumer<String, byte[]> consumer;
    private PooledKafkaRpcChannel channel;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        channel = new PooledKafkaRpcChannel(
                producer,
                List.of(consumer),
                REQUEST_TOPIC,
                REPLY_TOPIC,
                5_000,
                10
        );
    }

    @AfterEach
    void tearDown() {
        channel.close();
    }

    @Test
    void requestReturnsResponseWhenReplyMatchesCorrelationId() throws Exception {
        String correlationId = "corr-1";
        byte[] request = "req".getBytes();
        byte[] response = "resp".getBytes();

        consumer.schedulePollTask(() -> {
            TopicPartition tp = new TopicPartition(REPLY_TOPIC, 0);
            consumer.rebalance(List.of(tp));
            consumer.updateBeginningOffsets(Map.of(tp, 0L));
            consumer.updateEndOffsets(Map.of(tp, 1L));
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
            consumer.addRecord(new ConsumerRecord<>(
                    REPLY_TOPIC, 0, 0L, 0L, TimestampType.CREATE_TIME, 0, 0,
                    correlationId, response, headers, Optional.empty()
            ));
        });

        byte[] result = channel.request(correlationId, request);

        assertArrayEquals(response, result);
        assertEquals(1, producer.history().size());
        assertEquals(REQUEST_TOPIC, producer.history().get(0).topic());
        assertArrayEquals(request, producer.history().get(0).value());
    }

    @Test
    void requestWithHeadersAddsHeadersToRecord() throws Exception {
        String correlationId = "corr-2";
        byte[] request = "req".getBytes();
        byte[] response = "resp".getBytes();

        consumer.schedulePollTask(() -> {
            TopicPartition tp = new TopicPartition(REPLY_TOPIC, 0);
            consumer.rebalance(List.of(tp));
            consumer.updateBeginningOffsets(Map.of(tp, 0L));
            consumer.updateEndOffsets(Map.of(tp, 1L));
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
            consumer.addRecord(new ConsumerRecord<>(
                    REPLY_TOPIC, 0, 0L, 0L, TimestampType.CREATE_TIME, 0, 0,
                    correlationId, response, headers, Optional.empty()
            ));
        });

        channel.request(correlationId, request, Map.of(KafkaRpcConstants.HEADER_METHOD, "Svc/Method"));

        var sentHeaders = producer.history().get(0).headers();
        assertNotNull(sentHeaders.headers(KafkaRpcConstants.HEADER_METHOD).iterator().next().value());
    }

    @Test
    void requestThrowsTimeoutWhenNoReply() {
        String correlationId = "corr-timeout";
        byte[] request = "req".getBytes();
        MockProducer<String, byte[]> timeoutProducer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        MockConsumer<String, byte[]> timeoutConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        PooledKafkaRpcChannel timeoutChannel = new PooledKafkaRpcChannel(
                timeoutProducer, List.of(timeoutConsumer), REQUEST_TOPIC, REPLY_TOPIC, 50, 10);
        try {
            assertThrows(TimeoutException.class, () -> timeoutChannel.request(correlationId, request));
        } finally {
            timeoutChannel.close();
        }
    }

    @Test
    void rejectsDuplicateCorrelationIdForSyncRequest() {
        String correlationId = "dup-sync";
        CompletableFuture<byte[]> first = channel.requestAsync(correlationId, "req-1".getBytes());

        IOException error = assertThrows(IOException.class,
                () -> channel.request(correlationId, "req-2".getBytes()));
        assertTrue(error.getMessage().contains("Duplicate correlationId"));
        assertEquals(1L, channel.duplicateCorrelationIdCount());
        assertThrows(TimeoutException.class, () -> first.get(100, java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    void rejectsDuplicateCorrelationIdForAsyncRequest() {
        String correlationId = "dup-async";
        CompletableFuture<byte[]> first = channel.requestAsync(correlationId, "req-1".getBytes());
        CompletableFuture<byte[]> duplicate = channel.requestAsync(correlationId, "req-2".getBytes());

        ExecutionException exec = assertThrows(ExecutionException.class, duplicate::get);
        assertTrue(exec.getCause() instanceof IOException);
        assertTrue(exec.getCause().getMessage().contains("Duplicate correlationId"));
        assertEquals(1L, channel.duplicateCorrelationIdCount());
        assertThrows(TimeoutException.class, () -> first.get(100, java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    void allowsReusingCorrelationIdAfterRequestCompletion() throws Exception {
        String correlationId = "reuse-id";

        consumer.schedulePollTask(() -> {
            TopicPartition tp = new TopicPartition(REPLY_TOPIC, 0);
            consumer.rebalance(List.of(tp));
            consumer.updateBeginningOffsets(Map.of(tp, 0L));
            consumer.updateEndOffsets(Map.of(tp, 2L));
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
            consumer.addRecord(new ConsumerRecord<>(
                    REPLY_TOPIC, 0, 0L, 0L, TimestampType.CREATE_TIME, 0, 0,
                    correlationId, "resp-1".getBytes(), headers, Optional.empty()
            ));
        });
        byte[] firstResult = channel.request(correlationId, "req-1".getBytes());
        assertArrayEquals("resp-1".getBytes(), firstResult);

        consumer.schedulePollTask(() -> {
            TopicPartition tp = new TopicPartition(REPLY_TOPIC, 0);
            consumer.updateEndOffsets(Map.of(tp, 2L));
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
            consumer.addRecord(new ConsumerRecord<>(
                    REPLY_TOPIC, 0, 1L, 0L, TimestampType.CREATE_TIME, 0, 0,
                    correlationId, "resp-2".getBytes(), headers, Optional.empty()
            ));
        });
        byte[] secondResult = channel.request(correlationId, "req-2".getBytes());
        assertArrayEquals("resp-2".getBytes(), secondResult);
    }
}
