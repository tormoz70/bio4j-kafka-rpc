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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
