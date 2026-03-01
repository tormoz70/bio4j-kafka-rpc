package io.bio4j.kafkarpc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class KafkaRpcChannelTest {

    private static final String REQUEST_TOPIC = "request";
    private static final String REPLY_TOPIC = "reply";

    private MockProducer<String, byte[]> producer;
    private MockConsumer<String, byte[]> consumer;
    private KafkaRpcChannel channel;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.subscribe(Collections.singletonList(REPLY_TOPIC));
        consumer.rebalance(Collections.singletonList(new TopicPartition(REPLY_TOPIC, 0)));
        consumer.updateBeginningOffsets(Map.of(new TopicPartition(REPLY_TOPIC, 0), 0L));
        consumer.updateEndOffsets(Map.of(new TopicPartition(REPLY_TOPIC, 0), 1L));

        channel = new KafkaRpcChannel(producer, consumer, REQUEST_TOPIC, REPLY_TOPIC, 5_000);
    }

    @AfterEach
    void tearDown() {
        channel.close();
    }

    @Test
    void requestReturnsResponseWhenReplyMatchesCorrelationId() throws Exception {
        String correlationId = "corr-123";
        byte[] requestData = "request".getBytes();
        byte[] responseData = "response".getBytes();

        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
        consumer.addRecord(new ConsumerRecord<>(REPLY_TOPIC, 0, 0, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, correlationId, responseData, headers, java.util.Optional.empty()));

        byte[] result = channel.request(correlationId, requestData);

        assertArrayEquals(responseData, result);
        assertEquals(1, producer.history().size());
        assertEquals(REQUEST_TOPIC, producer.history().get(0).topic());
        assertArrayEquals(requestData, producer.history().get(0).value());
    }

    @Test
    void requestWithHeadersAddsHeadersToRecord() throws Exception {
        String correlationId = "corr-456";
        byte[] requestData = "req".getBytes();
        byte[] responseData = "resp".getBytes();

        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
        consumer.addRecord(new ConsumerRecord<>(REPLY_TOPIC, 0, 0, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, correlationId, responseData, headers, java.util.Optional.empty()));

        channel.request(correlationId, requestData, Map.of(KafkaRpcConstants.HEADER_METHOD, "Foo/Bar"));

        assertEquals(1, producer.history().size());
        var sentHeaders = producer.history().get(0).headers();
        assertNotNull(sentHeaders.headers(KafkaRpcConstants.HEADER_METHOD).iterator().next().value());
    }

    @Test
    void requestThrowsTimeoutWhenNoReply() {
        String correlationId = "no-reply";
        byte[] requestData = "request".getBytes();

        assertThrows(TimeoutException.class, () -> channel.request(correlationId, requestData));
    }

    @Test
    void gettersReturnExpectedValues() {
        assertEquals(REQUEST_TOPIC, channel.getRequestTopic());
        assertEquals(REPLY_TOPIC, channel.getReplyTopic());
    }

    @Test
    void requestAsyncReturnsResponse() throws Exception {
        String correlationId = "async-1";
        byte[] requestData = "req".getBytes();
        byte[] responseData = "resp".getBytes();

        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
        consumer.addRecord(new ConsumerRecord<>(REPLY_TOPIC, 0, 0, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, correlationId, responseData, headers, java.util.Optional.empty()));

        byte[] result = channel.requestAsync(correlationId, requestData).get();

        assertArrayEquals(responseData, result);
    }
}
