package ru.sbrf.uamc.kafkarpc;

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

import static org.junit.jupiter.api.Assertions.*;

class KafkaRpcServerTest {

    private static final String REQUEST_TOPIC = "request";
    private static final String REPLY_TOPIC = "reply";

    private MockProducer<String, byte[]> producer;
    private MockConsumer<String, byte[]> consumer;
    private KafkaRpcServer server;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.subscribe(Collections.singletonList(REQUEST_TOPIC));
        consumer.rebalance(Collections.singletonList(new TopicPartition(REQUEST_TOPIC, 0)));
        consumer.updateBeginningOffsets(Map.of(new TopicPartition(REQUEST_TOPIC, 0), 0L));
        consumer.updateEndOffsets(Map.of(new TopicPartition(REQUEST_TOPIC, 0), 1L));
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void processesRequestAndSendsReply() throws Exception {
        String correlationId = "corr-1";
        String method = "Service/Method";
        byte[] requestData = "request".getBytes();
        byte[] expectedResponse = "response".getBytes();

        var handlers = Map.<String, KafkaRpcServer.MethodHandler>of(
                method, (cid, req) -> {
                    assertEquals(correlationId, cid);
                    assertArrayEquals(requestData, req);
                    return expectedResponse;
                });

        server = new KafkaRpcServer(consumer, producer, REQUEST_TOPIC, handlers);
        server.start();

        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_METHOD, method.getBytes()));
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_REPLY_TOPIC, REPLY_TOPIC.getBytes()));
        consumer.addRecord(new ConsumerRecord<>(REQUEST_TOPIC, 0, 0, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, correlationId, requestData, headers, java.util.Optional.empty()));

        // Give server thread time to poll and process
        Thread.sleep(200);

        server.stop();
        Thread.sleep(100);

        assertEquals(1, producer.history().size());
        var sentRecord = producer.history().get(0);
        assertEquals(REPLY_TOPIC, sentRecord.topic());
        assertArrayEquals(expectedResponse, sentRecord.value());
    }

    @Test
    void singleHandlerUsedWhenMethodNotSpecified() throws Exception {
        String correlationId = "corr-2";
        byte[] requestData = "req".getBytes();
        byte[] expectedResponse = "resp".getBytes();

        var handlers = Map.<String, KafkaRpcServer.MethodHandler>of(
                "OnlyMethod", (cid, req) -> expectedResponse);

        server = new KafkaRpcServer(consumer, producer, REQUEST_TOPIC, handlers);
        server.start();

        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes()));
        headers.add(new RecordHeader(KafkaRpcConstants.HEADER_REPLY_TOPIC, REPLY_TOPIC.getBytes()));
        consumer.addRecord(new ConsumerRecord<>(REQUEST_TOPIC, 0, 0, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, correlationId, requestData, headers, java.util.Optional.empty()));

        Thread.sleep(200);
        server.stop();
        Thread.sleep(100);

        assertEquals(1, producer.history().size());
        assertArrayEquals(expectedResponse, producer.history().get(0).value());
    }

    @Test
    void dropsMessageWithoutCorrelationId() throws Exception {
        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        consumer.addRecord(new ConsumerRecord<>(REQUEST_TOPIC, 0, 0, 0L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "data".getBytes(), headers, java.util.Optional.empty()));

        var handlers = Map.<String, KafkaRpcServer.MethodHandler>of(
                "Any", (cid, req) -> "unexpected".getBytes());

        server = new KafkaRpcServer(consumer, producer, REQUEST_TOPIC, handlers);
        server.start();
        Thread.sleep(200);
        server.stop();
        Thread.sleep(100);

        assertEquals(0, producer.history().size());
    }
}
