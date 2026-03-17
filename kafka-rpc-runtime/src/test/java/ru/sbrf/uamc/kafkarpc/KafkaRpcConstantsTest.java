package ru.sbrf.uamc.kafkarpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaRpcConstantsTest {

    @Test
    void constantsHaveExpectedValues() {
        assertEquals("kafka-rpc-correlation-id", KafkaRpcConstants.HEADER_CORRELATION_ID);
        assertEquals("kafka-rpc-method", KafkaRpcConstants.HEADER_METHOD);
        assertEquals("kafka-rpc-reply-topic", KafkaRpcConstants.HEADER_REPLY_TOPIC);
        assertEquals(30_000, KafkaRpcConstants.DEFAULT_TIMEOUT_MS);
    }
}
