package ru.sbrf.uamc.kafkarpc;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSinkImplTest {

    @Test
    void sendsStreamEndAfterChunkDelivery() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-1", "Svc/Stream", true);

        sink.send("chunk".getBytes(StandardCharsets.UTF_8));
        sink.end();

        assertEquals(2, producer.history().size());
        assertArrayEquals("chunk".getBytes(StandardCharsets.UTF_8), producer.history().get(0).value());
        assertNotNull(producer.history().get(1).headers().lastHeader(KafkaRpcConstants.HEADER_STREAM_END));
    }

    @Test
    void endFailsWhenChunkSendFailedAsynchronously() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-2", "Svc/Stream", true);

        sink.send("chunk".getBytes(StandardCharsets.UTF_8));
        producer.errorNext(new RuntimeException("simulated broker failure"));

        IOException error = assertThrows(IOException.class, sink::end);
        assertEquals("Failed to send stream chunk", error.getMessage());
    }
}
