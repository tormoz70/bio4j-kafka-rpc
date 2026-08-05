package ru.sbrf.uamc.kafkarpc;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;

class StreamSinkImplTest {

    @Test
    @Timeout(2)
    void sendsStreamEndAfterChunkDelivery() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-1", "Svc/Stream", true);

        sink.send("chunk".getBytes(StandardCharsets.UTF_8));
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertEquals(1, producer.history().size()));
        sink.end();

        assertEquals(2, producer.history().size());
        assertArrayEquals("chunk".getBytes(StandardCharsets.UTF_8), producer.history().get(0).value());
        assertNotNull(producer.history().get(1).headers().lastHeader(KafkaRpcConstants.HEADER_STREAM_END));
    }

    @Test
    @Timeout(2)
    void sendReturnsBeforeBrokerAcknowledges() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-async", "Svc/Stream", true);

        long startNanos = System.nanoTime();
        sink.send("chunk".getBytes(StandardCharsets.UTF_8));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertEquals(1, producer.history().size());
        assertTrue(elapsedMs < 500, "send should return without waiting for broker ack");
        producer.completeNext();
    }

    @Test
    @Timeout(2)
    void sendFailureCancelsStreamAsynchronously() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-2", "Svc/Stream", true);

        sink.send("chunk".getBytes(StandardCharsets.UTF_8));
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertEquals(1, producer.history().size()));
        producer.errorNext(new RuntimeException("simulated broker failure"));

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
            assertTrue(sink.isCancelled());
            assertEquals(2, producer.history().size());
            assertNotNull(producer.history().get(1).headers().lastHeader(KafkaRpcConstants.HEADER_ERROR));
        });
    }

    @Test
    @Timeout(2)
    void endReturnsBeforeBrokerAcknowledges() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-end-async", "Svc/Stream", true);

        sink.send("chunk".getBytes(StandardCharsets.UTF_8));
        producer.completeNext();

        long startNanos = System.nanoTime();
        Thread endThread = Thread.ofVirtual().start(() -> {
            try {
                sink.end();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertEquals(2, producer.history().size()));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertTrue(elapsedMs < 500, "end should enqueue STREAM_END without waiting for broker ack");
        assertNotNull(producer.history().get(1).headers().lastHeader(KafkaRpcConstants.HEADER_STREAM_END));
        producer.completeNext();
        endThread.join(1000);
    }

    @Test
    @Timeout(5)
    void endFailureThrowsIOException() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-end-fail", "Svc/Stream", true);

        sink.send("chunk".getBytes(StandardCharsets.UTF_8));
        producer.completeNext();

        AtomicReference<IOException> error = new AtomicReference<>();
        Thread endThread = Thread.ofVirtual().start(() -> {
            try {
                sink.end();
            } catch (IOException e) {
                error.set(e);
            }
        });

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertEquals(2, producer.history().size()));
        producer.errorNext(new RuntimeException("simulated broker failure"));

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertNotNull(error.get()));
        endThread.join(1000);
        assertTrue(error.get().getMessage().contains("Failed to send stream end"));
    }

    @Test
    @Timeout(2)
    void cancelNotifiesClientWithErrorHeader() {
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-3", "Svc/Stream", true);

        sink.cancel();

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertEquals(1, producer.history().size()));
        var record = producer.history().get(0);
        assertNotNull(record.headers().lastHeader(KafkaRpcConstants.HEADER_ERROR));
    }
}
