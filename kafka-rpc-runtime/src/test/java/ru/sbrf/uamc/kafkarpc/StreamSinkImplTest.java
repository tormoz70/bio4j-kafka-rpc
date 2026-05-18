package ru.sbrf.uamc.kafkarpc;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSinkImplTest {

    @Test
    @Timeout(2)
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
    @Timeout(2)
    void sendFailsWhenChunkSendFailedAsynchronously() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
        StreamSinkImpl sink = new StreamSinkImpl(producer, "reply", "corr-2", "Svc/Stream", true);
        CompletableFuture<Void> sendFuture = CompletableFuture.runAsync(() -> {
            try {
                sink.send("chunk".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        long waitDeadline = System.currentTimeMillis() + 1_000;
        while (producer.history().isEmpty() && System.currentTimeMillis() < waitDeadline) {
            Thread.sleep(10);
        }
        assertEquals(1, producer.history().size());
        producer.errorNext(new RuntimeException("simulated broker failure"));

        ExecutionException exec = assertThrows(ExecutionException.class,
                () -> sendFuture.get(1, TimeUnit.SECONDS));
        Throwable runtime = exec.getCause();
        assertNotNull(runtime);
        Throwable io = runtime.getCause();
        assertInstanceOf(IOException.class, io);
        IOException error = (IOException) io;
        assertEquals("Failed to send stream chunk", error.getMessage());
    }
}
