package ru.sbrf.uamc.kafkarpc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamingCallImplTest {

    @Test
    void healthcheckNeedsConsecutiveFailuresBeforeClosingStream() {
        BlockingQueue<StreamChunk> queue = new LinkedBlockingQueue<>();
        AtomicInteger healthcheckCalls = new AtomicInteger();
        AtomicInteger errorCalls = new AtomicInteger();
        CountDownLatch onCloseLatch = new CountDownLatch(1);

        KafkaRpcChannel channel = new FakeHealthcheckChannel(healthcheckCalls);
        StreamingProcessor<byte[]> processor = new StreamingProcessor<>() {
            @Override
            public void onMessage(byte[] data) {
            }

            @Override
            public void onError(Throwable error) {
                errorCalls.incrementAndGet();
            }
        };

        StreamingCallImpl call = new StreamingCallImpl(
                "stream-1",
                queue,
                channel,
                "Svc/Stream",
                10,
                10,
                true,
                2,
                processor,
                onCloseLatch::countDown
        );

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertEquals(1, errorCalls.get()));
        assertTrue(healthcheckCalls.get() >= 4, "Expected reset between failures before stream closes");
        assertEquals(0, onCloseLatch.getCount());
        call.close();
    }

    @Test
    void processorExceptionIsForwardedToOnError() throws Exception {
        BlockingQueue<StreamChunk> queue = new LinkedBlockingQueue<>();
        CountDownLatch errorLatch = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();

        StreamingProcessor<byte[]> processor = new StreamingProcessor<>() {
            @Override
            public void onMessage(byte[] data) {
                throw new IllegalStateException("boom");
            }

            @Override
            public void onError(Throwable error) {
                errors.incrementAndGet();
                errorLatch.countDown();
            }
        };

        StreamingCallImpl call = new StreamingCallImpl(
                "stream-2",
                queue,
                new NoopChannel(),
                "Svc/Stream",
                1000,
                1000,
                false,
                processor,
                () -> {
                }
        );
        queue.offer(new StreamChunk.Data("chunk".getBytes()));

        assertTrue(errorLatch.await(2, TimeUnit.SECONDS));
        assertEquals(1, errors.get());
        call.close();
    }

    private static final class FakeHealthcheckChannel extends NoopChannel {
        private final AtomicInteger calls;

        private FakeHealthcheckChannel(AtomicInteger calls) {
            this.calls = calls;
        }

        @Override
        public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes, Map<String, String> headers) {
            int n = calls.incrementAndGet();
            if (n == 2) {
                return CompletableFuture.completedFuture(new byte[0]);
            }
            CompletableFuture<byte[]> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException("simulated hc failure " + n));
            return failed;
        }
    }

    private static class NoopChannel implements KafkaRpcChannel {
        @Override
        public String getRequestTopic() {
            return "request";
        }

        @Override
        public String getReplyTopic() {
            return "reply";
        }

        @Override
        public byte[] request(String correlationId, byte[] requestBytes) throws IOException, TimeoutException {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] request(String correlationId, byte[] requestBytes, Map<String, String> headers) throws IOException, TimeoutException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void send(String correlationId, byte[] requestBytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void send(String correlationId, byte[] requestBytes, Map<String, String> headers) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startStream(String correlationId, byte[] requestBytes, Map<String, String> headers, StreamingProcessor<byte[]> processor) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes) {
            return requestAsync(correlationId, requestBytes, null);
        }

        @Override
        public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes, Map<String, String> headers) {
            return CompletableFuture.completedFuture(new byte[0]);
        }

        @Override
        public void close() {
        }

        @Override
        public void closeStream(String correlationId) {
        }
    }
}
