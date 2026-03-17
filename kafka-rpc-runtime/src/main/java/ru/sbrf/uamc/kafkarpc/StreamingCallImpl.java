package ru.sbrf.uamc.kafkarpc;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
final class StreamingCallImpl {

    private final String streamId;
    private final BlockingQueue<StreamChunk> queue;
    private final KafkaRpcChannel channel;
    private final String method;
    private final int healthcheckIntervalMs;
    private final int healthcheckTimeoutMs;
    private final StreamingProcessor<byte[]> processor;
    private final Runnable onClose;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong healthcheckSeq = new AtomicLong(0);
    private Thread healthcheckThread;
    private Thread drainThread;

    StreamingCallImpl(String streamId, BlockingQueue<StreamChunk> queue, KafkaRpcChannel channel,
                      String method, int healthcheckIntervalMs, int healthcheckTimeoutMs,
                      boolean enableHealthcheck, StreamingProcessor<byte[]> processor,
                      Runnable onClose) {
        this.streamId = streamId;
        this.queue = queue;
        this.channel = channel;
        this.method = method;
        this.healthcheckIntervalMs = healthcheckIntervalMs;
        this.healthcheckTimeoutMs = healthcheckTimeoutMs;
        this.processor = processor;
        this.onClose = onClose;
        if (enableHealthcheck) {
            this.healthcheckThread = Thread.ofVirtual().name("stream-hc-" + streamId).start(this::runHealthcheck);
        }
        this.drainThread = Thread.ofVirtual().name("stream-drain-" + streamId).start(this::drain);
    }

    private void runHealthcheck() {
        while (!closed.get()) {
            try {
                Thread.sleep(healthcheckIntervalMs);
                if (closed.get()) break;
                String hcCorrelationId = streamId + "-hc-" + healthcheckSeq.incrementAndGet();
                Map<String, String> hcHeaders = new java.util.HashMap<>();
                hcHeaders.put(KafkaRpcConstants.HEADER_METHOD, method + KafkaRpcConstants.STREAM_HEALTHCHECK_SUFFIX);
                hcHeaders.put(KafkaRpcConstants.HEADER_STREAM_ID, streamId);
                CompletableFuture<byte[]> future = channel.requestAsync(hcCorrelationId, new byte[0], hcHeaders);
                try {
                    future.get(healthcheckTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    log.warn("Stream healthcheck failed for {}: {}", streamId, e.getMessage());
                    queue.add(new StreamChunk.Poison(
                            new IllegalStateException("Stream dead: no healthcheck response", e)));
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void drain() {
        try {
            while (!closed.get()) {
                StreamChunk chunk = queue.take();
                if (chunk instanceof StreamChunk.Data data) {
                    try {
                        processor.onMessage(data.bytes());
                    } catch (Exception e) {
                        log.warn("Processor onMessage error for {}: {}", streamId, e.getMessage());
                        processor.onError(e);
                        break;
                    }
                } else if (chunk instanceof StreamChunk.End) {
                    processor.onFinish();
                    break;
                } else if (chunk instanceof StreamChunk.Poison poison) {
                    processor.onError(poison.cause());
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            processor.onError(e);
        } finally {
            if (closed.compareAndSet(false, true)) {
                if (healthcheckThread != null) {
                    healthcheckThread.interrupt();
                }
                if (onClose != null) {
                    onClose.run();
                }
            }
        }
    }
}
