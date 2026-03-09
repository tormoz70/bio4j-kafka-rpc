package io.bio4j.kafkarpc;

import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
final class StreamingCallImpl implements StreamingCall {

    private final String streamId;
    private final BlockingQueue<StreamChunk> queue;
    private final KafkaRpcChannel channel;
    private final String method;
    private final int healthcheckIntervalMs;
    private final int healthcheckTimeoutMs;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong healthcheckSeq = new AtomicLong(0);
    private Thread healthcheckThread;
    private volatile Runnable onClose;

    void setOnClose(Runnable onClose) {
        this.onClose = onClose;
    }

    StreamingCallImpl(String streamId, BlockingQueue<StreamChunk> queue, KafkaRpcChannel channel,
                     String method, int healthcheckIntervalMs, int healthcheckTimeoutMs,
                     boolean enableHealthcheck) {
        this.streamId = streamId;
        this.queue = queue;
        this.channel = channel;
        this.method = method;
        this.healthcheckIntervalMs = healthcheckIntervalMs;
        this.healthcheckTimeoutMs = healthcheckTimeoutMs;
        if (enableHealthcheck) {
            this.healthcheckThread = Thread.ofVirtual().name("stream-hc-" + streamId).start(this::runHealthcheck);
        }
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

    @Override
    public Iterator<byte[]> iterator() {
        return new Iterator<>() {
            private StreamChunk nextChunk;
            private boolean ended;

            @Override
            public boolean hasNext() {
                if (ended) return false;
                if (nextChunk != null) {
                    if (nextChunk instanceof StreamChunk.End) {
                        ended = true;
                        return false;
                    }
                    if (nextChunk instanceof StreamChunk.Poison p) {
                        throw new RuntimeException("Stream failed", p.cause());
                    }
                    return true;
                }
                try {
                    nextChunk = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                if (nextChunk instanceof StreamChunk.End) {
                    ended = true;
                    return false;
                }
                if (nextChunk instanceof StreamChunk.Poison p) {
                    throw new RuntimeException("Stream failed", p.cause());
                }
                return true;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                byte[] bytes = ((StreamChunk.Data) nextChunk).bytes();
                nextChunk = null;
                return bytes;
            }
        };
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
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
