package ru.sbrf.uamc.kafkarpc;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
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
            this.healthcheckThread = Thread.ofVirtual()
                    .name("stream-hc-" + streamId)
                    .start(this::runHealthcheck);
        }
        this.drainThread = Thread.ofVirtual()
                .name("stream-drain-" + streamId)
                .start(this::drain);
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
                    // Используем offer() вместо add() + гарантированная очистка
                    poisonQueue("Stream dead: no healthcheck response", e);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Ловим любые неожиданные исключения, чтобы поток не умер молча
                log.error("Unexpected error in healthcheck thread for {}: {}", streamId, e.getMessage(), e);
                poisonQueue("Healthcheck thread error", e);
                break;
            }
            }
        }

    // Вынесено в отдельный метод для повторного использования и безопасности
    private void poisonQueue(String message, Throwable cause) {
        try {
            // offer() не бросает исключение при переполнении
            if (!queue.offer(new StreamChunk.Poison(new IllegalStateException(message, cause)),
                            100, TimeUnit.MILLISECONDS)) {
                log.warn("{}: Failed to poison queue (full)", streamId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("{}: Interrupted while poisoning queue", streamId, e);
        }
    }

    private void drain() {
        try {
            while (!closed.get()) {
                // Используем poll с таймаутом, чтобы периодически проверять closed
                StreamChunk chunk = queue.poll(50, TimeUnit.MILLISECONDS);
                if (chunk == null) {
                    continue; // Таймаут — проверяем closed и продолжаем
                }

                try {
                if (chunk instanceof StreamChunk.Data data) {
                        processor.onMessage(data.bytes());
                } else if (chunk instanceof StreamChunk.End) {
                    processor.onFinish();
                    break;
                } else if (chunk instanceof StreamChunk.Poison poison) {
                    processor.onError(poison.cause());
                    break;
                }
                } catch (Exception e) {
                    log.warn("Processor error for {}: {}", streamId, e.getMessage(), e);
                    try {
                        processor.onError(e);
                    } catch (Exception ex) {
                        log.error("processor.onError() threw exception for {}: {}", streamId, ex.getMessage(), ex);
                        // Не даём исключению прервать finally
                    }
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            try {
            processor.onError(e);
            } catch (Exception ex) {
                log.error("processor.onError() threw exception on interrupt for {}: {}", streamId, ex.getMessage(), ex);
            }
        } finally {
            // Гарантированная очистка ресурсов
            cleanup();
        }
    }

    private void cleanup() {
            if (closed.compareAndSet(false, true)) {
            log.debug("{}: Cleaning up streaming call", streamId);

            // Останавливаем healthcheck-поток
                if (healthcheckThread != null) {
                    healthcheckThread.interrupt();
                }

            // Очищаем очередь от оставшихся элементов (помогаем GC)
            queue.clear();

            // Гарантированно вызываем onClose, даже если он выбросит
                if (onClose != null) {
                try {
                    onClose.run();
                } catch (Exception e) {
                    log.error("{}: onClose() threw exception: {}", streamId, e.getMessage(), e);
                    // Не пробрасываем дальше — cleanup должен завершиться
                }
            }
        }
    }

    // Публичный метод для внешнего закрытия (вызывается из PooledKafkaRpcChannel.close())
    void close() {
        if (closed.get()) {
            return;
}
        log.debug("{}: External close requested", streamId);
        poisonQueue("Stream closed externally", new IOException("Channel closed"));

        // Ждём завершения drain-потока с таймаутом
        if (drainThread != null && Thread.currentThread() != drainThread) {
            try {
                drainThread.join(2000); // Ждём до 2 секунд
                if (drainThread.isAlive()) {
                    log.warn("{}: drainThread did not terminate in time", streamId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("{}: Interrupted while waiting for drainThread", streamId);
            }
        }
    }
}