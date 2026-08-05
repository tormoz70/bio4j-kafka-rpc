package ru.sbrf.uamc.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
final class StreamSinkImpl implements StreamSink {

    private static final String STREAM_CANCELLED_MESSAGE = "Stream cancelled";

    private final Producer<String, byte[]> producer;
    private final String replyTopic;
    private final String correlationId;
    private final String method;
    /** If true, all chunks use correlationId as key (one partition, ordered). If false, key is null (scalable, order not guaranteed). */
    private final boolean ordered;
    private final Semaphore inFlight = new Semaphore(KafkaRpcConstants.DEFAULT_STREAM_MAX_IN_FLIGHT);
    private final AtomicBoolean ended = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    StreamSinkImpl(Producer<String, byte[]> producer, String replyTopic, String correlationId, String method, boolean ordered) {
        this.producer = producer;
        this.replyTopic = replyTopic;
        this.correlationId = correlationId;
        this.method = method;
        this.ordered = ordered;
    }

    @Override
    public void send(byte[] chunk) throws IOException {
        if (cancelled.get() || ended.get()) {
            throw new IOException("Stream already ended or cancelled");
        }
        try {
            inFlight.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for in-flight capacity", e);
        }
        if (cancelled.get() || ended.get()) {
            inFlight.release();
            throw new IOException("Stream already ended or cancelled");
        }
        String key = ordered ? correlationId : null;
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, key, chunk);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes(StandardCharsets.UTF_8) : new byte[0]);
        producer.send(record, (metadata, exception) -> {
            inFlight.release();
            if (exception != null) {
                log.error("{} streamId={} topic={}", KafkaRpcLogEvents.SEND_FAILED, correlationId, replyTopic, exception);
                cancel();
            }
        });
    }

    @Override
    public void end() throws IOException {
        if (!ended.compareAndSet(false, true)) {
            return;
        }
        waitForInFlight();
        try {
            inFlight.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for in-flight capacity", e);
        }
        String key = ordered ? correlationId : null;
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, key, new byte[0]);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes(StandardCharsets.UTF_8) : new byte[0])
                .add(KafkaRpcConstants.HEADER_STREAM_END, "true".getBytes(StandardCharsets.UTF_8));
        AtomicReference<Exception> sendError = new AtomicReference<>();
        producer.send(record, (metadata, exception) -> {
            inFlight.release();
            if (exception != null) {
                log.error("{} streamId={} topic={}", KafkaRpcLogEvents.SEND_FAILED, correlationId, replyTopic, exception);
                sendError.set(exception);
            }
        });
        waitForInFlight();
        Exception error = sendError.get();
        if (error != null) {
            throw new IOException("Failed to send stream end", error);
        }
    }

    @Override
    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) {
            return;
        }
        if (ended.get()) {
            return;
        }
        notifyClientCancelled();
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    private void waitForInFlight() throws IOException {
        try {
            inFlight.acquire(KafkaRpcConstants.DEFAULT_STREAM_MAX_IN_FLIGHT);
            inFlight.release(KafkaRpcConstants.DEFAULT_STREAM_MAX_IN_FLIGHT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for in-flight chunks", e);
        }
    }

    private void notifyClientCancelled() {
        try {
            String key = ordered ? correlationId : null;
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, key, new byte[0]);
            record.headers()
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                    .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes(StandardCharsets.UTF_8) : new byte[0])
                    .add(KafkaRpcConstants.HEADER_ERROR, STREAM_CANCELLED_MESSAGE.getBytes(StandardCharsets.UTF_8));
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.warn("{} streamId={} topic={}", KafkaRpcLogEvents.SEND_FAILED, correlationId, replyTopic, exception);
                }
            });
        } catch (Exception e) {
            log.warn("{} streamId={} topic={}", KafkaRpcLogEvents.SEND_FAILED, correlationId, replyTopic, e);
        }
    }
}
