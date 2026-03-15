package io.bio4j.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
final class StreamSinkImpl implements StreamSink {

    private final Producer<String, byte[]> producer;
    private final String replyTopic;
    private final String correlationId;
    private final String method;
    /** If true, all chunks use correlationId as key (one partition, ordered). If false, key is null (scalable, order not guaranteed). */
    private final boolean ordered;
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
        String key = ordered ? correlationId : null;
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, key, chunk);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes(StandardCharsets.UTF_8) : new byte[0]);
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IOException("Failed to send stream chunk", e);
        }
    }

    @Override
    public void end() throws IOException {
        if (!ended.compareAndSet(false, true)) return;
        String key = ordered ? correlationId : null;
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, key, new byte[0]);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes(StandardCharsets.UTF_8) : new byte[0])
                .add(KafkaRpcConstants.HEADER_STREAM_END, "true".getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IOException("Failed to send stream end", e);
        }
    }

    @Override
    public void cancel() {
        cancelled.set(true);
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }
}
