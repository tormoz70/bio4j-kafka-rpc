package io.bio4j.kafkarpc;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

final class StreamSinkImpl implements StreamSink {

    private final Producer<String, byte[]> producer;
    private final String replyTopic;
    private final String correlationId;
    private final String method;
    private final AtomicBoolean ended = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    StreamSinkImpl(Producer<String, byte[]> producer, String replyTopic, String correlationId, String method) {
        this.producer = producer;
        this.replyTopic = replyTopic;
        this.correlationId = correlationId;
        this.method = method;
    }

    @Override
    public void send(byte[] chunk) throws IOException {
        if (cancelled.get() || ended.get()) {
            throw new IOException("Stream already ended or cancelled");
        }
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, correlationId, chunk);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes() : new byte[0]);
        producer.send(record);
    }

    @Override
    public void end() throws IOException {
        if (!ended.compareAndSet(false, true)) return;
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, correlationId, new byte[0]);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes() : new byte[0])
                .add(KafkaRpcConstants.HEADER_STREAM_END, "true".getBytes());
        producer.send(record);
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
