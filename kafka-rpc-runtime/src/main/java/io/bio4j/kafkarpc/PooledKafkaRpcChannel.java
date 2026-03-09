package io.bio4j.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared channel: one producer + one consumer thread per instance. Dispatches replies by correlationId
 * to waiting callers. Used by the channel pool (one per client name).
 */
@Slf4j
public class PooledKafkaRpcChannel implements KafkaRpcChannel {

    private final KafkaProducer<String, byte[]> producer;
    private final String requestTopic;
    private final String replyTopic;
    private final int timeoutMs;
    private final boolean streamHealthcheckEnabled;
    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> pending = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<StreamChunk>> streamQueues = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread consumerThread;

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs) {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs, true);
    }

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs,
                                 boolean streamHealthcheckEnabled) {
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.timeoutMs = timeoutMs;
        this.streamHealthcheckEnabled = streamHealthcheckEnabled;

        Properties prod = new Properties();
        prod.putAll(producerConfig);
        prod.putIfAbsent("key.serializer", StringSerializer.class.getName());
        prod.putIfAbsent("value.serializer", ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(prod);

        Properties cons = new Properties();
        cons.putAll(consumerConfig);
        cons.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        cons.putIfAbsent("value.deserializer", ByteArrayDeserializer.class.getName());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(cons);
        consumer.subscribe(Collections.singletonList(replyTopic));

        this.consumerThread = Thread.ofVirtual().name("kafka-rpc-pool-" + replyTopic).start(() -> runConsumer(consumer));
    }

    private void runConsumer(KafkaConsumer<String, byte[]> consumer) {
        try {
            while (!closed.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> r : records) {
                    String correlationId = getHeader(r, KafkaRpcConstants.HEADER_CORRELATION_ID);
                    if (correlationId == null) continue;
                    boolean streamEnd = getHeader(r, KafkaRpcConstants.HEADER_STREAM_END) != null;
                    BlockingQueue<StreamChunk> sq = streamQueues.get(correlationId);
                    if (sq != null) {
                        if (streamEnd) {
                            sq.add(new StreamChunk.End());
                            streamQueues.remove(correlationId);
                        } else {
                            sq.add(new StreamChunk.Data(r.value()));
                        }
                        continue;
                    }
                    CompletableFuture<byte[]> f = pending.remove(correlationId);
                    if (f != null) {
                        f.complete(r.value());
                    }
                }
            }
        } catch (Exception e) {
            if (!closed.get()) {
                log.warn("Pool consumer error for {}: {}", replyTopic, e.getMessage());
            }
        } finally {
            consumer.close();
        }
    }

    private static String getHeader(ConsumerRecord<String, byte[]> record, String name) {
        var iter = record.headers().headers(name).iterator();
        if (iter.hasNext()) {
            byte[] v = iter.next().value();
            return v != null ? new String(v) : null;
        }
        return null;
    }

    @Override
    public String getRequestTopic() { return requestTopic; }

    @Override
    public String getReplyTopic() { return replyTopic; }

    @Override
    public byte[] request(String correlationId, byte[] requestBytes) throws IOException, TimeoutException {
        return request(correlationId, requestBytes, null);
    }

    @Override
    public byte[] request(String correlationId, byte[] requestBytes, Map<String, String> headers)
            throws IOException, TimeoutException {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pending.put(correlationId, future);

        try {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
            record.headers()
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                    .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes());
            if (headers != null) {
                headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes() : new byte[0]));
            }
            producer.send(record);

            byte[] result = future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            return result;
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IOException(e.getCause());
        } catch (java.util.concurrent.TimeoutException e) {
            pending.remove(correlationId);
            throw new TimeoutException("No response within " + timeoutMs + " ms for correlationId=" + correlationId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pending.remove(correlationId);
            throw new IOException(e);
        }
    }

    @Override
    public void send(String correlationId, byte[] requestBytes) throws IOException {
        send(correlationId, requestBytes, null);
    }

    @Override
    public void send(String correlationId, byte[] requestBytes, Map<String, String> headers) throws IOException {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
        record.headers().add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes());
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes() : new byte[0]));
        }
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IOException("Oneway send failed", e);
        }
    }

    @Override
    public StreamingCall startStream(String correlationId, byte[] requestBytes, Map<String, String> headers) throws IOException {
        BlockingQueue<StreamChunk> queue = new LinkedBlockingQueue<>();
        streamQueues.put(correlationId, queue);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
        record.headers().add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes());
        record.headers().add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes());
        record.headers().add(KafkaRpcConstants.HEADER_IS_STREAM, "true".getBytes());
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes() : new byte[0]));
        }
        producer.send(record);
        String method = headers != null ? headers.get(KafkaRpcConstants.HEADER_METHOD) : "";
        StreamingCallImpl call = new StreamingCallImpl(correlationId, queue, this, method,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS,
                streamHealthcheckEnabled);
        call.setOnClose(() -> streamQueues.remove(correlationId));
        return call;
    }

    @Override
    public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes) {
        return requestAsync(correlationId, requestBytes, null);
    }

    @Override
    public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes, Map<String, String> headers) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pending.put(correlationId, future);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes());
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes() : new byte[0]));
        }
        producer.send(record);

        return future.orTimeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                .whenComplete((v, t) -> pending.remove(correlationId));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            producer.close();
            consumerThread.interrupt();
        }
    }
}
