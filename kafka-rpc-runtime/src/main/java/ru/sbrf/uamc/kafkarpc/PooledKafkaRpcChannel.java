package ru.sbrf.uamc.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared channel: one producer + one or more consumer threads per instance (same consumer group for reply topic).
 * Dispatches replies by correlationId to waiting callers. Used by the channel pool (one per client name).
 * Multiple consumers enable scaling via partitioning of the reply topic.
 */
@Slf4j
public class PooledKafkaRpcChannel implements KafkaRpcChannel {
    private static final long CONSUMER_RECOVERY_INITIAL_BACKOFF_MS = 1_000L;
    private static final long CONSUMER_RECOVERY_MAX_BACKOFF_MS = 30_000L;

    private final Producer<String, byte[]> producer;
    private final String requestTopic;
    private final String replyTopic;
    private final int timeoutMs;
    private final boolean streamHealthcheckEnabled;
    private final int streamHealthcheckIntervalMs;
    private final int streamHealthcheckTimeoutMs;
    private final long streamServerIdleTimeoutMs;
    private final int pollIntervalMs;
    private final int streamBufferSize;
    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> pending = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<StreamChunk>> streamQueues = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<Thread> consumerThreads = new ArrayList<>();
    private final Properties consumerConfigBase;

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs) {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs, true,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS,
                KafkaRpcConstants.DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS, 1,
                KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS, KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE);
    }

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs,
                                 boolean streamHealthcheckEnabled) {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs, streamHealthcheckEnabled,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS,
                KafkaRpcConstants.DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS, 1,
                KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS, KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE);
    }

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs,
                                 boolean streamHealthcheckEnabled,
                                 int streamHealthcheckIntervalMs, int streamHealthcheckTimeoutMs,
                                 long streamServerIdleTimeoutMs) {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs, streamHealthcheckEnabled,
                streamHealthcheckIntervalMs, streamHealthcheckTimeoutMs, streamServerIdleTimeoutMs, 1,
                KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS, KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE);
    }

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs,
                                 boolean streamHealthcheckEnabled,
                                 int streamHealthcheckIntervalMs, int streamHealthcheckTimeoutMs,
                                 long streamServerIdleTimeoutMs,
                                 int consumerCount) {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs, streamHealthcheckEnabled,
                streamHealthcheckIntervalMs, streamHealthcheckTimeoutMs, streamServerIdleTimeoutMs, consumerCount,
                KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS, KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE);
    }

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs,
                                 boolean streamHealthcheckEnabled,
                                 int streamHealthcheckIntervalMs, int streamHealthcheckTimeoutMs,
                                 long streamServerIdleTimeoutMs,
                                 int consumerCount,
                                 int pollIntervalMs, int streamBufferSize) {
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.timeoutMs = timeoutMs;
        this.streamHealthcheckEnabled = streamHealthcheckEnabled;
        this.streamHealthcheckIntervalMs = streamHealthcheckIntervalMs;
        this.streamHealthcheckTimeoutMs = streamHealthcheckTimeoutMs;
        this.streamServerIdleTimeoutMs = streamServerIdleTimeoutMs;
        this.pollIntervalMs = pollIntervalMs;
        this.streamBufferSize = streamBufferSize;

        Properties prod = new Properties();
        prod.putAll(producerConfig);
        prod.putIfAbsent("key.serializer", StringSerializer.class.getName());
        prod.putIfAbsent("value.serializer", ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(prod);

        Properties consBase = new Properties();
        consBase.putAll(consumerConfig);
        consBase.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        consBase.putIfAbsent("value.deserializer", ByteArrayDeserializer.class.getName());
        this.consumerConfigBase = consBase;
        int count = Math.max(1, consumerCount);
        for (int i = 0; i < count; i++) {
            final int index = i;
            Thread t = Thread.ofVirtual().name("kafka-rpc-pool-" + replyTopic + "-" + index).start(() -> runConsumerLoop(index));
            consumerThreads.add(t);
        }
    }

    // For tests: allows injecting mock producer/consumers without Kafka broker.
    PooledKafkaRpcChannel(Producer<String, byte[]> producer,
                          List<Consumer<String, byte[]>> consumers,
                          String requestTopic,
                          String replyTopic,
                          int timeoutMs,
                          int pollIntervalMs) {
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.timeoutMs = timeoutMs;
        this.streamHealthcheckEnabled = true;
        this.streamHealthcheckIntervalMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS;
        this.streamHealthcheckTimeoutMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS;
        this.streamServerIdleTimeoutMs = KafkaRpcConstants.DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS;
        this.pollIntervalMs = pollIntervalMs;
        this.streamBufferSize = KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE;
        this.producer = producer;
        this.consumerConfigBase = new Properties();

        for (int i = 0; i < consumers.size(); i++) {
            Consumer<String, byte[]> consumer = consumers.get(i);
            consumer.subscribe(Collections.singletonList(replyTopic));
            final int index = i;
            Thread t = Thread.ofVirtual().name("kafka-rpc-pool-test-" + replyTopic + "-" + index).start(() -> runConsumer(consumer));
            consumerThreads.add(t);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Properties producerConfig;
        private Properties consumerConfig;
        private String requestTopic;
        private String replyTopic;
        private int timeoutMs = KafkaRpcConstants.DEFAULT_TIMEOUT_MS;
        private boolean streamHealthcheckEnabled = true;
        private int streamHealthcheckIntervalMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS;
        private int streamHealthcheckTimeoutMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS;
        private long streamServerIdleTimeoutMs = KafkaRpcConstants.DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS;
        private int consumerCount = 1;
        private int pollIntervalMs = KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS;
        private int streamBufferSize = KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE;

        public Builder producerConfig(Properties config) { this.producerConfig = config; return this; }
        public Builder consumerConfig(Properties config) { this.consumerConfig = config; return this; }
        public Builder requestTopic(String topic) { this.requestTopic = topic; return this; }
        public Builder replyTopic(String topic) { this.replyTopic = topic; return this; }
        public Builder timeoutMs(int ms) { this.timeoutMs = ms; return this; }
        public Builder streamHealthcheckEnabled(boolean enabled) { this.streamHealthcheckEnabled = enabled; return this; }
        public Builder streamHealthcheckIntervalMs(int ms) { this.streamHealthcheckIntervalMs = ms; return this; }
        public Builder streamHealthcheckTimeoutMs(int ms) { this.streamHealthcheckTimeoutMs = ms; return this; }
        public Builder streamServerIdleTimeoutMs(long ms) { this.streamServerIdleTimeoutMs = ms; return this; }
        public Builder consumerCount(int count) { this.consumerCount = count; return this; }
        public Builder pollIntervalMs(int ms) { this.pollIntervalMs = ms; return this; }
        public Builder streamBufferSize(int size) { this.streamBufferSize = size; return this; }

        public PooledKafkaRpcChannel build() {
            if (producerConfig == null) throw new IllegalStateException("producerConfig is required");
            if (consumerConfig == null) throw new IllegalStateException("consumerConfig is required");
            if (requestTopic == null) throw new IllegalStateException("requestTopic is required");
            if (replyTopic == null) throw new IllegalStateException("replyTopic is required");
            return new PooledKafkaRpcChannel(producerConfig, consumerConfig, requestTopic, replyTopic,
                    timeoutMs, streamHealthcheckEnabled, streamHealthcheckIntervalMs, streamHealthcheckTimeoutMs,
                    streamServerIdleTimeoutMs, consumerCount, pollIntervalMs, streamBufferSize);
        }
    }

    private void runConsumerLoop(int consumerIndex) {
        long backoffMs = CONSUMER_RECOVERY_INITIAL_BACKOFF_MS;
        while (!closed.get()) {
            Consumer<String, byte[]> consumer = null;
            try {
                Properties cons = new Properties();
                cons.putAll(consumerConfigBase);
                consumer = new KafkaConsumer<>(cons);
                consumer.subscribe(Collections.singletonList(replyTopic));
                backoffMs = CONSUMER_RECOVERY_INITIAL_BACKOFF_MS;
                runConsumer(consumer);
            } catch (WakeupException e) {
                if (!closed.get()) {
                    log.warn("Pool consumer {} wakeup for {}, recreating", consumerIndex, replyTopic);
                    sleepRecoveryBackoff(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, CONSUMER_RECOVERY_MAX_BACKOFF_MS);
                }
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("Pool consumer {} failed for {}, recreating in {} ms", consumerIndex, replyTopic, backoffMs, e);
                    sleepRecoveryBackoff(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, CONSUMER_RECOVERY_MAX_BACKOFF_MS);
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
    }

    private void runConsumer(Consumer<String, byte[]> consumer) {
        while (!closed.get()) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
            for (ConsumerRecord<String, byte[]> r : records) {
                String correlationId = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_CORRELATION_ID);
                if (correlationId == null) continue;

                String errorMsg = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_ERROR);
                boolean streamEnd = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_STREAM_END) != null;

                BlockingQueue<StreamChunk> sq = streamQueues.get(correlationId);
                if (sq != null) {
                    if (errorMsg != null) {
                        sq.add(new StreamChunk.Poison(new IOException("Server error: " + errorMsg)));
                        streamQueues.remove(correlationId);
                    } else if (streamEnd) {
                        sq.add(new StreamChunk.End());
                        streamQueues.remove(correlationId);
                    } else {
                        sq.add(new StreamChunk.Data(r.value()));
                    }
                    continue;
                }

                CompletableFuture<byte[]> f = pending.remove(correlationId);
                if (f != null) {
                    if (errorMsg != null) {
                        f.completeExceptionally(new IOException("Server error: " + errorMsg));
                    } else {
                        f.complete(r.value());
                    }
                }
            }
        }
    }

    private void sleepRecoveryBackoff(long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
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
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                    .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
            if (headers != null) {
                headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : new byte[0]));
            }
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(new IOException("Failed to send request", exception));
                }
            });

            return future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
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
        record.headers().add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : new byte[0]));
        }
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IOException("Oneway send failed", e);
        }
    }

    @Override
    public void startStream(String correlationId, byte[] requestBytes, Map<String, String> headers, StreamingProcessor<byte[]> processor) throws IOException {
        BlockingQueue<StreamChunk> queue = new LinkedBlockingQueue<>(streamBufferSize);
        streamQueues.put(correlationId, queue);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
        record.headers().add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaRpcConstants.HEADER_IS_STREAM, "true".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaRpcConstants.HEADER_STREAM_SERVER_IDLE_TIMEOUT_MS, String.valueOf(streamServerIdleTimeoutMs).getBytes(StandardCharsets.UTF_8));
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : new byte[0]));
        }
        producer.send(record);
        String method = headers != null ? headers.get(KafkaRpcConstants.HEADER_METHOD) : "";
        Runnable onClose = () -> streamQueues.remove(correlationId);
        new StreamingCallImpl(correlationId, queue, this, method,
                streamHealthcheckIntervalMs, streamHealthcheckTimeoutMs, streamHealthcheckEnabled, processor, onClose);
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
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : new byte[0]));
        }
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(new IOException("Failed to send request", exception));
            }
        });

        return future.orTimeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                .whenComplete((v, t) -> pending.remove(correlationId));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            producer.close();
            for (Thread t : consumerThreads) {
                t.interrupt();
            }
        }
    }
}
