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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Shared channel: one producer + one or more consumer threads per instance (same consumer group for reply topic).
 * Dispatches replies by correlationId to waiting callers. Used by the channel pool (one per client name).
 * Multiple consumers enable scaling via partitioning of the reply topic.
 */
@Slf4j
public class PooledKafkaRpcChannel implements KafkaRpcChannel {
    private static final long CONSUMER_RECOVERY_INITIAL_BACKOFF_MS = 1_000L;
    private static final long CONSUMER_RECOVERY_MAX_BACKOFF_MS = 30_000L;
    private static final long CONSUMER_READY_TIMEOUT_MS = 30_000L;

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
    private final String consumerGroupId;
    private final ConcurrentHashMap<String, CompletableFuture<byte[]>> pending = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<StreamChunk>> streamQueues = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<Thread> consumerThreads = new ArrayList<>();
    private final Properties consumerConfigBase;
    private final ConcurrentHashMap<String, Long> streamLastActivity = new ConcurrentHashMap<>();
    private final CountDownLatch consumerReadyLatch;

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs) throws InterruptedException {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs, true,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS,
                KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS,
                KafkaRpcConstants.DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS, 1,
                KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS, KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE);
    }

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs,
                                 boolean streamHealthcheckEnabled) throws InterruptedException {
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
                                 long streamServerIdleTimeoutMs) throws InterruptedException {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs, streamHealthcheckEnabled,
                streamHealthcheckIntervalMs, streamHealthcheckTimeoutMs, streamServerIdleTimeoutMs, 1,
                KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS, KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE);
    }

    public PooledKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                 String requestTopic, String replyTopic, int timeoutMs,
                                 boolean streamHealthcheckEnabled,
                                 int streamHealthcheckIntervalMs, int streamHealthcheckTimeoutMs,
                                 long streamServerIdleTimeoutMs,
                                 int consumerCount) throws InterruptedException {
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
                                 int pollIntervalMs, int streamBufferSize) throws InterruptedException {
        this.cleanupScheduler.scheduleAtFixedRate(this::cleanupStaleEntries, 30, 30, TimeUnit.SECONDS);
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
        prod.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prod.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(prod);

        Properties consBase = new Properties();
        consBase.putAll(consumerConfig);
        consBase.putIfAbsent(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consBase.putIfAbsent(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumerGroupId = consBase.getProperty("group.id", "<undefined>");
        this.consumerConfigBase = consBase;

        int count = Math.max(1, consumerCount);
        this.consumerReadyLatch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            final int index = i;
            Thread t = Thread.ofVirtual()
                    .name("kafka-rpc-pool-" + replyTopic + "-" + index)
                    .start(() -> runConsumerLoop(index));
            consumerThreads.add(t);
        }

        // Ждём готовности консьюмеров (с таймаутом, чтобы не зависнуть навсегда)
        if (!consumerReadyLatch.await(CONSUMER_READY_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            log.warn("Only {} of {} consumers became ready in time for topic={}",
                    count - consumerReadyLatch.getCount(), count, replyTopic);
        } else {
            log.info("All {} consumers ready for topic={}", count, replyTopic);
        }
    }

    // For tests: allows injecting mock producer/consumers without Kafka broker.
    PooledKafkaRpcChannel(Producer<String, byte[]> producer,
                          List<Consumer<String, byte[]>> consumers,
                          String requestTopic,
                          String replyTopic,
                          int timeoutMs,
                          int pollIntervalMs) {
        this.cleanupScheduler.scheduleAtFixedRate(this::cleanupStaleEntries, 30, 30, TimeUnit.SECONDS);
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
        this.consumerGroupId = "<test>";

        // Для тестов: консьюмеры уже подписаны, сразу считаем их готовыми
        this.consumerReadyLatch = new CountDownLatch(consumers.size());

        for (int i = 0; i < consumers.size(); i++) {
            Consumer<String, byte[]> consumer = consumers.get(i);
            consumer.subscribe(Collections.singletonList(replyTopic));
            final int index = i;
            Thread t = Thread.ofVirtual()
                    .name("kafka-rpc-pool-test-" + replyTopic + "-" + index)
                    .start(() -> runConsumer(consumer));
            consumerThreads.add(t);
            // Сразу сигналим о готовности для тестового режима
            consumerReadyLatch.countDown();
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

        public PooledKafkaRpcChannel build() throws InterruptedException {
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
        log.debug("{} --- runConsumerLoop started", KafkaRpcLogEvents.CONSUMER_STARTING);
        long backoffMs = CONSUMER_RECOVERY_INITIAL_BACKOFF_MS;
        boolean isFirstStart = true;

        while (!closed.get()) {
            Consumer<String, byte[]> consumer = null;
            try {
                Properties cons = new Properties();
                cons.putAll(consumerConfigBase);
                consumer = new KafkaConsumer<>(cons);
                consumer.subscribe(Collections.singletonList(replyTopic));

                // Сигнал готовности ТОЛЬКО при первой успешной подписке
                if (isFirstStart && consumerReadyLatch != null) {
                    consumerReadyLatch.countDown();
                    isFirstStart = false;
                    log.info("{} role=client index={} READY topic={} group={}",
                            KafkaRpcLogEvents.CONSUMER_STARTED, consumerIndex, replyTopic, consumerGroupId);
                }

                log.info("{} role=client index={} topic={} group={}",
                        KafkaRpcLogEvents.CONSUMER_STARTED, consumerIndex, replyTopic, consumerGroupId);
                backoffMs = CONSUMER_RECOVERY_INITIAL_BACKOFF_MS;
                runConsumer(consumer);
            } catch (WakeupException e) {
                log.debug("%s --- WakeupException catch!".formatted(KafkaRpcLogEvents.RECEIVE), e);
                if (!closed.get()) {
                    log.warn("{} role=client reason=wakeup index={} topic={} group={}",
                            KafkaRpcLogEvents.CONSUMER_RECREATING, consumerIndex, replyTopic, consumerGroupId);
                    sleepRecoveryBackoff(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, CONSUMER_RECOVERY_MAX_BACKOFF_MS);
                }
            } catch (Exception e) {
                log.debug("%s --- Exception catch!".formatted(KafkaRpcLogEvents.RECEIVE), e);
                if (!closed.get()) {
                    log.warn("{} role=client reason=failure index={} topic={} group={} backoffMs={}",
                            KafkaRpcLogEvents.CONSUMER_RECREATING, consumerIndex, replyTopic, consumerGroupId, backoffMs, e);
                    sleepRecoveryBackoff(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, CONSUMER_RECOVERY_MAX_BACKOFF_MS);
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                    log.debug("{} --- consumer closed!", KafkaRpcLogEvents.CONSUMER_STARTING);
                }
            }
        }
        log.debug("{} --- runConsumerLoop exited", KafkaRpcLogEvents.CONSUMER_STARTING);
    }

    private void runConsumer(Consumer<String, byte[]> consumer) {
        log.debug("{} --- runConsumer started", KafkaRpcLogEvents.RECEIVE);
        while (!closed.get()) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
            log.trace("{} --- polled records: {}", KafkaRpcLogEvents.RECEIVE, records.count());
            for (ConsumerRecord<String, byte[]> r : records) {
                try {
                    String correlationId = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_CORRELATION_ID);
                    if (log.isDebugEnabled()) {
                        log.debug("{} role=client topic={} key={} correlationId={} headers={} payload={}",
                                KafkaRpcLogEvents.RECEIVE,
                                replyTopic,
                                r.key(),
                                correlationId,
                                KafkaRpcConstants.headersToDebugString(r.headers()),
                                KafkaRpcConstants.payloadToDebugString(r.value()));
                    }
                    if (correlationId == null) {
                        log.trace("{} --- correlationId is null! continue...", KafkaRpcLogEvents.RECEIVE);
                        continue;
                    }

                    String errorMsg = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_ERROR);
                    boolean streamEnd = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_STREAM_END) != null;

                    BlockingQueue<StreamChunk> sq = streamQueues.get(correlationId);
                    if (sq != null) {
                        streamLastActivity.put(correlationId, System.currentTimeMillis());
                        if (errorMsg != null) {
                            log.debug("{} --- try processing1 error: {}", KafkaRpcLogEvents.RECEIVE, errorMsg);
                            sq.offer(new StreamChunk.Poison(new IOException("Server error: " + errorMsg)));
                            streamQueues.remove(correlationId);
                            streamLastActivity.remove(correlationId);
                        } else if (streamEnd) {
                            log.debug("{} --- try processing1 streamEnd1!", KafkaRpcLogEvents.RECEIVE);
                            sq.offer(new StreamChunk.End());
                            streamQueues.remove(correlationId);
                            streamLastActivity.remove(correlationId);
                        } else {
                            log.debug("{} --- try processing1 value: {}", KafkaRpcLogEvents.RECEIVE, r.value());
                            if (!sq.offer(new StreamChunk.Data(r.value()), 100, TimeUnit.MILLISECONDS)) {
                                log.warn("{} role=client stream queue timeout, correlationId={}", KafkaRpcLogEvents.RECEIVE, correlationId);
                                sq.offer(new StreamChunk.Poison(new IOException("Stream delivery timeout")));
                                streamQueues.remove(correlationId);
                            }
                        }
                        continue;
                    }

                    CompletableFuture<byte[]> f = pending.remove(correlationId);
                    if (f != null) {
                        pendingTimestamps.remove(correlationId); // Очистка метки времени
                        if (!f.isDone()) { // Защита от гонки
                            if (errorMsg != null) {
                                log.debug("{} --- try processing2 error: {}", KafkaRpcLogEvents.RECEIVE, errorMsg);
                                f.completeExceptionally(new IOException("Server error: " + errorMsg));
                            } else {
                                log.debug("{} --- try processing2 value: {}", KafkaRpcLogEvents.RECEIVE, r.value());
                                f.complete(r.value());
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("Unexpected exception in runConsumer! Lets continue...".formatted(KafkaRpcLogEvents.RECEIVE), e);
                }
            }
        }
        log.debug("{} --- runConsumer exited", KafkaRpcLogEvents.RECEIVE);
    }

    private final ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "kafka-rpc-cleanup");
        t.setDaemon(true);
        return t;
    });
    private final ConcurrentHashMap<String, Long> pendingTimestamps = new ConcurrentHashMap<>();

    private void cleanupStaleEntries() {
        long now = System.currentTimeMillis();
        pending.forEach((id, future) -> {
            if (future.isDone()) {
                pendingTimestamps.remove(id);
            } else {
                Long ts = pendingTimestamps.get(id);
                if (ts != null && now - ts > timeoutMs * 2L) {
                    log.warn("{} Cleaning up stale pending request: {}", KafkaRpcLogEvents.CHANNEL_CLEANUP, id);
                    pending.remove(id, future);
                    future.completeExceptionally(new TimeoutException("Stale request cleanup"));
                }
            }
        });

        streamQueues.forEach((id, q) -> {
            Long last = streamLastActivity.get(id);
            // Если стрим неактивен дольше, чем 2 * serverIdleTimeout, считаем его мёртвым
            if (last != null && System.currentTimeMillis() - last > streamServerIdleTimeoutMs * 2) {
                log.warn("{} Cleaning up stale stream: {}", KafkaRpcLogEvents.CHANNEL_CLEANUP, id);
                q.offer(new StreamChunk.Poison(new TimeoutException("Stream idle timeout")));
                streamQueues.remove(id);
                streamLastActivity.remove(id);
            }
        });
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
        pendingTimestamps.put(correlationId, System.currentTimeMillis());

        try {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
            record.headers()
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                    .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
            if (headers != null) {
                headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : new byte[0]));
            }
            if (log.isDebugEnabled()) {
                log.debug("{} role=client kind=request topic={} key={} headers={} payload={}",
                        KafkaRpcLogEvents.SEND,
                        requestTopic,
                        correlationId,
                        KafkaRpcConstants.headersToDebugString(record.headers()),
                        KafkaRpcConstants.payloadToDebugString(requestBytes));
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
            throw new TimeoutException("No response within " + timeoutMs + " ms for correlationId=" + correlationId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } finally {
            pending.remove(correlationId);
            pendingTimestamps.remove(correlationId);
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
        if (log.isDebugEnabled()) {
            log.debug("{} role=client kind=oneway topic={} key={} headers={} payload={}",
                    KafkaRpcLogEvents.SEND,
                    requestTopic,
                    correlationId,
                    KafkaRpcConstants.headersToDebugString(record.headers()),
                    KafkaRpcConstants.payloadToDebugString(requestBytes));
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
        if (log.isDebugEnabled()) {
            log.debug("{} role=client kind=stream-request topic={} key={} headers={} payload={}",
                    KafkaRpcLogEvents.SEND,
                    requestTopic,
                    correlationId,
                    KafkaRpcConstants.headersToDebugString(record.headers()),
                    KafkaRpcConstants.payloadToDebugString(requestBytes));
        }
        try {
            producer.send(record).get();
        } catch (Exception e) {
            streamQueues.remove(correlationId);
            throw new IOException("Failed to send stream request", e);
        }
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
        pendingTimestamps.put(correlationId, System.currentTimeMillis());

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : new byte[0]));
        }
        if (log.isDebugEnabled()) {
            log.debug("{} role=client kind=async-request topic={} key={} headers={} payload={}",
                    KafkaRpcLogEvents.SEND,
                    requestTopic,
                    correlationId,
                    KafkaRpcConstants.headersToDebugString(record.headers()),
                    KafkaRpcConstants.payloadToDebugString(requestBytes));
        }
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(new IOException("Failed to send request", exception));
            }
        });

        return future.orTimeout(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                .whenComplete((v, t) -> {
                    pending.remove(correlationId);
                    pendingTimestamps.remove(correlationId);
                });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.debug("{} --- start close...", KafkaRpcLogEvents.CONSUMER_CLOSING);

            // Закрываем все стримы перед очисткой
            streamQueues.keySet().forEach(this::closeStream);

            // Завершаем все ожидающие запросы
            pending.forEach((id, f) -> {
                if (!f.isDone()) {
                    f.completeExceptionally(new IOException("Channel closed"));
                }
            });
            pending.clear();
            pendingTimestamps.clear();

            for (Thread t : consumerThreads) {
                t.interrupt();
            }
            // Ждём завершения с таймаутом
            long deadline = System.currentTimeMillis() + 2000; // 2 секунды
            for (Thread t : consumerThreads) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining > 0) {
                    try {
                        t.join(remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            cleanupScheduler.shutdownNow();
            producer.close();

            log.debug("{} --- closed", KafkaRpcLogEvents.CONSUMER_CLOSING);
        }
    }

    @Override
    public void closeStream(String correlationId) {
        BlockingQueue<StreamChunk> queue = streamQueues.get(correlationId);
        if (queue != null) {
            queue.offer(new StreamChunk.Poison(new IOException("Stream closed by channel")));
            streamLastActivity.remove(correlationId);
        }
    }
}