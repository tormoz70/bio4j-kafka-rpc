package ru.sbrf.uamc.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/** Server that consumes requests from a Kafka topic and dispatches to handlers. Supports multiple consumers (same group) for scaling via partitioning. */
@Slf4j
public class KafkaRpcServer implements AutoCloseable {
    private static final long CONSUMER_READY_TIMEOUT_MS = 30_000L;

    private final List<Consumer<String, byte[]>> consumers;
    private final Producer<String, byte[]> producer;
    private final String requestTopic;
    private final Map<String, MethodHandler> handlers;
    private final Map<String, StreamMethodHandler> streamHandlers;
    private final ConcurrentHashMap<String, StreamContext> activeStreams = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<Thread> consumerThreads = new ArrayList<>();
    private final int pollIntervalMs;
    private final String consumerGroupId;
    private final int maxConcurrentStreams;
    private final boolean manualCommitEnabled;
    /** Offsets to commit on the consumer poll thread (KafkaConsumer is not thread-safe). */
    private final ConcurrentHashMap<Consumer<String, byte[]>, ConcurrentHashMap<TopicPartition, Long>> pendingCommits =
            new ConcurrentHashMap<>();
    private Thread streamIdleThread;
    private final ExecutorService handlerExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @FunctionalInterface
    public interface MethodHandler {
        /**
         * Handles unary RPC request.
         * Returning {@code null} is valid for oneway RPCs (no {@code reply-topic} header).
         * For request-response RPCs, {@code null} with a reply topic is a contract violation.
         */
        byte[] handle(String correlationId, byte[] request);
    }

    @FunctionalInterface
    public interface StreamMethodHandler {
        void handle(String correlationId, byte[] request, StreamSink sink);
    }

    private static class StreamContext {
        final StreamSinkImpl sink;
        final long idleTimeoutMs;
        volatile long lastHealthcheckTime;

        StreamContext(StreamSinkImpl sink, long idleTimeoutMs) {
            this.sink = sink;
            this.idleTimeoutMs = idleTimeoutMs;
            this.lastHealthcheckTime = System.currentTimeMillis();
        }
    }

    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic,
                          Map<String, MethodHandler> handlers) {
        this(consumerConfig, producerConfig, requestTopic, handlers, Map.of(), 1, KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS);
    }

    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic,
                          Map<String, MethodHandler> handlers,
                          Map<String, StreamMethodHandler> streamHandlers) {
        this(consumerConfig, producerConfig, requestTopic, handlers, streamHandlers, 1, KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS);
    }

    /**
     * @param consumerCount number of consumer threads (same consumer group). Use &gt; 1 to scale via topic partitioning.
     */
    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic,
                          Map<String, MethodHandler> handlers,
                          Map<String, StreamMethodHandler> streamHandlers,
                          int consumerCount) {
        this(consumerConfig, producerConfig, requestTopic, handlers, streamHandlers, consumerCount, KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS);
    }

    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic,
                          Map<String, MethodHandler> handlers,
                          Map<String, StreamMethodHandler> streamHandlers,
                          int consumerCount,
                          int pollIntervalMs) {
        this(consumerConfig, producerConfig, requestTopic, handlers, streamHandlers, consumerCount, pollIntervalMs,
                KafkaRpcConstants.DEFAULT_MAX_CONCURRENT_STREAMS);
    }

    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic,
                          Map<String, MethodHandler> handlers,
                          Map<String, StreamMethodHandler> streamHandlers,
                          int consumerCount,
                          int pollIntervalMs,
                          int maxConcurrentStreams) {
        this.requestTopic = requestTopic;
        this.handlers = Map.copyOf(handlers);
        this.streamHandlers = streamHandlers != null ? Map.copyOf(streamHandlers) : Map.of();
        this.pollIntervalMs = pollIntervalMs;
        this.maxConcurrentStreams = Math.max(1, maxConcurrentStreams);
        this.consumerGroupId = consumerConfig.getProperty("group.id", "<undefined>");
        this.manualCommitEnabled = !Boolean.parseBoolean(
                consumerConfig.getProperty(ENABLE_AUTO_COMMIT_CONFIG, "false"));

        Properties consBase = new Properties();
        consBase.putAll(consumerConfig);
        consBase.putIfAbsent(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consBase.putIfAbsent(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        int count = Math.max(1, consumerCount);
        List<Consumer<String, byte[]>> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Properties cons = new Properties();
            cons.putAll(consBase);
            list.add(KafkaRpcConsumerFactory.create(cons));
        }
        this.consumers = Collections.unmodifiableList(list);

        Properties prod = new Properties();
        prod.putAll(producerConfig);
        prod.putIfAbsent(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prod.putIfAbsent(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(prod);
    }

    /** Constructor for testing - inject consumer and producer. */
    KafkaRpcServer(Consumer<String, byte[]> consumer, Producer<String, byte[]> producer,
                   String requestTopic, Map<String, MethodHandler> handlers) {
        this(consumer, producer, requestTopic, handlers, Map.of());
    }

    KafkaRpcServer(Consumer<String, byte[]> consumer, Producer<String, byte[]> producer,
                   String requestTopic, Map<String, MethodHandler> handlers,
                   Map<String, StreamMethodHandler> streamHandlers) {
        this.consumers = Collections.singletonList(consumer);
        this.producer = producer;
        this.requestTopic = requestTopic;
        this.handlers = Map.copyOf(handlers);
        this.streamHandlers = streamHandlers != null ? Map.copyOf(streamHandlers) : Map.of();
        this.pollIntervalMs = KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS;
        this.consumerGroupId = "<test>";
        this.maxConcurrentStreams = KafkaRpcConstants.DEFAULT_MAX_CONCURRENT_STREAMS;
        this.manualCommitEnabled = false;
    }

    public void start() {
        CountDownLatch consumerReadyLatch = new CountDownLatch(consumers.size());
        for (int i = 0; i < consumers.size(); i++) {
            Consumer<String, byte[]> c = consumers.get(i);
            final int index = i;
            AtomicBoolean firstAssignment = new AtomicBoolean(false);
            c.subscribe(Collections.singletonList(requestTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.info("REBALANCE: role=server index={} partitions revoked: {} group={}",
                            index, partitions, consumerGroupId);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("REBALANCE: role=server index={} partitions assigned: {} group={}",
                            index, partitions, consumerGroupId);
                    if (firstAssignment.compareAndSet(false, true)) {
                        consumerReadyLatch.countDown();
                        log.info("{} role=server index={} READY topic={} group={} assigned={}",
                                KafkaRpcLogEvents.CONSUMER_STARTED, index, requestTopic, consumerGroupId, partitions);
                    }
                }
            });
            log.info("{} role=server index={} topic={} group={}",
                    KafkaRpcLogEvents.CONSUMER_STARTED, index, requestTopic, consumerGroupId);
            Thread t = Thread.ofPlatform().name("kafka-rpc-server-" + index).start(() -> run(c));
            consumerThreads.add(t);
        }
        try {
            if (!consumerReadyLatch.await(CONSUMER_READY_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                log.warn("Only {} of {} server consumers became ready in time for topic={}",
                        consumers.size() - consumerReadyLatch.getCount(), consumers.size(), requestTopic);
            } else {
                log.info("All {} server consumers ready for topic={}", consumers.size(), requestTopic);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting server consumers readiness for topic={}", requestTopic);
        }
        streamIdleThread = Thread.ofVirtual().name("kafka-rpc-stream-idle").start(this::runStreamIdleCheck);
        log.info("{} topic={} group={} consumerCount={} handlers={} streamHandlers={} maxConcurrentStreams={} manualCommit={}",
                KafkaRpcLogEvents.SERVER_STARTED, requestTopic, consumerGroupId, consumers.size(), handlers, streamHandlers,
                maxConcurrentStreams, manualCommitEnabled);
    }

    private void runStreamIdleCheck() {
        while (running.get()) {
            try {
                Thread.sleep(2000);
                long now = System.currentTimeMillis();
                activeStreams.forEach((streamId, ctx) -> {
                    if (now - ctx.lastHealthcheckTime >= ctx.idleTimeoutMs) {
                        log.info("{} streamId={} action=cancel", KafkaRpcLogEvents.STREAM_IDLE_TIMEOUT, streamId);
                        ctx.sink.cancel();
                        activeStreams.remove(streamId, ctx);
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void stop() {
        running.set(false);
        for (Consumer<String, byte[]> c : consumers) {
            c.wakeup();
        }
        if (streamIdleThread != null) {
            streamIdleThread.interrupt();
        }
        activeStreams.forEach((id, ctx) -> ctx.sink.cancel());
        activeStreams.clear();
        handlerExecutor.shutdown();
        try {
            if (!handlerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                handlerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            handlerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        for (Thread t : consumerThreads) {
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run(Consumer<String, byte[]> consumer) {
        while (running.get()) {
            try {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
                for (ConsumerRecord<String, byte[]> record : records) {
                    handlerExecutor.submit(() -> processRecord(consumer, record));
                }
                flushPendingCommits(consumer);
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                flushPendingCommits(consumer);
                break;
            } catch (Exception e) {
                log.error("{} topic={}", KafkaRpcLogEvents.REQUEST_PROCESSING_FAILED, requestTopic, e);
            }
        }
        flushPendingCommits(consumer);
    }

    private void processRecord(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> record) {
        if (log.isDebugEnabled()) {
            log.debug("{} role=server topic={} key={} headers={} payload={}",
                    KafkaRpcLogEvents.RECEIVE,
                    requestTopic,
                    record.key(),
                    KafkaRpcConstants.headersToDebugString(record.headers()),
                    KafkaRpcConstants.payloadToDebugString(record.value()));
        }
        String correlationId = KafkaRpcConstants.getHeader(record, KafkaRpcConstants.HEADER_CORRELATION_ID);
        String method = KafkaRpcConstants.getHeader(record, KafkaRpcConstants.HEADER_METHOD);
        String replyTopic = KafkaRpcConstants.getHeader(record, KafkaRpcConstants.HEADER_REPLY_TOPIC);
        String isStream = KafkaRpcConstants.getHeader(record, KafkaRpcConstants.HEADER_IS_STREAM);

        if (correlationId == null) {
            log.warn("{} reason=missing-correlation-id topic={}", KafkaRpcLogEvents.REQUEST_DROPPED, requestTopic);
            scheduleCommit(consumer, record);
            return;
        }

        if (record.value() == null) {
            log.warn("{} reason=null-body topic={} correlationId={}",
                    KafkaRpcLogEvents.REQUEST_DROPPED, requestTopic, correlationId);
            scheduleCommit(consumer, record);
            return;
        }

        if (method != null && method.endsWith(KafkaRpcConstants.STREAM_HEALTHCHECK_SUFFIX)) {
            handleHealthcheck(consumer, record, correlationId, method, replyTopic);
            return;
        }

        if ("true".equals(isStream) && method != null && streamHandlers.containsKey(method)) {
            handleStreamRequest(consumer, record, correlationId, method, replyTopic);
            return;
        }

        MethodHandler handler = method != null ? handlers.get(method) : null;
        if (handler == null) {
            if (log.isDebugEnabled()) {
                log.warn("{} reason=no-handler topic={} method={} correlationId={}",
                        KafkaRpcLogEvents.REQUEST_DROPPED, requestTopic, method, correlationId);
            }
            scheduleCommit(consumer, record);
            return;
        }

        handleUnaryRequest(consumer, record, correlationId, method, replyTopic, handler);
    }

    private void handleHealthcheck(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> record,
                                   String correlationId, String method, String replyTopic) {
        String streamId = KafkaRpcConstants.getHeader(record, KafkaRpcConstants.HEADER_STREAM_ID);
        if (streamId == null) {
            scheduleCommit(consumer, record);
            return;
        }

        StreamContext ctx = activeStreams.get(streamId);
        if (ctx != null) {
            ctx.lastHealthcheckTime = System.currentTimeMillis();
        }

        if (replyTopic == null || replyTopic.isEmpty()) {
            scheduleCommit(consumer, record);
            return;
        }

        ProducerRecord<String, byte[]> reply = new ProducerRecord<>(replyTopic, record.key(), new byte[0]);
        reply.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_METHOD, method.getBytes(StandardCharsets.UTF_8));
        if (ctx == null) {
            reply.headers().add(KafkaRpcConstants.HEADER_STREAM_NOT_FOUND, "true".getBytes(StandardCharsets.UTF_8));
        }
        if (log.isDebugEnabled()) {
            log.debug("{} role=server kind=healthcheck-reply topic={} key={} headers={} payload={}",
                    KafkaRpcLogEvents.SEND,
                    replyTopic,
                    record.key(),
                    KafkaRpcConstants.headersToDebugString(reply.headers()),
                    KafkaRpcConstants.payloadToDebugString(reply.value()));
        }
        sendAndCommit(consumer, record, reply, () -> { }, (metadata, exception) -> {
            if (exception != null) {
                log.warn("{} role=server kind=healthcheck-reply topic={} correlationId={}",
                        KafkaRpcLogEvents.SEND_FAILED, replyTopic, correlationId, exception);
            }
        });
    }

    private void handleStreamRequest(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> record,
                                     String correlationId, String method, String replyTopic) {
        StreamMethodHandler streamHandler = streamHandlers.get(method);
        if (replyTopic == null || replyTopic.isEmpty()) {
            log.warn("{} reason=missing-reply-topic type=stream topic={} correlationId={}",
                    KafkaRpcLogEvents.REQUEST_DROPPED, requestTopic, correlationId);
            scheduleCommit(consumer, record);
            return;
        }
        Long idleTimeoutMs = parseStreamServerIdleTimeoutMs(record);
        if (idleTimeoutMs == null) {
            log.warn("{} reason=invalid-header type=stream topic={} correlationId={} header={}",
                    KafkaRpcLogEvents.REQUEST_DROPPED, requestTopic, correlationId, KafkaRpcConstants.HEADER_STREAM_SERVER_IDLE_TIMEOUT_MS);
            scheduleCommit(consumer, record);
            return;
        }
        if (activeStreams.size() >= maxConcurrentStreams && !activeStreams.containsKey(correlationId)) {
            log.warn("{} topic={} correlationId={} active={} limit={}",
                    KafkaRpcLogEvents.STREAM_REJECTED, requestTopic, correlationId, activeStreams.size(), maxConcurrentStreams);
            sendErrorReply(consumer, record, replyTopic, record.key(), correlationId, method,
                    "Too many concurrent streams", null);
            return;
        }

        boolean streamOrdered = parseStreamOrdered(record);
        StreamSinkImpl sink = new StreamSinkImpl(producer, replyTopic, correlationId, method, streamOrdered);
        activeStreams.put(correlationId, new StreamContext(sink, idleTimeoutMs));
        byte[] request = record.value();
        scheduleCommit(consumer, record);
        handlerExecutor.submit(() -> {
            try {
                streamHandler.handle(correlationId, request, sink);
                if (!sink.isCancelled()) {
                    sink.end();
                }
            } catch (Exception e) {
                log.error("{} correlationId={} method={}", KafkaRpcLogEvents.STREAM_HANDLER_FAILED, correlationId, method, e);
                sink.cancel();
            } finally {
                activeStreams.remove(correlationId);
            }
        });
    }

    private void handleUnaryRequest(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> record,
                                    String correlationId, String method, String replyTopic, MethodHandler handler) {
        try {
            byte[] response = handler.handle(correlationId, record.value());
            if (response == null) {
                if (replyTopic == null || replyTopic.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} correlationId={} method={} topic={} reason=oneway-completed",
                                KafkaRpcLogEvents.RECEIVE, correlationId, method, requestTopic);
                    }
                    scheduleCommit(consumer, record);
                    return;
                }
                log.warn("{} correlationId={} method={} topic={} replyTopic={} reason=handler-returned-null",
                        KafkaRpcLogEvents.HANDLER_CONTRACT_VIOLATION, correlationId, method, requestTopic, replyTopic);
                sendErrorReply(consumer, record, replyTopic, record.key(), correlationId, method,
                        "Handler returned null response", null);
                return;
            }
            if (replyTopic == null || replyTopic.isEmpty()) {
                log.warn("{} reason=missing-reply-topic correlationId={} topic={}",
                        KafkaRpcLogEvents.RESPONSE_DROPPED, correlationId, requestTopic);
                scheduleCommit(consumer, record);
                return;
            }
            ProducerRecord<String, byte[]> reply = new ProducerRecord<>(replyTopic, record.key(), response);
            reply.headers()
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                    .add(KafkaRpcConstants.HEADER_METHOD, (method != null ? method : "").getBytes(StandardCharsets.UTF_8));
            if (log.isDebugEnabled()) {
                log.debug("{} role=server kind=reply topic={} key={} headers={} payload={}",
                        KafkaRpcLogEvents.SEND,
                        replyTopic,
                        record.key(),
                        KafkaRpcConstants.headersToDebugString(reply.headers()),
                        KafkaRpcConstants.payloadToDebugString(reply.value()));
            }
            sendAndCommit(consumer, record, reply, () -> { }, (metadata, exception) -> {
                if (exception != null) {
                    log.error("{} role=server kind=reply topic={} correlationId={}",
                            KafkaRpcLogEvents.REPLY_SEND_FAILED, replyTopic, correlationId, exception);
                }
            });
        } catch (Exception e) {
            log.error("{} correlationId={} method={}", KafkaRpcLogEvents.HANDLER_FAILED, correlationId, method, e);
            if (replyTopic != null && !replyTopic.isEmpty()) {
                sendErrorReply(consumer, record, replyTopic, record.key(), correlationId, method, e);
            } else {
                scheduleCommit(consumer, record);
            }
        }
    }

    private void sendErrorReply(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> requestRecord,
                                String replyTopic, String key, String correlationId, String method, Exception error) {
        sendErrorReply(consumer, requestRecord, replyTopic, key, correlationId, method, "Internal server error", error);
    }

    private void sendErrorReply(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> requestRecord,
                                String replyTopic, String key, String correlationId, String method,
                                String errorMessage, Exception error) {
        try {
            if (error != null) {
                log.debug("{} correlationId={} method={} cause={}",
                        KafkaRpcLogEvents.HANDLER_FAILED, correlationId, method, error.toString());
            }
            ProducerRecord<String, byte[]> reply = new ProducerRecord<>(replyTopic, key, new byte[0]);
            reply.headers()
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                    .add(KafkaRpcConstants.HEADER_METHOD, (method != null ? method : "").getBytes(StandardCharsets.UTF_8))
                    .add(KafkaRpcConstants.HEADER_ERROR, errorMessage.getBytes(StandardCharsets.UTF_8));
            if (log.isDebugEnabled()) {
                log.debug("{} role=server kind=error-reply topic={} key={} headers={} payload={}",
                        KafkaRpcLogEvents.SEND,
                        replyTopic,
                        key,
                        KafkaRpcConstants.headersToDebugString(reply.headers()),
                        KafkaRpcConstants.payloadToDebugString(reply.value()));
            }
            sendAndCommit(consumer, requestRecord, reply, () -> { }, (metadata, exception) -> {
                if (exception != null) {
                    log.error("{} role=server kind=error-reply topic={} correlationId={}",
                            KafkaRpcLogEvents.ERROR_REPLY_SEND_FAILED, replyTopic, correlationId, exception);
                }
            });
        } catch (Exception e) {
            log.error("{} correlationId={} method={} topic={}",
                    KafkaRpcLogEvents.ERROR_REPLY_PREPARE_FAILED, correlationId, method, requestTopic, e);
        }
    }

    private void sendAndCommit(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> requestRecord,
                               ProducerRecord<String, byte[]> reply, Runnable onSuccess,
                               org.apache.kafka.clients.producer.Callback callback) {
        producer.send(reply, (metadata, exception) -> {
            callback.onCompletion(metadata, exception);
            if (exception == null) {
                onSuccess.run();
                scheduleCommit(consumer, requestRecord);
            }
        });
    }

    private void scheduleCommit(Consumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> record) {
        if (!manualCommitEnabled) {
            return;
        }
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        long nextOffset = record.offset() + 1;
        pendingCommits
                .computeIfAbsent(consumer, c -> new ConcurrentHashMap<>())
                .merge(tp, nextOffset, Math::max);
    }

    private void flushPendingCommits(Consumer<String, byte[]> consumer) {
        if (!manualCommitEnabled) {
            return;
        }
        ConcurrentHashMap<TopicPartition, Long> offsets = pendingCommits.get(consumer);
        if (offsets == null || offsets.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        offsets.forEach((tp, offset) -> toCommit.put(tp, new OffsetAndMetadata(offset)));
        offsets.clear();
        try {
            consumer.commitSync(toCommit);
        } catch (Exception e) {
            log.error("{} topic={} partitions={}",
                    KafkaRpcLogEvents.OFFSET_COMMIT_FAILED, requestTopic, toCommit.keySet(), e);
            offsets.forEach((tp, offset) -> pendingCommits
                    .computeIfAbsent(consumer, c -> new ConcurrentHashMap<>())
                    .merge(tp, offset, Math::max));
        }
    }

    private static Long parseStreamServerIdleTimeoutMs(ConsumerRecord<String, byte[]> record) {
        String v = KafkaRpcConstants.getHeader(record, KafkaRpcConstants.HEADER_STREAM_SERVER_IDLE_TIMEOUT_MS);
        if (v == null || v.isEmpty()) {
            return null;
        }
        try {
            long ms = Long.parseLong(v.trim());
            return ms > 0 ? ms : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean parseStreamOrdered(ConsumerRecord<String, byte[]> record) {
        String v = KafkaRpcConstants.getHeader(record, KafkaRpcConstants.HEADER_STREAM_ORDERED);
        return v == null || !"false".equalsIgnoreCase(v.trim());
    }

    @Override
    public void close() {
        stop();
        for (Consumer<String, byte[]> c : consumers) {
            c.close();
        }
        producer.close();
    }
}
