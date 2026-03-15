package io.bio4j.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Server that consumes requests from a Kafka topic and dispatches to handlers. Supports multiple consumers (same group) for scaling via partitioning. */
@Slf4j
public class KafkaRpcServer implements AutoCloseable {

    private final List<Consumer<String, byte[]>> consumers;
    private final Producer<String, byte[]> producer;
    private final String requestTopic;
    private final Map<String, MethodHandler> handlers;
    private final Map<String, StreamMethodHandler> streamHandlers;
    private final ConcurrentHashMap<String, StreamContext> activeStreams = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<Thread> consumerThreads = new ArrayList<>();
    private Thread streamIdleThread;
    private final ExecutorService streamExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "kafka-rpc-stream-handler");
        t.setDaemon(true);
        return t;
    });

    @FunctionalInterface
    public interface MethodHandler {
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
        this(consumerConfig, producerConfig, requestTopic, handlers, Map.of(), 1);
    }

    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic,
                          Map<String, MethodHandler> handlers,
                          Map<String, StreamMethodHandler> streamHandlers) {
        this(consumerConfig, producerConfig, requestTopic, handlers, streamHandlers, 1);
    }

    /**
     * @param consumerCount number of consumer threads (same consumer group). Use &gt; 1 to scale via topic partitioning.
     */
    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic,
                          Map<String, MethodHandler> handlers,
                          Map<String, StreamMethodHandler> streamHandlers,
                          int consumerCount) {
        this.requestTopic = requestTopic;
        this.handlers = Map.copyOf(handlers);
        this.streamHandlers = streamHandlers != null ? Map.copyOf(streamHandlers) : Map.of();

        Properties consBase = new Properties();
        consBase.putAll(consumerConfig);
        consBase.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        consBase.putIfAbsent("value.deserializer", ByteArrayDeserializer.class.getName());
        int count = Math.max(1, consumerCount);
        List<Consumer<String, byte[]>> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Properties cons = new Properties();
            cons.putAll(consBase);
            list.add(new KafkaConsumer<>(cons));
        }
        this.consumers = Collections.unmodifiableList(list);

        Properties prod = new Properties();
        prod.putAll(producerConfig);
        prod.putIfAbsent("key.serializer", StringSerializer.class.getName());
        prod.putIfAbsent("value.serializer", ByteArraySerializer.class.getName());
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
    }

    public void start() {
        for (int i = 0; i < consumers.size(); i++) {
            Consumer<String, byte[]> c = consumers.get(i);
            c.subscribe(Collections.singletonList(requestTopic));
            final int index = i;
            Thread t = Thread.ofVirtual().name("kafka-rpc-server-" + index).start(() -> run(c));
            consumerThreads.add(t);
        }
        streamIdleThread = Thread.ofVirtual().name("kafka-rpc-stream-idle").start(this::runStreamIdleCheck);
        log.info("Kafka RPC server started, requestTopic={}, consumerCount={}", requestTopic, consumers.size());
    }

    private void runStreamIdleCheck() {
        while (running.get()) {
            try {
                Thread.sleep(2000);
                long now = System.currentTimeMillis();
                activeStreams.forEach((streamId, ctx) -> {
                    if (now - ctx.lastHealthcheckTime >= ctx.idleTimeoutMs) {
                        log.info("Stream {} idle timeout, cancelling", streamId);
                        ctx.sink.cancel();
                        activeStreams.remove(streamId);
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
        streamExecutor.shutdown();
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
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> record : records) {
                    processRecord(record);
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // expected on shutdown
                break;
            } catch (Exception e) {
                log.error("Error processing request", e);
            }
        }
    }

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        String correlationId = getHeader(record, KafkaRpcConstants.HEADER_CORRELATION_ID);
        String method = getHeader(record, KafkaRpcConstants.HEADER_METHOD);
        String replyTopic = getHeader(record, KafkaRpcConstants.HEADER_REPLY_TOPIC);
        String isStream = getHeader(record, KafkaRpcConstants.HEADER_IS_STREAM);

        if (correlationId == null) {
            log.warn("Dropping message without correlation ID");
            return;
        }

        if (method != null && method.endsWith(KafkaRpcConstants.STREAM_HEALTHCHECK_SUFFIX)) {
            String streamId = getHeader(record, KafkaRpcConstants.HEADER_STREAM_ID);
            if (streamId != null) {
                StreamContext ctx = activeStreams.get(streamId);
                if (ctx != null) {
                    ctx.lastHealthcheckTime = System.currentTimeMillis();
                    if (replyTopic != null && !replyTopic.isEmpty()) {
                        ProducerRecord<String, byte[]> reply = new ProducerRecord<>(replyTopic, record.key(), new byte[0]);
                        reply.headers()
                                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                                .add(KafkaRpcConstants.HEADER_METHOD, (method != null ? method : "").getBytes());
                        producer.send(reply);
                    }
                }
            }
            return;
        }

        if ("true".equals(isStream) && method != null && streamHandlers.containsKey(method)) {
            StreamMethodHandler streamHandler = streamHandlers.get(method);
            if (replyTopic == null || replyTopic.isEmpty()) {
                log.warn("Dropping stream request: missing reply-topic");
                return;
            }
            Long idleTimeoutMs = parseStreamServerIdleTimeoutMs(record);
            if (idleTimeoutMs == null) {
                log.warn("Dropping stream request correlationId={}: missing or invalid required header {}", correlationId, KafkaRpcConstants.HEADER_STREAM_SERVER_IDLE_TIMEOUT_MS);
                return;
            }
            StreamSinkImpl sink = new StreamSinkImpl(producer, replyTopic, correlationId, method);
            activeStreams.put(correlationId, new StreamContext(sink, idleTimeoutMs));
            byte[] request = record.value();
            streamExecutor.submit(() -> {
                try {
                    streamHandler.handle(correlationId, request, sink);
                    sink.end();
                } catch (Exception e) {
                    log.error("Stream handler error for correlationId={}", correlationId, e);
                    sink.cancel();
                } finally {
                    activeStreams.remove(correlationId);
                }
            });
            return;
        }

        MethodHandler handler = method != null ? handlers.get(method) : null;
        if (handler == null) {
            if (handlers.size() == 1) {
                handler = handlers.values().iterator().next();
            } else {
                log.warn("No handler for method: {}", method);
                return;
            }
        }

        try {
            byte[] response = handler.handle(correlationId, record.value());
            if (response == null) {
                return;
            }
            if (replyTopic == null || replyTopic.isEmpty()) {
                log.warn("Dropping response for correlationId={}: missing reply-topic header from client", correlationId);
                return;
            }
            ProducerRecord<String, byte[]> reply = new ProducerRecord<>(replyTopic, record.key(), response);
            reply.headers()
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                    .add(KafkaRpcConstants.HEADER_METHOD, (method != null ? method : "").getBytes());
            producer.send(reply);
        } catch (Exception e) {
            log.error("Handler error for correlationId={}", correlationId, e);
        }
    }

    private static String getHeader(ConsumerRecord<String, byte[]> record, String name) {
        var iter = record.headers().headers(name).iterator();
        if (iter.hasNext()) {
            byte[] v = iter.next().value();
            return v != null && v.length > 0 ? new String(v) : null;
        }
        return null;
    }

    /** Parses required stream idle timeout header. Returns null if missing or invalid (server requires this header). */
    private static Long parseStreamServerIdleTimeoutMs(ConsumerRecord<String, byte[]> record) {
        String v = getHeader(record, KafkaRpcConstants.HEADER_STREAM_SERVER_IDLE_TIMEOUT_MS);
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

    @Override
    public void close() {
        stop();
        for (Consumer<String, byte[]> c : consumers) {
            c.close();
        }
        producer.close();
    }
}
