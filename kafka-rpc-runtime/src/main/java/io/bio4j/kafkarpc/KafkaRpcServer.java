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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** Server that consumes requests from a Kafka topic and dispatches to handlers. */
@Slf4j
public class KafkaRpcServer implements AutoCloseable {

    private final Consumer<String, byte[]> consumer;
    private final Producer<String, byte[]> producer;
    private final String requestTopic;
    private final String replyTopic;
    private final Map<String, MethodHandler> handlers;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread consumerThread;

    @FunctionalInterface
    public interface MethodHandler {
        byte[] handle(String correlationId, byte[] request);
    }

    public KafkaRpcServer(Properties consumerConfig, Properties producerConfig,
                          String requestTopic, String replyTopic,
                          Map<String, MethodHandler> handlers) {
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.handlers = Map.copyOf(handlers);

        Properties cons = new Properties();
        cons.putAll(consumerConfig);
        cons.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        cons.putIfAbsent("value.deserializer", ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(cons);

        Properties prod = new Properties();
        prod.putAll(producerConfig);
        prod.putIfAbsent("key.serializer", StringSerializer.class.getName());
        prod.putIfAbsent("value.serializer", ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(prod);
    }

    /** Constructor for testing - inject consumer and producer. */
    KafkaRpcServer(Consumer<String, byte[]> consumer, Producer<String, byte[]> producer,
                   String requestTopic, String replyTopic, Map<String, MethodHandler> handlers) {
        this.consumer = consumer;
        this.producer = producer;
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.handlers = Map.copyOf(handlers);
    }

    public void start() {
        consumer.subscribe(Collections.singletonList(requestTopic));
        consumerThread = Thread.ofVirtual().name("kafka-rpc-server").start(this::run);
        log.info("Kafka RPC server started, requestTopic={}, replyTopic={}", requestTopic, replyTopic);
    }

    public void stop() {
        running.set(false);
        consumer.wakeup();
        if (consumerThread != null) {
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void run() {
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

        if (correlationId == null) {
            log.warn("Dropping message without correlation ID");
            return;
        }

        MethodHandler handler = method != null ? handlers.get(method) : null;
        if (handler == null) {
            // try first (and only) handler if no method specified
            if (handlers.size() == 1) {
                handler = handlers.values().iterator().next();
            } else {
                log.warn("No handler for method: {}", method);
                return;
            }
        }

        try {
            byte[] response = handler.handle(correlationId, record.value());
            ProducerRecord<String, byte[]> reply = new ProducerRecord<>(replyTopic, record.key(), response);
            reply.headers()
                    .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                    .add(KafkaRpcConstants.HEADER_METHOD, (method != null ? method : "").getBytes());
            producer.send(reply);
        } catch (Exception e) {
            log.error("Handler error for correlationId={}", correlationId, e);
            // TODO: send error response
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

    @Override
    public void close() {
        stop();
        consumer.close();
        producer.close();
    }
}
