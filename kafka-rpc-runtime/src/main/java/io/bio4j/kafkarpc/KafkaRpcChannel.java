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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/** Channel for request-response RPC over Kafka. Handles correlation ID, request/reply topics, and timeout. */
@Slf4j
@lombok.Getter
public class KafkaRpcChannel implements AutoCloseable {

    private final Producer<String, byte[]> producer;
    private final Consumer<String, byte[]> consumer;
    private final String requestTopic;
    private final String replyTopic;
    private final int timeoutMs;

    public KafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                           String requestTopic, String replyTopic) {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, KafkaRpcConstants.DEFAULT_TIMEOUT_MS);
    }

    public KafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                           String requestTopic, String replyTopic, int timeoutMs) {
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.timeoutMs = timeoutMs;

        Properties prod = new Properties();
        prod.putAll(producerConfig);
        ensureProducerDefaults(prod);
        this.producer = new KafkaProducer<>(prod);

        Properties cons = new Properties();
        cons.putAll(consumerConfig);
        ensureConsumerDefaults(cons);
        this.consumer = new KafkaConsumer<>(cons);
        this.consumer.subscribe(Collections.singletonList(replyTopic));
    }

    /** Constructor for testing - inject producer and consumer. */
    KafkaRpcChannel(Producer<String, byte[]> producer, Consumer<String, byte[]> consumer,
                    String requestTopic, String replyTopic, int timeoutMs) {
        this.producer = producer;
        this.consumer = consumer;
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.timeoutMs = timeoutMs;
        this.consumer.subscribe(Collections.singletonList(replyTopic));
    }

    private static void ensureProducerDefaults(Properties p) {
        p.putIfAbsent("key.serializer", StringSerializer.class.getName());
        p.putIfAbsent("value.serializer", ByteArraySerializer.class.getName());
    }

    private static void ensureConsumerDefaults(Properties p) {
        p.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        p.putIfAbsent("value.deserializer", ByteArrayDeserializer.class.getName());
    }

    /**
     * Sends a request and waits for the response.
     *
     * @param correlationId unique ID to match request with response
     * @param requestBytes  serialized request
     * @return response bytes
     */
    public byte[] request(String correlationId, byte[] requestBytes) throws IOException, TimeoutException {
        return request(correlationId, requestBytes, null);
    }

    /**
     * Sends a request and waits for the response.
     *
     * @param correlationId unique ID to match request with response
     * @param requestBytes  serialized request
     * @param headers       optional headers (e.g. method name)
     */
    public byte[] request(String correlationId, byte[] requestBytes, Map<String, String> headers)
            throws IOException, TimeoutException {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes());
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes() : new byte[0]));
        }

        producer.send(record);

        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> r : records) {
                String recvCorrelationId = getHeader(r, KafkaRpcConstants.HEADER_CORRELATION_ID);
                if (correlationId.equals(recvCorrelationId)) {
                    return r.value();
                }
            }
        }
        throw new TimeoutException("No response within " + timeoutMs + " ms for correlationId=" + correlationId);
    }

    /**
     * Sends a request asynchronously.
     */
    public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes) {
        return requestAsync(correlationId, requestBytes, null);
    }

    public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes, Map<String, String> headers) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return request(correlationId, requestBytes, headers);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void sendReply(String correlationId, byte[] responseBytes, String method) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(replyTopic, correlationId, responseBytes);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes() : new byte[0]);
        producer.send(record);
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
    public void close() {
        producer.close();
        consumer.close();
    }
}
