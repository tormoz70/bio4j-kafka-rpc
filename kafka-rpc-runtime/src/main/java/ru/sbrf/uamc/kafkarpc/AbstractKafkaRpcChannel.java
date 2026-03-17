package ru.sbrf.uamc.kafkarpc;

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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Single-use channel: one producer + one consumer per instance. Subclass for generated *RpcChannel.
 * @deprecated Use {@link PooledKafkaRpcChannel} for better resource utilization and streaming support.
 */
@Deprecated
@Slf4j
public abstract class AbstractKafkaRpcChannel implements KafkaRpcChannel {

    private final Producer<String, byte[]> producer;
    private final Consumer<String, byte[]> consumer;
    private final String requestTopic;
    private final String replyTopic;
    private final int timeoutMs;

    protected AbstractKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
                                      String requestTopic, String replyTopic) {
        this(producerConfig, consumerConfig, requestTopic, replyTopic, KafkaRpcConstants.DEFAULT_TIMEOUT_MS);
    }

    protected AbstractKafkaRpcChannel(Properties producerConfig, Properties consumerConfig,
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

    protected AbstractKafkaRpcChannel(Producer<String, byte[]> producer, Consumer<String, byte[]> consumer,
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
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestTopic, correlationId, requestBytes);
        record.headers()
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8));
        if (headers != null) {
            headers.forEach((k, v) -> record.headers().add(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : new byte[0]));
        }

        producer.send(record);

        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS));
            for (ConsumerRecord<String, byte[]> r : records) {
                String recvCorrelationId = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_CORRELATION_ID);
                if (correlationId.equals(recvCorrelationId)) {
                    String errorMsg = KafkaRpcConstants.getHeader(r, KafkaRpcConstants.HEADER_ERROR);
                    if (errorMsg != null) {
                        throw new IOException("Server error: " + errorMsg);
                    }
                    return r.value();
                }
            }
        }
        throw new TimeoutException("No response within " + timeoutMs + " ms for correlationId=" + correlationId);
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
        throw new UnsupportedOperationException("Server streaming is only supported with PooledKafkaRpcChannel");
    }

    @Override
    public CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes) {
        return requestAsync(correlationId, requestBytes, null);
    }

    @Override
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
                .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(KafkaRpcConstants.HEADER_METHOD, method != null ? method.getBytes(StandardCharsets.UTF_8) : new byte[0]);
        producer.send(record);
    }

    @Override
    public void close() {
        producer.close();
        consumer.close();
    }
}
