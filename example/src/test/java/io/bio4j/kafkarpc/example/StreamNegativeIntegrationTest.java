package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.KafkaRpcConstants;
import io.bio4j.kafkarpc.spring.KafkaRpcChannelPool;
import io.bio4j.kafkarpc.spring.KafkaRpcProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Negative tests: server stops responding to healthcheck, client stops sending healthcheck.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        topics = {"greeter.request", "greeter.reply", "greeter.request.neg", "greeter.reply.neg"},
        bootstrapServersProperty = "kafka-rpc.bootstrap-servers",
        kraft = false
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Tag("integration")
@TestPropertySource(properties = {
        "kafka-rpc.clients.greeter-neg.request-topic=greeter.request.neg",
        "kafka-rpc.clients.greeter-neg.reply-topic=greeter.reply.neg"
})
class StreamNegativeIntegrationTest {

    private static final String REQUEST_TOPIC_NEG = "greeter.request.neg";
    private static final String REPLY_TOPIC_NEG = "greeter.reply.neg";

    @Autowired
    private KafkaRpcChannelPool channelPool;

    @Value("${kafka-rpc.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * "Server dies": fake server sends a few chunks then never responds to healthcheck.
     * Client should receive those chunks and then get an error (stream dead) when healthcheck times out.
     */
    @Test
    void whenServerStopsRespondingToHealthcheck_clientSeesStreamDead() throws Exception {
        CountDownLatch serverStarted = new CountDownLatch(1);
        Thread badServer = new Thread(() -> runBadServer(serverStarted), "bad-server");
        badServer.setDaemon(true);
        badServer.start();
        assertTrue(serverStarted.await(10, TimeUnit.SECONDS), "Bad server should start");

        var request = StreamCountRequest.newBuilder().setFrom(1).setTo(10).build();
        var stub = new GreeterKafkaRpc.Stub(channelPool.getOrCreate("greeter-neg"));
        var values = new ArrayList<Integer>();
        Exception streamError = null;

        try (var stream = stub.streamCount(request)) {
            for (StreamCountItem item : stream) {
                values.add(item.getValue());
            }
        } catch (Exception e) {
            streamError = e;
        }

        assertEquals(List.of(1, 2), values, "Should receive exactly 2 chunks before server 'dies'");
        assertNotNull(streamError, "Expected exception when server stops responding to healthcheck");
        String fullMessage = streamError.getMessage() + (streamError.getCause() != null ? " / " + streamError.getCause().getMessage() : "");
        assertTrue(fullMessage.contains("Stream") || fullMessage.contains("healthcheck"),
                "Expected stream or healthcheck in error: " + fullMessage);
    }

    private void runBadServer(CountDownLatch started) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("group.id", "bad-server");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());

        try (var consumer = new KafkaConsumer<String, byte[]>(consumerProps);
             var producer = new KafkaProducer<String, byte[]>(producerProps)) {
            consumer.subscribe(Collections.singletonList(REQUEST_TOPIC_NEG));
            started.countDown();

            var records = consumer.poll(Duration.ofSeconds(30));
            for (ConsumerRecord<String, byte[]> record : records) {
                String method = getHeader(record, KafkaRpcConstants.HEADER_METHOD);
                if (method != null && method.endsWith(KafkaRpcConstants.STREAM_HEALTHCHECK_SUFFIX)) continue;
                String correlationId = getHeader(record, KafkaRpcConstants.HEADER_CORRELATION_ID);
                String replyTopic = getHeader(record, KafkaRpcConstants.HEADER_REPLY_TOPIC);
                String isStream = getHeader(record, KafkaRpcConstants.HEADER_IS_STREAM);
                if (!"true".equals(isStream) || correlationId == null || replyTopic == null) continue;

                for (int v = 1; v <= 2; v++) {
                    byte[] chunk = StreamCountItem.newBuilder().setValue(v).build().toByteArray();
                    var pr = new ProducerRecord<String, byte[]>(replyTopic, correlationId, chunk);
                    pr.headers()
                            .add(KafkaRpcConstants.HEADER_CORRELATION_ID, correlationId.getBytes())
                            .add(KafkaRpcConstants.HEADER_METHOD, "Greeter/StreamCount".getBytes());
                    producer.send(pr);
                }
                break;
            }
        }
    }

    private static String getHeader(ConsumerRecord<String, byte[]> record, String name) {
        var it = record.headers().headers(name).iterator();
        if (!it.hasNext()) return null;
        byte[] v = it.next().value();
        return v != null && v.length > 0 ? new String(v) : null;
    }

    public static class StreamChunkCountHolder {
        public final AtomicInteger chunkCount = new AtomicInteger(0);
    }
}

/**
 * Client dies: streamHealthcheckEnabled=false, so server stops sending after idle timeout.
 * Uses a counting service that sends one chunk per 500ms.
 */
@SpringBootTest(classes = { StreamNegativeClientDiesIntegrationTest.TestApp.class,
        StreamNegativeClientDiesIntegrationTest.CountingStreamConfig.class }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        topics = {"greeter.request", "greeter.reply", "echo.request", "echo.reply"},
        bootstrapServersProperty = "kafka-rpc.bootstrap-servers",
        kraft = false
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Tag("integration")
@TestPropertySource(properties = "kafka-rpc.clients.greeter.stream-healthcheck-enabled=false")
class StreamNegativeClientDiesIntegrationTest {

    @SpringBootApplication
    @ComponentScan(basePackageClasses = KafkaRpcExampleApplication.class,
            excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = GreeterServiceImpl.class))
    static class TestApp {
        public static void main(String[] args) {
            SpringApplication.run(TestApp.class, args);
        }
    }

    @Autowired
    private KafkaRpcChannelPool channelPool;

    @Autowired
    private StreamNegativeIntegrationTest.StreamChunkCountHolder chunkCountHolder;

    /**
     * Client does not send healthchecks; server cancels stream after ~20s idle.
     * We read 2 chunks then wait; server should stop sending after timeout, so total sent < 50.
     */
    @Test
    void whenClientStopsSendingHealthcheck_serverCancelsStream() throws Exception {
        var request = StreamCountRequest.newBuilder().setFrom(1).setTo(200).build();
        var stub = new GreeterKafkaRpc.Stub(channelPool.getOrCreate("greeter"));
        var values = new ArrayList<Integer>();

        try (var stream = stub.streamCount(request)) {
            for (StreamCountItem item : stream) {
                values.add(item.getValue());
                if (values.size() >= 2) break;
            }
        }
        assertEquals(2, values.size());

        Thread.sleep(25_000);

        int sent = chunkCountHolder.chunkCount.get();
        assertTrue(sent < 60, "Server should have cancelled and stopped sending; sent=" + sent);
    }

    @Configuration
    static class CountingStreamConfig {
        @Bean
        StreamNegativeIntegrationTest.StreamChunkCountHolder streamChunkCountHolder() {
            return new StreamNegativeIntegrationTest.StreamChunkCountHolder();
        }

        @Bean
        @Primary
        GreeterKafkaRpc.ServiceBase countingGreeterServiceImpl(
                KafkaRpcProperties properties,
                StreamNegativeIntegrationTest.StreamChunkCountHolder holder) {
            return new GreeterKafkaRpc.ServiceBase(properties) {
                @Override
                protected GetGreetingResponse getGreeting(GetGreetingRequest request) {
                    return GetGreetingResponse.newBuilder().setGreeting("Hello, " + request.getName() + "!").build();
                }
                @Override
                protected SayHelloResponse sayHello(SayHelloRequest request) {
                    return SayHelloResponse.newBuilder().setReply("Echo: " + request.getMessage()).build();
                }
                @Override
                protected void notify(NotifyRequest request) {}
                @Override
                protected void streamCount(StreamCountRequest request, io.bio4j.kafkarpc.StreamSink sink) throws IOException {
                    for (int i = request.getFrom(); i <= request.getTo(); i++) {
                        if (sink.isCancelled()) break;
                        sink.send(StreamCountItem.newBuilder().setValue(i).build().toByteArray());
                        holder.chunkCount.incrementAndGet();
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            };
        }
    }
}
