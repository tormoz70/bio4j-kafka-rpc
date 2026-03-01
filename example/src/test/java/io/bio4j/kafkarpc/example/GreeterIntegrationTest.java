package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.KafkaRpcChannel;
import io.bio4j.kafkarpc.KafkaRpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class GreeterIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    private ExecutorService serverExecutor;
    private String bootstrapServers;

    @BeforeEach
    void setUp() {
        bootstrapServers = kafka.getBootstrapServers();
    }

    @AfterEach
    void tearDown() {
        if (serverExecutor != null) {
            serverExecutor.shutdownNow();
        }
    }

    @Test
    void getGreetingReturnsExpectedResponse() throws Exception {
        startServer();
        Thread.sleep(2000);

        Properties consumerConfig = consumerConfig();
        Properties producerConfig = producerConfig();

        try (var channel = new KafkaRpcChannel(producerConfig, consumerConfig,
                "greeter.request", "greeter.reply")) {
            var stub = new GreeterKafkaRpc.Stub(channel, "greeter.request", "greeter.reply");
            var resp = stub.getGreeting(GetGreetingRequest.newBuilder().setName("Test").build());
            assertEquals("Hello, Test", resp.getGreeting());
        }
    }

    @Test
    void sayHelloReturnsExpectedResponse() throws Exception {
        startServer();
        Thread.sleep(2000);

        Properties consumerConfig = consumerConfig();
        Properties producerConfig = producerConfig();

        try (var channel = new KafkaRpcChannel(producerConfig, consumerConfig,
                "greeter.request", "greeter.reply")) {
            var stub = new GreeterKafkaRpc.Stub(channel, "greeter.request", "greeter.reply");
            var resp = stub.sayHello(SayHelloRequest.newBuilder().setMessage("Hi").build());
            assertEquals("Echo: Hi", resp.getReply());
        }
    }

    private void startServer() {
        var impl = new GreeterKafkaRpc.ServiceBase() {
            @Override
            public String getRequestTopic() { return "greeter.request"; }
            @Override
            public String getReplyTopic() { return "greeter.reply"; }
            @Override
            protected GetGreetingResponse getGreeting(GetGreetingRequest req) {
                return GetGreetingResponse.newBuilder().setGreeting("Hello, " + req.getName()).build();
            }
            @Override
            protected SayHelloResponse sayHello(SayHelloRequest req) {
                return SayHelloResponse.newBuilder().setReply("Echo: " + req.getMessage()).build();
            }
        };

        Properties consumerConfig = consumerConfig();
        Properties producerConfig = producerConfig();

        KafkaRpcServer server = new KafkaRpcServer(consumerConfig, producerConfig,
                impl.getRequestTopic(), impl.getReplyTopic(), impl.getHandlers());
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            server.start();
            return null;
        });
    }

    private Properties consumerConfig() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("group.id", "test-" + System.currentTimeMillis());
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("auto.offset.reset", "earliest");
        return p;
    }

    private Properties producerConfig() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return p;
    }
}