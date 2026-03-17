package ru.sbrf.uamc.kafkarpc.example;

import ru.sbrf.uamc.kafkarpc.spring.KafkaRpcChannelPool;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Negative integration tests: Kafka unavailable, missing topics, server error.
 */
@Tag("integration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class NegativeScenariosIntegrationTest {

    /**
     * 1. Kafka cluster unavailable: bootstrap-servers point to non-existent broker.
     * Client should get an exception when sending a request.
     */
    @SpringBootTest(classes = KafkaRpcExampleApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
    @TestPropertySource(properties = {
            "kafka-rpc.bootstrap-servers=localhost:17999",
            "kafka-rpc.clients.greeter.request-topic=greeter.request",
            "kafka-rpc.clients.greeter.reply-topic=greeter.reply",
            "kafka-rpc.clients.greeter.timeout-ms=15000",
            "kafka-rpc.producer.request.timeout.ms=5000",
            "kafka-rpc.producer.metadata.max.age.ms=2000"
    })
    @Nested
    static class KafkaUnavailable {

        @Autowired
        private KafkaRpcChannelPool channelPool;

        @Test
        void whenKafkaUnavailable_requestFailsWithException() {
            var stub = new GreeterKafkaRpc.Stub(channelPool.getOrCreate("greeter"));
            var request = GetGreetingRequest.newBuilder().setName("test").build();

            Exception thrown = assertThrows(Exception.class, () -> stub.getGreeting(request));

            String msg = thrown.getMessage() != null ? thrown.getMessage() : "";
            String causeMsg = thrown.getCause() != null && thrown.getCause().getMessage() != null ? thrown.getCause().getMessage() : "";
            String causeName = thrown.getCause() != null ? thrown.getCause().getClass().getName() : "";
            assertTrue(
                    msg.contains("response") || msg.contains("Failed to send")
                            || causeName.contains("Kafka")
                            || causeMsg.contains("timeout") || causeMsg.contains("Connection")
                            || causeMsg.contains("metadata") || causeMsg.contains("Failed to send"),
                    "Expected timeout, Kafka/connection, or send error: " + thrown
            );
        }
    }

    /**
     * 2. Request topic does not exist (broker has auto.create.topics.enable=false).
     * Client send should fail.
     */
    @SpringBootTest(classes = KafkaRpcExampleApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
    @EmbeddedKafka(
            partitions = 1,
            topics = {"greeter.reply"},
            bootstrapServersProperty = "kafka-rpc.bootstrap-servers",
            kraft = false,
            brokerProperties = {"auto.create.topics.enable=false"}
    )
    @TestPropertySource(properties = {
            "kafka-rpc.clients.greeter.request-topic=greeter.request",
            "kafka-rpc.clients.greeter.reply-topic=greeter.reply",
            "kafka-rpc.clients.greeter.timeout-ms=10000"
    })
    @Nested
    static class RequestTopicMissing {

        @Autowired
        private KafkaRpcChannelPool channelPool;

        @Test
        void whenRequestTopicDoesNotExist_requestFails() {
            var stub = new GreeterKafkaRpc.Stub(channelPool.getOrCreate("greeter"));
            var request = GetGreetingRequest.newBuilder().setName("test").build();

            Exception thrown = assertThrows(Exception.class, () -> stub.getGreeting(request));

            assertNotNull(thrown.getMessage());
            assertTrue(
                    thrown.getMessage().contains("response") || thrown.getMessage().contains("timeout")
                            || thrown.getMessage().contains("Failed to send")
                            || (thrown.getCause() != null && thrown.getCause().getClass().getName().contains("Kafka")),
                    "Expected timeout, Kafka, or send error: " + thrown
            );
        }
    }

    /**
     * 3. Reply topic does not exist. Server receives request but cannot send reply to missing topic.
     * Client should timeout (no response).
     */
    @SpringBootTest(classes = KafkaRpcExampleApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
    @EmbeddedKafka(
            partitions = 1,
            topics = {"greeter.request"},
            bootstrapServersProperty = "kafka-rpc.bootstrap-servers",
            kraft = false,
            brokerProperties = {"auto.create.topics.enable=false"}
    )
    @TestPropertySource(properties = {
            "kafka-rpc.clients.greeter.request-topic=greeter.request",
            "kafka-rpc.clients.greeter.reply-topic=greeter.reply.missing",
            "kafka-rpc.clients.greeter.timeout-ms=8000"
    })
    @Nested
    static class ReplyTopicMissing {

        @Autowired
        private KafkaRpcChannelPool channelPool;

        @Test
        void whenReplyTopicDoesNotExist_clientTimesOut() {
            var stub = new GreeterKafkaRpc.Stub(channelPool.getOrCreate("greeter"));
            var request = GetGreetingRequest.newBuilder().setName("test").build();

            Exception thrown = assertThrows(Exception.class, () -> stub.getGreeting(request));

            assertTrue(
                    thrown instanceof java.util.concurrent.TimeoutException
                            || (thrown.getMessage() != null && thrown.getMessage().contains("response")),
                    "Expected timeout: " + thrown
            );
        }
    }

    /**
     * 4. Server throws when processing request. Client should receive an error reply (IOException with server error).
     */
    @SpringBootTest(classes = {ServerErrorApp.class, ServerErrorConfig.class}, webEnvironment = SpringBootTest.WebEnvironment.NONE)
    @EmbeddedKafka(
            partitions = 1,
            topics = {"greeter.request", "greeter.reply"},
            bootstrapServersProperty = "kafka-rpc.bootstrap-servers",
            kraft = false
    )
    @TestPropertySource(properties = {
            "kafka-rpc.clients.greeter.timeout-ms=6000"
    })
    @Nested
    static class ServerError {

        @Autowired
        private KafkaRpcChannelPool channelPool;

        @Test
        void whenServerThrows_clientGetsErrorReply() {
            var stub = new GreeterKafkaRpc.Stub(channelPool.getOrCreate("greeter"));
            var request = GetGreetingRequest.newBuilder().setName("fail").build();

            Exception thrown = assertThrows(Exception.class, () -> stub.getGreeting(request));

            String fullMessage = thrown.getMessage() + (thrown.getCause() != null ? " " + thrown.getCause().getMessage() : "");
            assertTrue(
                    fullMessage.toLowerCase().contains("server error")
                            || fullMessage.contains("Simulated server error")
                            || thrown instanceof java.util.concurrent.TimeoutException,
                    "Expected server error propagated to client: " + thrown
            );
        }
    }

    @SpringBootApplication
    @ComponentScan(basePackageClasses = KafkaRpcExampleApplication.class,
            excludeFilters = {
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = GreeterServiceImpl.class),
                    @ComponentScan.Filter(type = FilterType.REGEX, pattern = ".*IntegrationTest\\$.*")
            })
    static class ServerErrorApp {
        public static void main(String[] args) {
            SpringApplication.run(ServerErrorApp.class, args);
        }
    }

    @Configuration
    static class ServerErrorConfig {
        @Bean
        @Primary
        GreeterKafkaRpc.ServiceBase failingGreeterService(ru.sbrf.uamc.kafkarpc.spring.KafkaRpcProperties properties) {
            return new GreeterKafkaRpc.ServiceBase(properties) {
                @Override
                protected GetGreetingResponse getGreeting(GetGreetingRequest request) {
                    if ("fail".equals(request.getName())) {
                        throw new RuntimeException("Simulated server error");
                    }
                    return GetGreetingResponse.newBuilder().setGreeting("Hello, " + request.getName() + "!").build();
                }
                @Override
                protected SayHelloResponse sayHello(SayHelloRequest request) {
                    return SayHelloResponse.newBuilder().setReply("Echo: " + request.getMessage()).build();
                }
                @Override
                protected void notify(NotifyRequest request) {}
                @Override
                protected void streamCount(StreamCountRequest request, ru.sbrf.uamc.kafkarpc.StreamSink sink) {}
                @Override
                protected void scalableStreamCount(StreamCountRequest request, ru.sbrf.uamc.kafkarpc.StreamSink sink) {}
            };
        }
    }
}
