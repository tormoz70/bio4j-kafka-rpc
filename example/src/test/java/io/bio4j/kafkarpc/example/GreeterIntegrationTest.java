package io.bio4j.kafkarpc.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class GreeterIntegrationTest {

    private static final String REQUEST_TOPIC = "greeter.request";
    private static final String REPLY_TOPIC = "greeter.reply";

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withReuse(true);

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka-rpc.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeEach
    void setUp() throws Exception {
        createTopics(kafka.getBootstrapServers());
    }

    @Test
    void greetReturnsExpectedResponse() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/greet?name=Testcontainers",
                String.class);
        assertEquals(200, response.getStatusCode().value());
        assertEquals("Hello, Testcontainers!", response.getBody());
    }

    @Test
    void greetWithDefaultName() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/greet",
                String.class);
        assertEquals(200, response.getStatusCode().value());
        assertEquals("Hello, World!", response.getBody());
    }

    private void createTopics(String bootstrapServers) throws Exception {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (var admin = AdminClient.create(props)) {
            try {
                admin.createTopics(List.of(
                        new NewTopic(REQUEST_TOPIC, 1, (short) 1),
                        new NewTopic(REPLY_TOPIC, 1, (short) 1)
                )).all().get();
            } catch (Exception e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    return;
                }
                throw e;
            }
        }
    }
}
