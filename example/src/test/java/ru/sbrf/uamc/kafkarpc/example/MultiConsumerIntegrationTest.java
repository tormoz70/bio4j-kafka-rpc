package ru.sbrf.uamc.kafkarpc.example;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration test with multiple consumers (server and client) and 2 partitions.
 * Verifies scaling via partitioning works end-to-end.
 */
@SpringBootTest(classes = KafkaRpcExampleApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 2,
        topics = {"greeter.request", "greeter.reply", "echo.request", "echo.reply"},
        bootstrapServersProperty = "kafka-rpc.bootstrap-servers",
        kraft = false
)
@TestPropertySource(properties = {
        "kafka-rpc.server-consumer-count=2",
        "kafka-rpc.client-consumer-count=2"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Tag("integration")
class MultiConsumerIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private GreeterStubProvider greeterStubProvider;

    @Test
    void greetWithMultipleConsumersReturnsExpectedResponse() {
        ResponseEntity<String> response = await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> restTemplate.getForEntity(
                        "http://localhost:" + port + "/greet?name=Scale", String.class),
                        r -> r.getStatusCode().is2xxSuccessful());
        assertNotNull(response.getBody());
        assertEquals("Hello, Scale!", response.getBody());
    }

    @Test
    void getGreetingViaStubWithMultipleConsumers() throws Exception {
        var request = GetGreetingRequest.newBuilder().setName("Multi").build();
        GetGreetingResponse response = greeterStubProvider.getStub().getGreeting(request);
        assertNotNull(response);
        assertEquals("Hello, Multi!", response.getGreeting());
    }
}
