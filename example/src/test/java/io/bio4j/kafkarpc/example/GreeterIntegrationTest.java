package io.bio4j.kafkarpc.example;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {"greeter.request", "greeter.reply", "echo.request", "echo.reply"},
        bootstrapServersProperty = "kafka-rpc.bootstrap-servers",
        kraft = false
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Tag("integration")
class GreeterIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private GreeterStubProvider greeterStubProvider;

    @Test
    void greetReturnsExpectedResponse() {
        ResponseEntity<String> response = await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> restTemplate.getForEntity(
                        "http://localhost:" + port + "/greet?name=Testcontainers", String.class),
                        r -> r.getStatusCode().is2xxSuccessful());
        assertNotNull(response.getBody());
        assertEquals("Hello, Testcontainers!", response.getBody());
    }

    @Test
    void greetWithDefaultName() {
        ResponseEntity<String> response = await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> restTemplate.getForEntity(
                        "http://localhost:" + port + "/greet", String.class),
                        r -> r.getStatusCode().is2xxSuccessful());
        assertNotNull(response.getBody());
        assertEquals("Hello, World!", response.getBody());
    }

    @Test
    void echoReturnsExpectedResponse() {
        ResponseEntity<String> response = await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> restTemplate.getForEntity(
                        "http://localhost:" + port + "/echo?message=test", String.class),
                        r -> r.getStatusCode().is2xxSuccessful());
        assertNotNull(response.getBody());
        assertEquals("Echo: test", response.getBody());
    }

    @Test
    void echoWithDefaultMessage() {
        ResponseEntity<String> response = await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> restTemplate.getForEntity(
                        "http://localhost:" + port + "/echo", String.class),
                        r -> r.getStatusCode().is2xxSuccessful());
        assertNotNull(response.getBody());
        assertEquals("Echo: hello", response.getBody());
    }

    @Test
    void getGreetingAsyncReturnsExpectedResponse() throws Exception {
        var request = GetGreetingRequest.newBuilder().setName("AsyncTest").build();
        CompletableFuture<GetGreetingResponse> future = greeterStubProvider.getAsyncStub().getGreetingAsync(request);
        GetGreetingResponse response = future.get(30, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals("Hello, AsyncTest!", response.getGreeting());
    }

    @Test
    void sayHelloAsyncReturnsExpectedResponse() throws Exception {
        var request = SayHelloRequest.newBuilder().setMessage("async-message").build();
        CompletableFuture<SayHelloResponse> future = greeterStubProvider.getAsyncStub().sayHelloAsync(request);
        SayHelloResponse response = future.get(30, TimeUnit.SECONDS);
        assertNotNull(response);
        assertEquals("Echo: async-message", response.getReply());
    }

    @Test
    void notifyOnewayCompletesWithoutException() throws Exception {
        var request = NotifyRequest.newBuilder().setEvent("test-event").build();
        greeterStubProvider.getStub().notify(request);
    }

    @Test
    void notifyOnewayAsyncCompletesWithoutException() throws Exception {
        var request = NotifyRequest.newBuilder().setEvent("async-event").build();
        CompletableFuture<Void> future = greeterStubProvider.getAsyncStub().notifyAsync(request);
        future.get(30, TimeUnit.SECONDS);
    }
}
