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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = KafkaRpcExampleApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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

    @Test
    void streamCountReturnsSequenceAndEnds() throws Exception {
        streamCountOrdered_returnsSequenceInOrder();
    }

    /** Ordered stream (Stream*): all chunks go to one partition, message order preserved. */
    @Test
    void streamCountOrdered_returnsSequenceInOrder() throws Exception {
        var request = StreamCountRequest.newBuilder().setFrom(1).setTo(5).build();
        var values = new java.util.ArrayList<Integer>();
        var done = new java.util.concurrent.CountDownLatch(1);
        greeterStubProvider.getStub().streamCount(request, new GreeterKafkaRpc.StreamCountProcessor() {
            @Override
            public void onMessage(StreamCountItem item) { values.add(item.getValue()); }
            @Override
            public void onFinish() { done.countDown(); }
            @Override
            public void onError(Throwable error) { done.countDown(); }
        });
        assertTrue(done.await(60, java.util.concurrent.TimeUnit.SECONDS), "Stream should finish");
        assertEquals(java.util.List.of(1, 2, 3, 4, 5), values, "Ordered stream must deliver messages in order");
    }

    /** Scalable stream (ScalableStream*): chunks may be distributed across partitions; we only assert all items received. */
    @Test
    void scalableStreamCount_receivesAllItems() throws Exception {
        var request = StreamCountRequest.newBuilder().setFrom(1).setTo(10).build();
        var values = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
        var done = new java.util.concurrent.CountDownLatch(1);
        greeterStubProvider.getStub().scalableStreamCount(request, new GreeterKafkaRpc.ScalableStreamCountProcessor() {
            @Override
            public void onMessage(StreamCountItem item) { values.add(item.getValue()); }
            @Override
            public void onFinish() { done.countDown(); }
            @Override
            public void onError(Throwable error) { done.countDown(); }
        });
        assertTrue(done.await(60, java.util.concurrent.TimeUnit.SECONDS), "Stream should finish");
        assertEquals(10, values.size(), "Should receive 10 items");
        var sorted = new java.util.ArrayList<>(values);
        java.util.Collections.sort(sorted);
        assertEquals(java.util.List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), sorted, "Scalable stream must deliver all values (order may vary)");
    }
}
