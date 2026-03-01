package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.KafkaRpcChannel;
import io.bio4j.kafkarpc.spring.KafkaRpcProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
public class GreeterController {

    private static final String REQUEST_TOPIC = "greeter.request";
    private static final String REPLY_TOPIC = "greeter.reply";

    private final KafkaRpcProperties properties;

    public GreeterController(KafkaRpcProperties properties) {
        this.properties = properties;
    }

    @GetMapping("/greet")
    public String greet(@RequestParam(defaultValue = "World") String name) throws Exception {
        var producerConfig = baseProducerConfig();
        var consumerConfig = baseConsumerConfig();
        try (var channel = new KafkaRpcChannel(producerConfig, consumerConfig, REQUEST_TOPIC, REPLY_TOPIC, properties.getTimeoutMs())) {
            var stub = new GreeterKafkaRpc.Stub(channel, REQUEST_TOPIC, REPLY_TOPIC);
            var response = stub.getGreeting(GetGreetingRequest.newBuilder().setName(name).build());
            return response.getGreeting();
        }
    }

    private Properties baseProducerConfig() {
        var p = new Properties();
        p.put("bootstrap.servers", properties.getBootstrapServers());
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return p;
    }

    private Properties baseConsumerConfig() {
        var p = new Properties();
        p.put("bootstrap.servers", properties.getBootstrapServers());
        p.put("group.id", "greeter-client-" + System.currentTimeMillis());
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("auto.offset.reset", "latest");
        return p;
    }
}
