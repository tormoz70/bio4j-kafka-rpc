package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.KafkaRpcChannel;
import lombok.experimental.UtilityClass;

import java.util.Properties;

/** Example client calling the Greeter service. */
@UtilityClass
public class GreeterClient {

    public static void main(String[] args) throws Exception {
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");

        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "localhost:9092");
        consumerConfig.put("group.id", "greeter-client-" + System.currentTimeMillis());
        consumerConfig.put("auto.offset.reset", "latest");

        try (var channel = new KafkaRpcChannel(
                producerConfig, consumerConfig,
                "greeter.request", "greeter.reply",
                10_000)) {

            var stub = new GreeterKafkaRpc.Stub(channel, "greeter.request", "greeter.reply");

            var greetingResponse = stub.getGreeting(
                    GetGreetingRequest.newBuilder().setName("World").build());
            System.out.println("Greeting: " + greetingResponse.getGreeting());

            var sayHelloResponse = stub.sayHello(
                    SayHelloRequest.newBuilder().setMessage("Hi from Kafka RPC!").build());
            System.out.println("Reply: " + sayHelloResponse.getReply());
        }
    }
}
