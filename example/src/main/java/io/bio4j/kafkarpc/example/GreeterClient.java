package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.KafkaRpcChannel;

import java.util.Properties;

public class GreeterClient {

    public static void main(String[] args) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "localhost:9092");
        consumerConfig.put("group.id", "greeter-client-" + System.currentTimeMillis());
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerConfig.put("auto.offset.reset", "latest");

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (var channel = new KafkaRpcChannel(producerConfig, consumerConfig,
                "greeter.request", "greeter.reply")) {
            var stub = new GreeterKafkaRpc.Stub(channel, "greeter.request", "greeter.reply");

            var greetingResp = stub.getGreeting(GetGreetingRequest.newBuilder().setName("World").build());
            System.out.println("GetGreeting: " + greetingResp.getGreeting());

            var sayHelloResp = stub.sayHello(SayHelloRequest.newBuilder().setMessage("Hi!").build());
            System.out.println("SayHello: " + sayHelloResp.getReply());
        }
    }
}