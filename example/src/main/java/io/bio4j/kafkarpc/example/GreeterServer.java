package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.KafkaRpcServer;

import java.util.Properties;

public class GreeterServer {

    public static void main(String[] args) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "localhost:9092");
        consumerConfig.put("group.id", "greeter-server");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerConfig.put("auto.offset.reset", "earliest");

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        var impl = new GreeterKafkaRpc.ServiceBase() {
            @Override
            public String getRequestTopic() { return "greeter.request"; }
            @Override
            public String getReplyTopic() { return "greeter.reply"; }
            @Override
            protected GetGreetingResponse getGreeting(GetGreetingRequest req) {
                return GetGreetingResponse.newBuilder()
                    .setGreeting("Hello, " + req.getName())
                    .build();
            }
            @Override
            protected SayHelloResponse sayHello(SayHelloRequest req) {
                return SayHelloResponse.newBuilder()
                    .setReply("Echo: " + req.getMessage())
                    .build();
            }
        };

        KafkaRpcServer server = new KafkaRpcServer(consumerConfig, producerConfig,
            impl.getRequestTopic(), impl.getReplyTopic(), impl.getHandlers());
        server.start();
        System.out.println("Greeter server started. Press Ctrl+C to stop.");
        Thread.currentThread().join();
    }
}