package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.KafkaRpcServer;
import lombok.experimental.UtilityClass;

import java.util.Properties;

/** Example server implementing the Greeter service. */
@UtilityClass
public class GreeterServer {

    public static void main(String[] args) {
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "localhost:9092");
        consumerConfig.put("group.id", "greeter-server");

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");

        var impl = new GreeterKafkaRpc.ServiceBase() {
            @Override
            public String getRequestTopic() {
                return "greeter.request";
            }

            @Override
            public String getReplyTopic() {
                return "greeter.reply";
            }

            @Override
            protected GetGreetingResponse getGreeting(GetGreetingRequest request) {
                return GetGreetingResponse.newBuilder()
                        .setGreeting("Hello, " + request.getName() + "!")
                        .build();
            }

            @Override
            protected SayHelloResponse sayHello(SayHelloRequest request) {
                return SayHelloResponse.newBuilder()
                        .setReply("Echo: " + request.getMessage())
                        .build();
            }
        };

        var server = new KafkaRpcServer(
                consumerConfig, producerConfig,
                impl.getRequestTopic(), impl.getReplyTopic(),
                impl.getHandlers());

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
}
