package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.spring.KafkaRpcProperties;
import org.springframework.stereotype.Component;

@Component
public class GreeterServiceImpl extends GreeterKafkaRpc.ServiceBase {

    private final String requestTopic;

    public GreeterServiceImpl(KafkaRpcProperties properties) {
        this.requestTopic = properties.getService().getOrDefault("greeter", new KafkaRpcProperties.Service())
                .getRequestTopic();
        if (this.requestTopic == null || this.requestTopic.isEmpty()) {
            throw new IllegalStateException("kafka-rpc.service.greeter.request-topic must be set");
        }
    }

    @Override
    public String getRequestTopic() {
        return requestTopic;
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
}
