package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.spring.KafkaRpcProperties;
import org.springframework.stereotype.Component;

@Component
public class GreeterServiceImpl extends GreeterKafkaRpc.ServiceBase {

    public GreeterServiceImpl(KafkaRpcProperties properties) {
        super(properties);
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
