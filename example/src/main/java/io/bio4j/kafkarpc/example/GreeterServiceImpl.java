package io.bio4j.kafkarpc.example;

import org.springframework.stereotype.Component;

@Component
public class GreeterServiceImpl extends GreeterKafkaRpc.ServiceBase {

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
}
