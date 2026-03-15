package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.StreamSink;
import io.bio4j.kafkarpc.spring.KafkaRpcProperties;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Component
public class GreeterServiceImpl extends GreeterKafkaRpc.ServiceBase {

    private static final Logger log = LoggerFactory.getLogger(GreeterServiceImpl.class);

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

    @Override
    protected void notify(NotifyRequest request) {
        log.info("Notify (oneway): event={}", request.getEvent());
    }

    @Override
    protected void streamCount(StreamCountRequest request, StreamSink sink) throws IOException {
        for (int i = request.getFrom(); i <= request.getTo(); i++) {
            if (sink.isCancelled()) break;
            sink.send(StreamCountItem.newBuilder().setValue(i).build().toByteArray());
        }
    }

    @Override
    protected void scalableStreamCount(StreamCountRequest request, StreamSink sink) throws IOException {
        for (int i = request.getFrom(); i <= request.getTo(); i++) {
            if (sink.isCancelled()) break;
            sink.send(StreamCountItem.newBuilder().setValue(i).build().toByteArray());
        }
    }
}
