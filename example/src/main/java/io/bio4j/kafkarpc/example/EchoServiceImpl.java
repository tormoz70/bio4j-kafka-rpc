package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.spring.KafkaRpcProperties;
import org.springframework.stereotype.Component;

@Component
public class EchoServiceImpl extends EchoKafkaRpc.ServiceBase {

    private final String requestTopic;

    public EchoServiceImpl(KafkaRpcProperties properties) {
        this.requestTopic = properties.getService().getOrDefault("echo", new KafkaRpcProperties.Service())
                .getRequestTopic();
        if (this.requestTopic == null || this.requestTopic.isEmpty()) {
            throw new IllegalStateException("kafka-rpc.service.echo.request-topic must be set");
        }
    }

    @Override
    public String getRequestTopic() {
        return requestTopic;
    }

    @Override
    protected EchoResponse echo(EchoRequest request) {
        return EchoResponse.newBuilder()
                .setMessage("Echo: " + request.getMessage())
                .build();
    }
}
