package io.bio4j.kafkarpc.example;

import io.bio4j.kafkarpc.spring.KafkaRpcProperties;
import org.springframework.stereotype.Component;

@Component
public class EchoServiceImpl extends EchoKafkaRpc.ServiceBase {

    public EchoServiceImpl(KafkaRpcProperties properties) {
        super(properties);
    }

    @Override
    protected EchoResponse echo(EchoRequest request) {
        return EchoResponse.newBuilder()
                .setMessage("Echo: " + request.getMessage())
                .build();
    }
}
