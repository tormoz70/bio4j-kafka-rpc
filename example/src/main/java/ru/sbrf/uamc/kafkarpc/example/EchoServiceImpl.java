package ru.sbrf.uamc.kafkarpc.example;

import ru.sbrf.uamc.kafkarpc.spring.KafkaRpcProperties;
import ru.sbrf.uamc.kafkarpc.spring.ServiceConfigTools;
import org.springframework.stereotype.Component;

@Component
public class EchoServiceImpl extends EchoKafkaRpc.ServiceBase {

    public EchoServiceImpl(KafkaRpcProperties properties) {
        super(ServiceConfigTools.resolveRequestTopic(properties, "echo"));
    }

    @Override
    protected EchoResponse echo(EchoRequest request) {
        return EchoResponse.newBuilder()
                .setMessage("Echo: " + request.getMessage())
                .build();
    }
}
