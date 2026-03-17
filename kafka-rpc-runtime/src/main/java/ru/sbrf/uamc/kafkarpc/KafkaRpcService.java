package ru.sbrf.uamc.kafkarpc;

import java.util.Collections;
import java.util.Map;

/** Marker interface for Kafka RPC service implementations (Spring integration). */
public interface KafkaRpcService {

    /** Service name used for config lookup (e.g. kafka-rpc.service.greeter). */
    String getServiceName();

    String getRequestTopic();

    Map<String, KafkaRpcServer.MethodHandler> getHandlers();

    /** Server-streaming methods: method name -> handler. Default: none. */
    default Map<String, KafkaRpcServer.StreamMethodHandler> getStreamHandlers() {
        return Collections.emptyMap();
    }
}
