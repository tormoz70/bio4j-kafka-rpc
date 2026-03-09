package io.bio4j.kafkarpc;

import java.util.Map;

/** Marker interface for Kafka RPC service implementations (Spring integration). */
public interface KafkaRpcService {

    /** Service name used for config lookup (e.g. kafka-rpc.service.greeter). */
    String getServiceName();

    String getRequestTopic();

    Map<String, KafkaRpcServer.MethodHandler> getHandlers();
}
