package io.bio4j.kafkarpc;

import java.util.Map;

/** Marker interface for Kafka RPC service implementations (Spring integration). */
public interface KafkaRpcService {

    String getRequestTopic();

    String getReplyTopic();

    Map<String, KafkaRpcServer.MethodHandler> getHandlers();
}
