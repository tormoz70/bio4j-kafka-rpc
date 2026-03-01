package io.bio4j.kafkarpc;

import lombok.experimental.UtilityClass;

/** Header names and conventions for Kafka RPC. */
@UtilityClass
public class KafkaRpcConstants {

    public static final String HEADER_CORRELATION_ID = "kafka-rpc-correlation-id";
    public static final String HEADER_METHOD = "kafka-rpc-method";
    public static final String HEADER_REPLY_TOPIC = "kafka-rpc-reply-topic";

    public static final int DEFAULT_TIMEOUT_MS = 30_000;
}
