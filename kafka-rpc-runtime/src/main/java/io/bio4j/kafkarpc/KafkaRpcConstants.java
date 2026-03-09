package io.bio4j.kafkarpc;

import lombok.experimental.UtilityClass;

/** Header names and conventions for Kafka RPC. */
@UtilityClass
public class KafkaRpcConstants {

    public static final String HEADER_CORRELATION_ID = "kafka-rpc-correlation-id";
    public static final String HEADER_METHOD = "kafka-rpc-method";
    public static final String HEADER_REPLY_TOPIC = "kafka-rpc-reply-topic";

    /** Client sends with stream request; server sends on last chunk. */
    public static final String HEADER_STREAM_END = "kafka-rpc-stream-end";
    /** Client sends with healthcheck request to identify the stream. */
    public static final String HEADER_STREAM_ID = "kafka-rpc-stream-id";
    /** Client sends to mark request as server-streaming. */
    public static final String HEADER_IS_STREAM = "kafka-rpc-stream";

    public static final String STREAM_HEALTHCHECK_SUFFIX = "/healthcheck";

    public static final int DEFAULT_TIMEOUT_MS = 30_000;
    /** Default interval for client to send stream healthcheck (ms). */
    public static final int DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS = 5_000;
    /** Default client timeout: no healthcheck response = stream dead (ms). */
    public static final int DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS = 15_000;
    /** Default server timeout: no healthcheck from client = stop streaming (ms). */
    public static final int DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS = 20_000;
}
