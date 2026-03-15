package io.bio4j.kafkarpc;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/** Header names, conventions, and utilities for Kafka RPC. */
@UtilityClass
public class KafkaRpcConstants {

    public static final String HEADER_CORRELATION_ID = "kafka-rpc-correlation-id";
    public static final String HEADER_METHOD = "kafka-rpc-method";
    public static final String HEADER_REPLY_TOPIC = "kafka-rpc-reply-topic";
    public static final String HEADER_ERROR = "kafka-rpc-error";

    /** Client sends with stream request; server sends on last chunk. */
    public static final String HEADER_STREAM_END = "kafka-rpc-stream-end";
    /** Client sends with healthcheck request to identify the stream. */
    public static final String HEADER_STREAM_ID = "kafka-rpc-stream-id";
    /** Client sends to mark request as server-streaming. */
    public static final String HEADER_IS_STREAM = "kafka-rpc-stream";
    /** Client sends with stream request: server idle timeout (ms). Server uses it to cancel stream when no healthcheck. */
    public static final String HEADER_STREAM_SERVER_IDLE_TIMEOUT_MS = "kafka-rpc-stream-server-idle-timeout-ms";
    /** Client sends with stream request: "true" = ordered (all chunks to one partition), "false" = scalable (chunks may go to any partition). Default "true". */
    public static final String HEADER_STREAM_ORDERED = "kafka-rpc-stream-ordered";

    public static final String STREAM_HEALTHCHECK_SUFFIX = "/healthcheck";

    public static final int DEFAULT_TIMEOUT_MS = 30_000;
    /** Default interval for client to send stream healthcheck (ms). */
    public static final int DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS = 5_000;
    /** Default client timeout: no healthcheck response = stream dead (ms). */
    public static final int DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS = 15_000;
    /** Default for stream-server-idle-timeout (ms). Client-only: used when not set in config; server requires header. */
    public static final int DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS = 20_000;

    public static final int DEFAULT_POLL_INTERVAL_MS = 100;
    public static final int DEFAULT_STREAM_BUFFER_SIZE = 1024;

    /** Extract a string header value from a Kafka consumer record. Returns null if missing or empty. */
    public static String getHeader(ConsumerRecord<String, byte[]> record, String name) {
        var iter = record.headers().headers(name).iterator();
        if (iter.hasNext()) {
            byte[] v = iter.next().value();
            return v != null && v.length > 0 ? new String(v, StandardCharsets.UTF_8) : null;
        }
        return null;
    }
}
