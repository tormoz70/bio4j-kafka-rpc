package io.bio4j.kafkarpc;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Channel for request-response RPC over Kafka. Implementations may be single-use (per request)
 * or pooled (shared consumer, keyed by client name).
 */
public interface KafkaRpcChannel extends AutoCloseable {

    String getRequestTopic();

    String getReplyTopic();

    byte[] request(String correlationId, byte[] requestBytes) throws IOException, TimeoutException;

    byte[] request(String correlationId, byte[] requestBytes, Map<String, String> headers)
            throws IOException, TimeoutException;

    /**
     * Fire-and-forget: send request without waiting for a response (oneway).
     * Use for RPCs that return google.protobuf.Empty.
     */
    void send(String correlationId, byte[] requestBytes) throws IOException;

    void send(String correlationId, byte[] requestBytes, Map<String, String> headers) throws IOException;

    CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes);

    CompletableFuture<byte[]> requestAsync(String correlationId, byte[] requestBytes, Map<String, String> headers);

    @Override
    void close();
}
