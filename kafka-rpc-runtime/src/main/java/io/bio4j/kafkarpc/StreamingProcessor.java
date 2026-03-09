package io.bio4j.kafkarpc;

/**
 * Callback processor for server-streaming RPC. Passed to {@link KafkaRpcChannel#startStream};
 * receives messages, finish, and errors on an internal thread.
 */
public interface StreamingProcessor<T> {

    /**
     * Called when a stream message is received.
     */
    void onMessage(T data);

    /**
     * Called when the stream ends normally (server sent stream end).
     */
    default void onFinish() {}

    /**
     * Called when the stream fails (e.g. healthcheck timeout, parse error).
     */
    default void onError(Throwable error) {}
}
