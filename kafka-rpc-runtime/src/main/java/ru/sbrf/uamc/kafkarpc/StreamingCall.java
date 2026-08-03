package ru.sbrf.uamc.kafkarpc;

/**
 * Handle for an active client-side server-streaming RPC. Close to stop receiving chunks
 * and release channel resources (try-with-resources supported).
 */
public interface StreamingCall extends AutoCloseable {

    String correlationId();

    @Override
    void close();
}
