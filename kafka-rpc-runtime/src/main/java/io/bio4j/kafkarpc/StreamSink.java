package io.bio4j.kafkarpc;

import java.io.IOException;

/**
 * Server-side sink for server-streaming RPC. Handler sends chunks via {@link #send(byte[])}
 * and signals end of stream with {@link #end()}. If client stops sending healthchecks,
 * the framework will call {@link #cancel()}.
 */
public interface StreamSink {

    void send(byte[] chunk) throws IOException;

    void end() throws IOException;

    /** Called by framework when client healthcheck timeout; handler should stop sending. */
    void cancel();

    boolean isCancelled();
}
