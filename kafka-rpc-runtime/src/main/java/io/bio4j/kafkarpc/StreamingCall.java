package io.bio4j.kafkarpc;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Client-side handle for a server-streaming RPC. Use {@link #iterator()} to read chunks
 * (each element is raw bytes of one streamed message). Caller must call {@link #close()}
 * when done. Healthcheck runs in the background; if server does not respond to healthcheck,
 * the iterator will throw.
 */
public interface StreamingCall extends Closeable {

    Iterator<byte[]> iterator();

    boolean isClosed();

    @Override
    void close();
}
