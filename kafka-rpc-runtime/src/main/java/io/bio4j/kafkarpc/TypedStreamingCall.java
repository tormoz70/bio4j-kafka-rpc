package io.bio4j.kafkarpc;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Typed server-stream: wraps raw {@link StreamingCall} and parses each chunk with the given parser.
 * Use in generated stubs to return {@code Iterable<ResponseType>} with {@link #close()}.
 */
public final class TypedStreamingCall<T> implements AutoCloseable, Iterable<T> {

    private final StreamingCall raw;
    private final Function<byte[], T> parser;

    public TypedStreamingCall(StreamingCall raw, Function<byte[], T> parser) {
        this.raw = raw;
        this.parser = parser;
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<byte[]> it = raw.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return parser.apply(it.next());
            }
        };
    }

    @Override
    public void close() {
        raw.close();
    }

    public boolean isClosed() {
        return raw.isClosed();
    }
}
