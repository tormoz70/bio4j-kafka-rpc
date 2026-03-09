package io.bio4j.kafkarpc;

/**
 * Element from stream queue: either data, end-of-stream, or poison (stream dead).
 */
public sealed interface StreamChunk permits StreamChunk.Data, StreamChunk.End, StreamChunk.Poison {

    record Data(byte[] bytes) implements StreamChunk {}

    record End() implements StreamChunk {}

    record Poison(Throwable cause) implements StreamChunk {}
}
