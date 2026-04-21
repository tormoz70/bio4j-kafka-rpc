package ru.sbrf.uamc.kafkarpc;

import lombok.experimental.UtilityClass;

/** Stable event names for structured Kafka RPC logging. */
@UtilityClass
public class KafkaRpcLogEvents {

    public static final String CONSUMER_STARTED = "event=kafka.rpc.consumer.started";
    public static final String CONSUMER_RECREATING = "event=kafka.rpc.consumer.recreating";
    public static final String SERVER_STARTED = "event=kafka.rpc.server.started";
    public static final String SERVER_LIFECYCLE_STARTED = "event=kafka.rpc.server.lifecycle-started";
    public static final String CHANNEL_CREATED = "event=kafka.rpc.channel.created";
    public static final String CHANNEL_CLOSE_FAILED = "event=kafka.rpc.channel.close-failed";

    public static final String STREAM_IDLE_TIMEOUT = "event=kafka.rpc.stream.idle-timeout";
    public static final String REQUEST_PROCESSING_FAILED = "event=kafka.rpc.request.processing-failed";
    public static final String RECEIVE = "event=kafka.rpc.receive";
    public static final String SEND = "event=kafka.rpc.send";
    public static final String SEND_FAILED = "event=kafka.rpc.send-failed";

    public static final String REQUEST_DROPPED = "event=kafka.rpc.request.dropped";
    public static final String RESPONSE_DROPPED = "event=kafka.rpc.response.dropped";
    public static final String HANDLER_FAILED = "event=kafka.rpc.handler-failed";
    public static final String STREAM_HANDLER_FAILED = "event=kafka.rpc.stream.handler-failed";
    public static final String ERROR_REPLY_PREPARE_FAILED = "event=kafka.rpc.error-reply.prepare-failed";
    public static final String ERROR_REPLY_FAILED = "event=kafka.rpc.error-reply.failed";
}
