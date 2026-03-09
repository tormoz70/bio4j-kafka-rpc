package io.bio4j.kafkarpc.spring;

import io.bio4j.kafkarpc.KafkaRpcChannel;
import io.bio4j.kafkarpc.PooledKafkaRpcChannel;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pool of shared RPC channels by client name. One PooledKafkaRpcChannel per client (one producer + one consumer thread).
 * Used by generated *ChannelProvider beans. Registered as bean via KafkaRpcChannelPoolAutoConfiguration.
 */
@RequiredArgsConstructor
@Slf4j
public class KafkaRpcChannelPool {

    private final KafkaRpcProperties properties;
    private final Map<String, PooledKafkaRpcChannel> channels = new ConcurrentHashMap<>();

    /**
     * Returns the shared channel for the client. Creates it on first use (lazy).
     *
     * @param clientName key from kafka-rpc.clients (e.g. greeter, echo)
     */
    public KafkaRpcChannel getOrCreate(String clientName) {
        return channels.computeIfAbsent(clientName, this::createChannel);
    }

    private PooledKafkaRpcChannel createChannel(String clientName) {
        String requestTopic = properties.getRequestTopicForClient(clientName);
        String replyTopic = properties.getReplyTopicForClient(clientName);
        if (requestTopic == null || replyTopic == null) {
            throw new IllegalStateException(
                    "kafka-rpc.clients." + clientName + ".request-topic and .reply-topic must be set");
        }
        var producerConfig = properties.getProducerPropertiesForClient(clientName);
        var consumerConfig = properties.getConsumerPropertiesForClientPooled(clientName);
        int timeoutMs = properties.getTimeoutMsForClient(clientName);
        log.info("Creating pooled RPC channel for client {}", clientName);
        return new PooledKafkaRpcChannel(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs);
    }

    @PreDestroy
    public void closeAll() {
        channels.forEach((name, ch) -> {
            try {
                ch.close();
            } catch (Exception e) {
                log.warn("Error closing channel for {}: {}", name, e.getMessage());
            }
        });
        channels.clear();
    }
}
