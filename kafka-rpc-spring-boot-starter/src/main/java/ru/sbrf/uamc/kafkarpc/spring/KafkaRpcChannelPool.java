package ru.sbrf.uamc.kafkarpc.spring;

import ru.sbrf.uamc.kafkarpc.KafkaRpcChannel;
import ru.sbrf.uamc.kafkarpc.PooledKafkaRpcChannel;
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
        boolean streamHealthcheckEnabled = properties.getStreamHealthcheckEnabledForClient(clientName);
        int streamHealthcheckIntervalMs = properties.getStreamHealthcheckIntervalMsForClient(clientName);
        int streamHealthcheckTimeoutMs = properties.getStreamHealthcheckTimeoutMsForClient(clientName);
        long streamServerIdleTimeoutMs = properties.getStreamServerIdleTimeoutMsForClient(clientName);
        int consumerCount = properties.getClientConsumerCountForClient(clientName);
        int pollIntervalMs = properties.getPollIntervalMsForClient(clientName);
        int streamBufferSize = properties.getStreamBufferSizeForClient(clientName);
        log.info("Creating pooled RPC channel for client {} (consumerCount={})", clientName, consumerCount);
        return new PooledKafkaRpcChannel(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs,
                streamHealthcheckEnabled, streamHealthcheckIntervalMs, streamHealthcheckTimeoutMs, streamServerIdleTimeoutMs,
                consumerCount, pollIntervalMs, streamBufferSize);
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
