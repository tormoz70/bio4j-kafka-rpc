package ru.sbrf.uamc.kafkarpc.spring;

import ru.sbrf.uamc.kafkarpc.KafkaRpcChannel;
import ru.sbrf.uamc.kafkarpc.KafkaRpcLogEvents;
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
        return channels.computeIfAbsent(clientName, c -> {
            try {
                return createChannel(c);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while creating Kafka RPC channel for client=" + c, e);
            }
        });
    }

    private PooledKafkaRpcChannel createChannel(String clientName) throws InterruptedException {
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
        String consumerGroup = consumerConfig.getProperty("group.id", "<undefined>");
        log.info("{} role=client client={} requestTopic={} replyTopic={} group={} consumerCount={}",
                KafkaRpcLogEvents.CHANNEL_CREATED, clientName, requestTopic, replyTopic, consumerGroup, consumerCount);
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
                log.warn("{} role=client client={} error={}", KafkaRpcLogEvents.CHANNEL_CLOSE_FAILED, name, e.getMessage());
            }
        });
        channels.clear();
    }
}
