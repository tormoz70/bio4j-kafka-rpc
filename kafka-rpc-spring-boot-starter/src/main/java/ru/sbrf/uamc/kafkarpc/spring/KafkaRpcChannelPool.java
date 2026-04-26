package ru.sbrf.uamc.kafkarpc.spring;

import ru.sbrf.uamc.kafkarpc.KafkaRpcChannel;
import ru.sbrf.uamc.kafkarpc.KafkaRpcLogEvents;
import ru.sbrf.uamc.kafkarpc.PooledKafkaRpcChannel;
import jakarta.annotation.PreDestroy;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Pool of shared RPC channels by client name. One PooledKafkaRpcChannel per client (one producer + one consumer thread).
 * Used by generated *ChannelProvider beans. Registered as bean via KafkaRpcChannelPoolAutoConfiguration.
 */
@Slf4j
public class KafkaRpcChannelPool {
    @FunctionalInterface
    interface ChannelFactory {
        PooledKafkaRpcChannel create(String clientName) throws InterruptedException;
    }

    private final KafkaRpcProperties properties;
    private final MeterRegistry meterRegistry;
    private final ChannelFactory channelFactory;
    private final Map<String, PooledKafkaRpcChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, Long> lastAccessMs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService idleEvictionExecutor;

    public KafkaRpcChannelPool(KafkaRpcProperties properties) {
        this(properties, null);
    }

    public KafkaRpcChannelPool(KafkaRpcProperties properties, MeterRegistry meterRegistry) {
        this(properties, meterRegistry, null);
    }

    KafkaRpcChannelPool(KafkaRpcProperties properties, MeterRegistry meterRegistry, ChannelFactory channelFactory) {
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.channelFactory = channelFactory != null ? channelFactory : this::createChannel;
        this.idleEvictionExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "kafka-rpc-channel-pool-evictor");
            thread.setDaemon(true);
            return thread;
        });
        registerMetrics();
        startIdleEvictionIfEnabled();
    }

    /**
     * Returns the shared channel for the client. Creates it on first use (lazy).
     *
     * @param clientName key from kafka-rpc.clients (e.g. greeter, echo)
     */
    public KafkaRpcChannel getOrCreate(String clientName) {
        PooledKafkaRpcChannel existing = channels.get(clientName);
        if (existing != null) {
            touch(clientName);
            return existing;
        }
        synchronized (this) {
            PooledKafkaRpcChannel recheck = channels.get(clientName);
            if (recheck != null) {
                touch(clientName);
                return recheck;
            }
            if (channels.size() >= properties.getChannelPoolMaxSize()) {
                throw new IllegalStateException(
                        "Kafka RPC channel pool limit exceeded: maxSize=" + properties.getChannelPoolMaxSize());
            }
            try {
                PooledKafkaRpcChannel created = channelFactory.create(clientName);
                channels.put(clientName, created);
                touch(clientName);
                return created;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while creating Kafka RPC channel for client=" + clientName, e);
            }
        }
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
        int streamHealthcheckMaxFailures = properties.getStreamHealthcheckMaxFailuresForClient(clientName);
        long streamServerIdleTimeoutMs = properties.getStreamServerIdleTimeoutMsForClient(clientName);
        int consumerCount = properties.getClientConsumerCountForClient(clientName);
        int pollIntervalMs = properties.getPollIntervalMsForClient(clientName);
        int streamBufferSize = properties.getStreamBufferSizeForClient(clientName);
        String consumerGroup = consumerConfig.getProperty("group.id", "<undefined>");
        log.info("{} role=client client={} requestTopic={} replyTopic={} group={} consumerCount={}",
                KafkaRpcLogEvents.CHANNEL_CREATED, clientName, requestTopic, replyTopic, consumerGroup, consumerCount);
        return new PooledKafkaRpcChannel(producerConfig, consumerConfig, requestTopic, replyTopic, timeoutMs,
                streamHealthcheckEnabled, streamHealthcheckIntervalMs, streamHealthcheckTimeoutMs, streamHealthcheckMaxFailures, streamServerIdleTimeoutMs,
                consumerCount, pollIntervalMs, streamBufferSize);
    }

    @PreDestroy
    public void closeAll() {
        idleEvictionExecutor.shutdownNow();
        channels.forEach((name, ch) -> {
            try {
                ch.close();
            } catch (Exception e) {
                log.warn("{} role=client client={} error={}", KafkaRpcLogEvents.CHANNEL_CLOSE_FAILED, name, e.getMessage());
            }
        });
        channels.clear();
        lastAccessMs.clear();
    }

    private void touch(String clientName) {
        lastAccessMs.put(clientName, System.currentTimeMillis());
    }

    private void startIdleEvictionIfEnabled() {
        long idleTimeoutMs = properties.getChannelPoolIdleTimeoutMs();
        if (idleTimeoutMs <= 0) {
            return;
        }
        long intervalMs = Math.max(1_000L, properties.getChannelPoolCleanupIntervalMs());
        idleEvictionExecutor.scheduleAtFixedRate(this::evictIdleChannels, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void evictIdleChannels() {
        long idleTimeoutMs = properties.getChannelPoolIdleTimeoutMs();
        if (idleTimeoutMs <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        channels.forEach((client, channel) -> {
            Long lastAccess = lastAccessMs.get(client);
            if (lastAccess != null && now - lastAccess >= idleTimeoutMs) {
                if (channels.remove(client, channel)) {
                    lastAccessMs.remove(client);
                    try {
                        channel.close();
                    } catch (Exception e) {
                        log.warn("{} role=client client={} error={}",
                                KafkaRpcLogEvents.CHANNEL_CLOSE_FAILED, client, e.getMessage());
                    }
                }
            }
        });
    }

    private void registerMetrics() {
        if (meterRegistry == null) {
            return;
        }
        Gauge.builder("kafka.rpc.pool.size", channels, Map::size)
                .description("Current number of pooled Kafka RPC channels")
                .register(meterRegistry);
    }
}
