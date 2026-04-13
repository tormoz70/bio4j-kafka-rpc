package ru.sbrf.uamc.kafkarpc.spring;

import ru.sbrf.uamc.kafkarpc.KafkaRpcConstants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration properties for Kafka RPC. Uses plain kafka-clients (no spring-kafka).
 * Supports multiple clients and multiple services; each can override global producer/consumer settings.
 */
@Data
@ConfigurationProperties(prefix = "kafka-rpc")
public class KafkaRpcProperties {

    /** Kafka bootstrap servers (global default). */
    private String bootstrapServers = "localhost:9092";

    /** Enable Kafka RPC server (consumes requests). */
    private boolean serverEnabled = true;

    /** Default consumer group ID prefix for server. */
    private String serverGroupId = "kafka-rpc-server";

    /** Number of consumer threads for server (request topic). Enables scaling via partitioning. Default 1. */
    private int serverConsumerCount = 1;

    /** Default request timeout in milliseconds (client). */
    private int timeoutMs = 30_000;

    /** Default stream healthcheck interval (client, ms). Overridable per client. */
    private Integer streamHealthcheckIntervalMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS;
    /** Default stream healthcheck timeout (client, ms). Overridable per client. */
    private Integer streamHealthcheckTimeoutMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS;
    /** Default stream server idle timeout (ms). Client sends in stream request header; overridable per client. */
    private Integer streamServerIdleTimeoutMs = KafkaRpcConstants.DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS;

    /** Kafka consumer poll interval (ms). Lower values reduce latency at the cost of CPU. Default 100ms. */
    private int pollIntervalMs = KafkaRpcConstants.DEFAULT_POLL_INTERVAL_MS;

    /** Maximum buffered stream chunks on the client side. Provides backpressure when the consumer is slow. Default 1024. */
    private int streamBufferSize = KafkaRpcConstants.DEFAULT_STREAM_BUFFER_SIZE;

    /**
     * Per-client config. Key = client name (e.g. greeter). Used by channel pool and generated stub providers.
     * Each client inherits global producer/consumer and can override.
     */
    private Map<String, Client> clients = new HashMap<>();

    /**
     * Per-service (server) config. Key = service name (e.g. greeter). Each service inherits global producer/consumer and can override.
     */
    private Map<String, Service> service = new HashMap<>();

    /**
     * Global Kafka producer overrides. Base for all clients and servers; per-client/per-service maps are applied on top.
     */
    private Map<String, String> producer = new HashMap<>();

    /**
     * Global Kafka consumer overrides. Base for all clients and servers; per-client/per-service maps are applied on top.
     */
    private Map<String, String> consumer = new HashMap<>();

    /** Default number of consumer threads for client (reply topic). Enables scaling via partitioning. Default 1. */
    private int clientConsumerCount = 1;

    /**
     * Build producer Properties for a client: global base + client overrides.
     *
     * @param clientName key from kafka-rpc.clients (e.g. greeter)
     */
    public Properties getProducerPropertiesForClient(String clientName) {
        var p = baseProducerProperties();
        if (clientName != null && clients != null) {
            var client = clients.get(clientName);
            if (client != null && client.getProducer() != null) {
                client.getProducer().forEach(p::setProperty);
            }
        }
        return p;
    }

    /**
     * Build consumer Properties for a client: global base + client overrides. group.id gets unique suffix.
     *
     * @param clientName key from kafka-rpc.clients (e.g. greeter)
     */
    public Properties getConsumerPropertiesForClient(String clientName) {
        var p = baseConsumerProperties(false);
        if (clientName != null && clients != null) {
            var client = clients.get(clientName);
            if (client != null && client.getConsumer() != null) {
                client.getConsumer().forEach(p::setProperty);
            }
        }
        String prefix = p.getProperty("group.id", "kafka-rpc-client");
        p.setProperty("group.id", prefix + "-" + System.currentTimeMillis());
        return p;
    }

    /**
     * Build consumer Properties for a pooled client channel (stable group.id, no unique suffix).
     * Used by KafkaRpcChannelPool so the same consumer group is reused.
     */
    public Properties getConsumerPropertiesForClientPooled(String clientName) {
        var p = baseConsumerProperties(false);
        if (clientName != null && clients != null) {
            var client = clients.get(clientName);
            if (client != null && client.getConsumer() != null) {
                client.getConsumer().forEach(p::setProperty);
            }
        }
        String base = p.getProperty("group.id", "kafka-rpc-client");
        p.setProperty("group.id", base + "-" + clientName);
        return p;
    }

    /**
     * Build producer Properties for a server (service): global base + service overrides.
     *
     * @param serviceName key from kafka-rpc.service (e.g. greeter)
     */
    public Properties getProducerPropertiesForServer(String serviceName) {
        var p = baseProducerProperties();
        if (serviceName != null && service != null) {
            var svc = service.get(serviceName);
            if (svc != null && svc.getProducer() != null) {
                svc.getProducer().forEach(p::setProperty);
            }
        }
        return p;
    }

    /**
     * Build consumer Properties for a server: global base + service overrides + group.id.
     *
     * @param serviceName  key from kafka-rpc.service (e.g. greeter)
     * @param requestTopic Kafka topic the server consumes from; used as group.id suffix
     */
    public Properties getConsumerPropertiesForServer(String serviceName, String requestTopic) {
        var p = baseConsumerProperties(true);
        if (serviceName != null && service != null) {
            var svc = service.get(serviceName);
            if (svc != null && svc.getConsumer() != null) {
                svc.getConsumer().forEach(p::setProperty);
            }
        }
        p.setProperty("group.id", serverGroupId + "-" + requestTopic);
        return p;
    }

    /** Global producer base (bootstrap + default serializers + kafka-rpc.producer). */
    public Properties getProducerProperties() {
        return baseProducerProperties();
    }

    /** Global consumer base. For backward compatibility. Prefer getConsumerPropertiesForClient(name) / getConsumerPropertiesForServer(name, topic). */
    public Properties getConsumerProperties(boolean forServer) {
        var p = baseConsumerProperties(forServer);
        if (!forServer) {
            String prefix = p.getProperty("group.id", "kafka-rpc-client");
            p.setProperty("group.id", prefix + "-" + System.currentTimeMillis());
        }
        return p;
    }

    private Properties baseProducerProperties() {
        var p = new Properties();
        p.put("bootstrap.servers", getBootstrapServers());
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        // Safe producer defaults for RPC traffic; can be overridden by kafka-rpc.producer.*
        p.put("acks", "all");
        p.put("enable.idempotence", "true");
        p.put("retries", "10");
        p.put("max.in.flight.requests.per.connection", "5");
        p.put("request.timeout.ms", "30000");
        p.put("delivery.timeout.ms", "120000");
        if (producer != null) {
            producer.forEach(p::setProperty);
        }
        return p;
    }

    private Properties baseConsumerProperties(boolean forServer) {
        var p = new Properties();
        p.put("bootstrap.servers", getBootstrapServers());
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("auto.offset.reset", forServer ? "earliest" : "latest");
        if (consumer != null) {
            consumer.forEach(p::setProperty);
        }
        return p;
    }

    /** Timeout for a client (ms). Uses client-specific timeout if set, else global timeoutMs. */
    public int getTimeoutMsForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getTimeoutMs() != null) {
                return c.getTimeoutMs();
            }
        }
        return timeoutMs;
    }

    /** Request topic for client (from kafka-rpc.clients.<name>.request-topic). */
    public String getRequestTopicForClient(String clientName) {
        if (clientName == null || clients == null) return null;
        var c = clients.get(clientName);
        return c != null ? c.getRequestTopic() : null;
    }

    /** Reply topic for client (from kafka-rpc.clients.<name>.reply-topic). */
    public String getReplyTopicForClient(String clientName) {
        if (clientName == null || clients == null) return null;
        var c = clients.get(clientName);
        return c != null ? c.getReplyTopic() : null;
    }

    /** Whether stream healthcheck is enabled for this client. Default true. */
    public boolean getStreamHealthcheckEnabledForClient(String clientName) {
        if (clientName == null || clients == null) return true;
        var c = clients.get(clientName);
        return c == null || c.getStreamHealthcheckEnabled() == null || c.getStreamHealthcheckEnabled();
    }

    /** Stream healthcheck interval for this client (ms). Uses client override or global default. */
    public int getStreamHealthcheckIntervalMsForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getStreamHealthcheckIntervalMs() != null) {
                return c.getStreamHealthcheckIntervalMs();
            }
        }
        return streamHealthcheckIntervalMs != null ? streamHealthcheckIntervalMs : KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS;
    }

    /** Stream healthcheck timeout for this client (ms). Uses client override or global default. */
    public int getStreamHealthcheckTimeoutMsForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getStreamHealthcheckTimeoutMs() != null) {
                return c.getStreamHealthcheckTimeoutMs();
            }
        }
        return streamHealthcheckTimeoutMs != null ? streamHealthcheckTimeoutMs : KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS;
    }

    /** Stream server idle timeout for this client (ms). Client sends value in stream request header; uses client override or global default. */
    public long getStreamServerIdleTimeoutMsForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getStreamServerIdleTimeoutMs() != null) {
                return c.getStreamServerIdleTimeoutMs();
            }
        }
        return streamServerIdleTimeoutMs != null ? streamServerIdleTimeoutMs : KafkaRpcConstants.DEFAULT_STREAM_SERVER_IDLE_TIMEOUT_MS;
    }

    /** Number of consumer threads for server (request topic) for this service. Uses service override or global serverConsumerCount. */
    public int getServerConsumerCountForService(String serviceName) {
        if (serviceName != null && service != null) {
            var svc = service.get(serviceName);
            if (svc != null && svc.getConsumerCount() != null) {
                return Math.max(1, svc.getConsumerCount());
            }
        }
        return Math.max(1, serverConsumerCount);
    }

    /** Number of consumer threads for client (reply topic) for this client. Uses client override or global default. */
    public int getClientConsumerCountForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getConsumerCount() != null) {
                return Math.max(1, c.getConsumerCount());
            }
        }
        return Math.max(1, clientConsumerCount);
    }

    /** Poll interval for this service (ms). Uses service override or global default. */
    public int getPollIntervalMsForService(String serviceName) {
        if (serviceName != null && service != null) {
            var svc = service.get(serviceName);
            if (svc != null && svc.getPollIntervalMs() != null) {
                return svc.getPollIntervalMs();
            }
        }
        return pollIntervalMs;
    }

    /** Poll interval for this client (ms). Uses client override or global default. */
    public int getPollIntervalMsForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getPollIntervalMs() != null) {
                return c.getPollIntervalMs();
            }
        }
        return pollIntervalMs;
    }

    /** Stream buffer size for this client. Uses client override or global default. */
    public int getStreamBufferSizeForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getStreamBufferSize() != null) {
                return c.getStreamBufferSize();
            }
        }
        return streamBufferSize;
    }

    @Data
    public static class Client {
        private String requestTopic;
        private String replyTopic;
        /** Override global timeout for this client (ms). */
        private Integer timeoutMs;
        /** When false, stream RPCs do not send healthcheck (for testing: server will cancel after idle timeout). Default true. */
        private Boolean streamHealthcheckEnabled = true;
        /** Stream healthcheck interval (ms). Override global kafka-rpc.stream-healthcheck-interval-ms. */
        private Integer streamHealthcheckIntervalMs;
        /** Stream healthcheck timeout (ms). Override global kafka-rpc.stream-healthcheck-timeout-ms. */
        private Integer streamHealthcheckTimeoutMs;
        /** Stream server idle timeout (ms). Sent in stream request header; server cancels stream after no healthcheck. Override global kafka-rpc.stream-server-idle-timeout-ms. */
        private Integer streamServerIdleTimeoutMs;
        /** Number of consumer threads for reply topic. Override global kafka-rpc.client-consumer-count. */
        private Integer consumerCount;
        /** Consumer poll interval (ms). Override global kafka-rpc.poll-interval-ms. */
        private Integer pollIntervalMs;
        /** Maximum buffered stream chunks. Override global kafka-rpc.stream-buffer-size. */
        private Integer streamBufferSize;
        /** Per-client producer overrides (on top of global kafka-rpc.producer). */
        private Map<String, String> producer = new HashMap<>();
        /** Per-client consumer overrides (on top of global kafka-rpc.consumer). */
        private Map<String, String> consumer = new HashMap<>();
    }

    @Data
    public static class Service {
        private String requestTopic;
        /** Number of consumer threads for request topic. Override global kafka-rpc.server-consumer-count. */
        private Integer consumerCount;
        /** Consumer poll interval (ms). Override global kafka-rpc.poll-interval-ms. */
        private Integer pollIntervalMs;
        /** Per-service producer overrides (on top of global kafka-rpc.producer). */
        private Map<String, String> producer = new HashMap<>();
        /** Per-service consumer overrides (on top of global kafka-rpc.consumer). */
        private Map<String, String> consumer = new HashMap<>();
    }
}
