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

    

    /** Number of consumer threads for server (request topic). Enables scaling via partitioning. Default 1. */
    private int serverConsumerCount = 1;

    /** Default request timeout in milliseconds (client). */
    private int timeoutMs = 30_000;

    /** Default stream healthcheck interval (client, ms). Overridable per client. */
    private Integer streamHealthcheckIntervalMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_INTERVAL_MS;
    /** Default stream healthcheck timeout (client, ms). Overridable per client. */
    private Integer streamHealthcheckTimeoutMs = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_TIMEOUT_MS;
    /** Default stream healthcheck max consecutive failures before stream is marked dead. */
    private Integer streamHealthcheckMaxFailures = KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_MAX_FAILURES;
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
    /** Maximum number of pooled channels created lazily by {@link KafkaRpcChannelPool}. */
    private int channelPoolMaxSize = 128;
    /** Idle channel eviction timeout in ms. 0 disables eviction. */
    private long channelPoolIdleTimeoutMs = 0;
    /** Background cleanup interval for pool eviction checks in ms. */
    private long channelPoolCleanupIntervalMs = 30_000;

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
     * Build consumer Properties for a pooled client channel.
     * Uses client.group-id if set, otherwise defaults to reply-topic + "-group".
     */
    public Properties getConsumerPropertiesForClientPooled(String clientName) {
        var p = baseConsumerProperties(false);
        Client client = null;
        if (clientName != null && clients != null) {
            client = clients.get(clientName);
            if (client != null && client.getConsumer() != null) {
                client.getConsumer().forEach(p::setProperty);
            }
        }
        String groupId;
        if (client != null && client.getGroupId() != null && !client.getGroupId().isBlank()) {
            groupId = client.getGroupId();
        } else if (client != null && client.getReplyTopic() != null && !client.getReplyTopic().isBlank()) {
            groupId = client.getReplyTopic() + "-group";
        } else {
            groupId = clientName + "-group";
        }
        p.setProperty("group.id", groupId);
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
     * Uses service.group-id if set, otherwise defaults to request-topic + "-group".
     *
     * @param serviceName  key from kafka-rpc.service (e.g. greeter)
     * @param requestTopic Kafka topic the server consumes from; used for default group.id
     */
    public Properties getConsumerPropertiesForServer(String serviceName, String requestTopic) {
        var p = baseConsumerProperties(true);
        Service svc = null;
        if (serviceName != null && service != null) {
            svc = service.get(serviceName);
            if (svc != null && svc.getConsumer() != null) {
                svc.getConsumer().forEach(p::setProperty);
            }
        }
        String groupId = (svc != null && svc.getGroupId() != null && !svc.getGroupId().isBlank())
                ? svc.getGroupId()
                : requestTopic + "-group";
        p.setProperty("group.id", groupId);
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
        p.put("max.request.size", Integer.toString(KafkaRpcConstants.DEFAULT_MAX_MESSAGE_SIZE_BYTES));
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
        p.put("max.partition.fetch.bytes", Integer.toString(KafkaRpcConstants.DEFAULT_MAX_MESSAGE_SIZE_BYTES));
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

    /** Max consecutive failed healthchecks for this client. Uses client override or global default. */
    public int getStreamHealthcheckMaxFailuresForClient(String clientName) {
        if (clientName != null && clients != null) {
            var c = clients.get(clientName);
            if (c != null && c.getStreamHealthcheckMaxFailures() != null) {
                return Math.max(1, c.getStreamHealthcheckMaxFailures());
            }
        }
        return streamHealthcheckMaxFailures != null
                ? Math.max(1, streamHealthcheckMaxFailures)
                : KafkaRpcConstants.DEFAULT_STREAM_HEALTHCHECK_MAX_FAILURES;
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
        /** Explicit consumer group ID for this client. Default: &lt;clientName&gt;-group. */
        private String groupId;
        /** Override global timeout for this client (ms). */
        private Integer timeoutMs;
        /** When false, stream RPCs do not send healthcheck (for testing: server will cancel after idle timeout). Default true. */
        private Boolean streamHealthcheckEnabled = true;
        /** Stream healthcheck interval (ms). Override global kafka-rpc.stream-healthcheck-interval-ms. */
        private Integer streamHealthcheckIntervalMs;
        /** Stream healthcheck timeout (ms). Override global kafka-rpc.stream-healthcheck-timeout-ms. */
        private Integer streamHealthcheckTimeoutMs;
        /** Max consecutive failed healthchecks before the stream is marked dead. Override global value. */
        private Integer streamHealthcheckMaxFailures;
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
        /** Explicit consumer group ID for this service. Default: &lt;request-topic&gt;-group. */
        private String groupId;
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
