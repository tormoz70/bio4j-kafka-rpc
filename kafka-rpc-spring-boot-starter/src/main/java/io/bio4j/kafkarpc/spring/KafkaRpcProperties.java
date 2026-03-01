package io.bio4j.kafkarpc.spring;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Kafka RPC. Uses plain kafka-clients (no spring-kafka).
 */
@Data
@ConfigurationProperties(prefix = "kafka-rpc")
public class KafkaRpcProperties {

    /** Kafka bootstrap servers. */
    private String bootstrapServers = "localhost:9092";

    /** Enable Kafka RPC server (consumes requests). */
    private boolean serverEnabled = true;

    /** Default consumer group ID prefix for server. */
    private String serverGroupId = "kafka-rpc-server";

    /** Default request timeout in milliseconds. */
    private int timeoutMs = 30_000;
}
