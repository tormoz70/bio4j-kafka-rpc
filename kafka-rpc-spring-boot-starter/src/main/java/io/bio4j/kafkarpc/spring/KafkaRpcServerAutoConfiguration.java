package io.bio4j.kafkarpc.spring;

import io.bio4j.kafkarpc.KafkaRpcServer;
import io.bio4j.kafkarpc.KafkaRpcService;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Configuration
@EnableConfigurationProperties(KafkaRpcProperties.class)
@ConditionalOnProperty(name = "kafka-rpc.server-enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnBean(KafkaRpcService.class)
@Slf4j
public class KafkaRpcServerAutoConfiguration {

    @Bean
    public KafkaRpcServerLifecycle kafkaRpcServerLifecycle(
            List<KafkaRpcService> services,
            KafkaRpcProperties properties) {
        return new KafkaRpcServerLifecycle(services, properties);
    }

    @RequiredArgsConstructor
    @Slf4j
    public static class KafkaRpcServerLifecycle {
        private final List<KafkaRpcService> services;
        private final KafkaRpcProperties properties;

        private final List<KafkaRpcServer> servers = new ArrayList<>();

        @jakarta.annotation.PostConstruct
        public void start() {
            var consumerConfig = baseConsumerConfig();
            var producerConfig = baseProducerConfig();

            for (KafkaRpcService service : services) {
                var serverConfig = new Properties();
                serverConfig.putAll(consumerConfig);
                serverConfig.put("group.id", properties.getServerGroupId() + "-" + service.getRequestTopic());

                var server = new KafkaRpcServer(
                        serverConfig, baseProducerConfig(),
                        service.getRequestTopic(), service.getReplyTopic(),
                        service.getHandlers());
                servers.add(server);
                server.start();
                log.info("Started Kafka RPC server for {} -> {}", service.getRequestTopic(), service.getReplyTopic());
            }
        }

        @PreDestroy
        public void stop() {
            servers.forEach(KafkaRpcServer::close);
        }

        private Properties baseConsumerConfig() {
            var p = new Properties();
            p.put("bootstrap.servers", properties.getBootstrapServers());
            p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put("auto.offset.reset", "earliest");
            return p;
        }

        private Properties baseProducerConfig() {
            var p = new Properties();
            p.put("bootstrap.servers", properties.getBootstrapServers());
            p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            return p;
        }
    }
}
