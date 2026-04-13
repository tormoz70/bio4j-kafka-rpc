package ru.sbrf.uamc.kafkarpc.spring;

import ru.sbrf.uamc.kafkarpc.KafkaRpcServer;
import ru.sbrf.uamc.kafkarpc.KafkaRpcService;
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
            for (KafkaRpcService service : services) {
                String serviceName = service.getServiceName();
                String requestTopic = ServiceConfigTools.resolveRequestTopic(properties, serviceName);
                var consumerConfig = properties.getConsumerPropertiesForServer(serviceName, requestTopic);
                var producerConfig = properties.getProducerPropertiesForServer(serviceName);
                int consumerCount = properties.getServerConsumerCountForService(serviceName);
                int pollIntervalMs = properties.getPollIntervalMsForService(serviceName);

                var server = new KafkaRpcServer(
                        consumerConfig, producerConfig,
                        requestTopic,
                        service.getHandlers(),
                        service.getStreamHandlers(),
                        consumerCount,
                        pollIntervalMs);
                servers.add(server);
                server.start();
                log.info("Started Kafka RPC server for {} (service {})", requestTopic, serviceName);
            }
        }

        @PreDestroy
        public void stop() {
            servers.forEach(KafkaRpcServer::close);
        }
    }
}
