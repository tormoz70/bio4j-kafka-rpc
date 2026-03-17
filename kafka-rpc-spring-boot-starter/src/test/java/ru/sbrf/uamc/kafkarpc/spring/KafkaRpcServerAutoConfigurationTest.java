package ru.sbrf.uamc.kafkarpc.spring;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.sbrf.uamc.kafkarpc.KafkaRpcServer;
import ru.sbrf.uamc.kafkarpc.KafkaRpcService;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaRpcServerAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaRpcServerAutoConfiguration.class))
            .withPropertyValues(
                    "kafka-rpc.bootstrap-servers=localhost:9092",
                    "kafka-rpc.service.greeter.request-topic=greeter.request"
            );

    @Test
    void createsLifecycleWhenServiceBeanExists() {
        contextRunner.withUserConfiguration(TestServiceConfig.class).run(context ->
                assertTrue(context.containsBean("kafkaRpcServerLifecycle")));
    }

    @Test
    void doesNotCreateLifecycleWhenServerDisabled() {
        contextRunner
                .withUserConfiguration(TestServiceConfig.class)
                .withPropertyValues("kafka-rpc.server-enabled=false")
                .run(context -> assertTrue(!context.containsBean("kafkaRpcServerLifecycle")));
    }

    @Test
    void doesNotCreateLifecycleWithoutServiceBean() {
        contextRunner.run(context -> assertTrue(!context.containsBean("kafkaRpcServerLifecycle")));
    }

    @Configuration
    static class TestServiceConfig {
        @Bean
        KafkaRpcService testService() {
            return new KafkaRpcService() {
                @Override
                public String getServiceName() {
                    return "greeter";
                }

                @Override
                public String getRequestTopic() {
                    return "greeter.request";
                }

                @Override
                public Map<String, KafkaRpcServer.MethodHandler> getHandlers() {
                    return Map.of("Greeter/GetGreeting", (correlationId, request) -> new byte[0]);
                }
            };
        }
    }
}
