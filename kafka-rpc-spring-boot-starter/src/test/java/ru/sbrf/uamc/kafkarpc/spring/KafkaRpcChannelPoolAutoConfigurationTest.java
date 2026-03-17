package ru.sbrf.uamc.kafkarpc.spring;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertSame;

class KafkaRpcChannelPoolAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaRpcChannelPoolAutoConfiguration.class))
            .withPropertyValues(
                    "kafka-rpc.bootstrap-servers=localhost:9092",
                    "kafka-rpc.clients.greeter.request-topic=greeter.request",
                    "kafka-rpc.clients.greeter.reply-topic=greeter.reply"
            );

    @Test
    void createsPoolBeanWhenMissing() {
        contextRunner.run(context -> {
            var bean = context.getBean(KafkaRpcChannelPool.class);
            assertSame(bean, context.getBean("kafkaRpcChannelPool"));
        });
    }

    @Test
    void respectsUserDefinedPoolBean() {
        contextRunner.withUserConfiguration(CustomPoolConfig.class).run(context -> {
            var bean = context.getBean(KafkaRpcChannelPool.class);
            assertSame(bean, context.getBean("customPool"));
        });
    }

    @Configuration
    static class CustomPoolConfig {
        @Bean
        KafkaRpcChannelPool customPool(KafkaRpcProperties properties) {
            return new KafkaRpcChannelPool(properties);
        }
    }
}
