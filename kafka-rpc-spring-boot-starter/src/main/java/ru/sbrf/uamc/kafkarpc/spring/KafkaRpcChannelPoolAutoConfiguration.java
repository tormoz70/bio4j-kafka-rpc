package ru.sbrf.uamc.kafkarpc.spring;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.micrometer.core.instrument.MeterRegistry;

@Configuration
@EnableConfigurationProperties(KafkaRpcProperties.class)
public class KafkaRpcChannelPoolAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public KafkaRpcChannelPool kafkaRpcChannelPool(
            KafkaRpcProperties properties,
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        return new KafkaRpcChannelPool(properties, meterRegistryProvider.getIfAvailable());
    }
}
