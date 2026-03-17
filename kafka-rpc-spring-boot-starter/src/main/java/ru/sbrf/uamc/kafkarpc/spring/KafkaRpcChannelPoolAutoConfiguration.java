package ru.sbrf.uamc.kafkarpc.spring;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaRpcProperties.class)
public class KafkaRpcChannelPoolAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public KafkaRpcChannelPool kafkaRpcChannelPool(KafkaRpcProperties properties) {
        return new KafkaRpcChannelPool(properties);
    }
}
