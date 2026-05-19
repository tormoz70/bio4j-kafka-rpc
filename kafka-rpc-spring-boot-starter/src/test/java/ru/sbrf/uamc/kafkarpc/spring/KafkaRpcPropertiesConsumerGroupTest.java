package ru.sbrf.uamc.kafkarpc.spring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaRpcPropertiesConsumerGroupTest {

    @Test
    void idleEvictionForcesMutateConsumerGroup() {
        var properties = new KafkaRpcProperties();
        properties.setChannelPoolIdleTimeoutMs(60_000);
        properties.setChannelPoolMutateConsumerGroupOnChannelStart(false);
        assertTrue(properties.resolveMutateConsumerGroupOnChannelStart());
    }

    @Test
    void noIdleEvictionRespectsMutateFlag() {
        var properties = new KafkaRpcProperties();
        properties.setChannelPoolIdleTimeoutMs(0);
        properties.setChannelPoolMutateConsumerGroupOnChannelStart(false);
        assertFalse(properties.resolveMutateConsumerGroupOnChannelStart());

        properties.setChannelPoolMutateConsumerGroupOnChannelStart(true);
        assertTrue(properties.resolveMutateConsumerGroupOnChannelStart());
    }

    @Test
    void defaultMutateWhenIdleEvictionDisabled() {
        var properties = new KafkaRpcProperties();
        properties.setChannelPoolIdleTimeoutMs(0);
        assertTrue(properties.resolveMutateConsumerGroupOnChannelStart());
    }
}
