package ru.sbrf.uamc.kafkarpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PooledKafkaRpcChannelGroupIdTest {

    @Test
    void stableGroupUsesConfiguredId() {
        assertEquals("my-reply-group", PooledKafkaRpcChannel.resolveChannelConsumerGroupId("my-reply-group", false));
    }

    @Test
    void mutatedGroupAppendsInstSuffix() {
        String effective = PooledKafkaRpcChannel.resolveChannelConsumerGroupId("my-reply-group", true);
        assertTrue(effective.startsWith("my-reply-group-inst-"));
        assertNotEquals(effective, PooledKafkaRpcChannel.resolveChannelConsumerGroupId("my-reply-group", true));
    }
}
