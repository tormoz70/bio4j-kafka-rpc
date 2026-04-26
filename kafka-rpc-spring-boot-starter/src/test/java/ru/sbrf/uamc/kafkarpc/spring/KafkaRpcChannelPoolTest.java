package ru.sbrf.uamc.kafkarpc.spring;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.sbrf.uamc.kafkarpc.PooledKafkaRpcChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class KafkaRpcChannelPoolTest {

    private KafkaRpcChannelPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.closeAll();
        }
    }

    @Test
    void enforcesPoolMaxSizeForDifferentClients() {
        KafkaRpcProperties properties = new KafkaRpcProperties();
        properties.setChannelPoolMaxSize(1);

        PooledKafkaRpcChannel first = mock(PooledKafkaRpcChannel.class);
        PooledKafkaRpcChannel reused = first;
        pool = new KafkaRpcChannelPool(properties, null, clientName -> first);

        assertSame(first, pool.getOrCreate("client-a"));
        assertSame(reused, pool.getOrCreate("client-a"));
        assertThrows(IllegalStateException.class, () -> pool.getOrCreate("client-b"));
    }

    @Test
    void evictsIdleChannelsWhenTimeoutConfigured() {
        KafkaRpcProperties properties = new KafkaRpcProperties();
        properties.setChannelPoolMaxSize(8);
        properties.setChannelPoolIdleTimeoutMs(50);
        properties.setChannelPoolCleanupIntervalMs(20);

        AtomicInteger created = new AtomicInteger();
        Map<Integer, PooledKafkaRpcChannel> createdChannels = new ConcurrentHashMap<>();
        pool = new KafkaRpcChannelPool(properties, null, clientName -> {
            int n = created.incrementAndGet();
            PooledKafkaRpcChannel channel = mock(PooledKafkaRpcChannel.class);
            createdChannels.put(n, channel);
            return channel;
        });

        PooledKafkaRpcChannel first = (PooledKafkaRpcChannel) pool.getOrCreate("client-a");
        verify(first, timeout(500).times(1)).close();

        PooledKafkaRpcChannel second = (PooledKafkaRpcChannel) pool.getOrCreate("client-a");
        assertSame(createdChannels.get(2), second);
    }
}
