package ru.sbrf.uamc.kafkarpc;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

final class CommitFailingConsumer extends MockConsumer<String, byte[]> {

    private final AtomicInteger commitAttempts;

    CommitFailingConsumer(AtomicInteger commitAttempts) {
        super(OffsetResetStrategy.EARLIEST);
        this.commitAttempts = commitAttempts;
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (commitAttempts.incrementAndGet() == 1) {
            throw new RuntimeException("simulated commit failure");
        }
        super.commitSync(offsets);
    }
}
