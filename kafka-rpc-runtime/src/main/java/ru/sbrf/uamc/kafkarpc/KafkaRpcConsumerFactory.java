package ru.sbrf.uamc.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaRpcConsumerFactory {

    public static Consumer<String, byte[]> create(Properties cons, String requestTopic) {
        val consumer = new KafkaConsumer<String, byte[]>(cons);
        val consumerGroupId = cons.getProperty("group.id");
        // При создании консьюмера добавьте логгер-листинер
        consumer.subscribe(Collections.singletonList(requestTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("REBALANCE: partitions revoked: {} group={}", partitions, consumerGroupId);
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("REBALANCE: partitions assigned: {} group={}", partitions, consumerGroupId);
                // Опционально: логировать позиции
                partitions.forEach(tp -> {
                    long pos = consumer.position(tp);
                    log.debug("Partition {} position after assign: {}", tp, pos);
                });
            }
        });
        return consumer;
    }
}
