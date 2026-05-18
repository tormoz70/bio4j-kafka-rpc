package ru.sbrf.uamc.kafkarpc;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

@Slf4j
public class KafkaRpcConsumerFactory {

    public static Consumer<String, byte[]> create(Properties cons) {
        val consumer = new KafkaConsumer<String, byte[]>(cons);
        val consumerGroupId = cons.getProperty("group.id", "<undefined>");
        log.debug("Created Kafka consumer for group={}", consumerGroupId);
        return consumer;
    }
}
