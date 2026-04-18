package ru.sbrf.uamc.kafkarpc.spring;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaRpcPropertiesProducerDefaultsTest {

    @Test
    void producerDefaultsAreApplied() {
        var properties = new KafkaRpcProperties();
        properties.setBootstrapServers("kafka:9092");

        var producer = properties.getProducerPropertiesForServer(null);

        assertEquals("kafka:9092", producer.getProperty("bootstrap.servers"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", producer.getProperty("key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", producer.getProperty("value.serializer"));
        assertEquals("all", producer.getProperty("acks"));
        assertEquals("true", producer.getProperty("enable.idempotence"));
        assertEquals("10", producer.getProperty("retries"));
        assertEquals("5", producer.getProperty("max.in.flight.requests.per.connection"));
        assertEquals("30000", producer.getProperty("request.timeout.ms"));
        assertEquals("120000", producer.getProperty("delivery.timeout.ms"));
        assertEquals("10485760", producer.getProperty("max.request.size"),
                "Default max.request.size should be 10 MiB");
    }

    @Test
    void consumerDefaultsMaxPartitionFetchBytesIs10MiB() {
        var properties = new KafkaRpcProperties();
        properties.setBootstrapServers("kafka:9092");

        var consumer = properties.getConsumerPropertiesForServer("svc", "svc.request");

        assertEquals("10485760", consumer.getProperty("max.partition.fetch.bytes"),
                "Default max.partition.fetch.bytes should be 10 MiB");
    }

    @Test
    void producerMaxRequestSizeCanBeOverriddenGlobally() {
        var properties = new KafkaRpcProperties();
        properties.setBootstrapServers("kafka:9092");
        properties.setProducer(Map.of("max.request.size", "2097152"));

        var producer = properties.getProducerPropertiesForServer(null);

        assertEquals("2097152", producer.getProperty("max.request.size"));
    }

    @Test
    void globalProducerOverridesReplaceDefaults() {
        var properties = new KafkaRpcProperties();
        properties.setBootstrapServers("kafka:9092");
        Map<String, String> producerOverrides = new HashMap<>();
        producerOverrides.put("acks", "1");
        producerOverrides.put("enable.idempotence", "false");
        producerOverrides.put("retries", "2");
        producerOverrides.put("delivery.timeout.ms", "5000");
        properties.setProducer(producerOverrides);

        var producer = properties.getProducerPropertiesForServer(null);

        assertEquals("1", producer.getProperty("acks"));
        assertEquals("false", producer.getProperty("enable.idempotence"));
        assertEquals("2", producer.getProperty("retries"));
        assertEquals("5000", producer.getProperty("delivery.timeout.ms"));
    }

    @Test
    void clientProducerOverridesTakePrecedenceOverGlobal() {
        var properties = new KafkaRpcProperties();
        properties.setBootstrapServers("kafka:9092");
        properties.setProducer(Map.of("acks", "1", "retries", "3"));

        var client = new KafkaRpcProperties.Client();
        client.setProducer(Map.of("acks", "all", "retries", "15"));
        properties.setClients(Map.of("greeter", client));

        var producer = properties.getProducerPropertiesForClient("greeter");

        assertEquals("all", producer.getProperty("acks"));
        assertEquals("15", producer.getProperty("retries"));
    }
}
