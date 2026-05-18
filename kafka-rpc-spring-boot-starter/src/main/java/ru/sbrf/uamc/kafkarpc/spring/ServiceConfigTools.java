package ru.sbrf.uamc.kafkarpc.spring;

public final class ServiceConfigTools {
    private ServiceConfigTools() {
    }

    public static String resolveRequestTopic(KafkaRpcProperties properties, String serviceName) {
        KafkaRpcProperties.Service cfg = properties.getServiceConfig(serviceName);
        if (cfg == null) {
            cfg = new KafkaRpcProperties.Service();
        }
        String topic = cfg.getRequestTopic();
        if (topic == null || topic.isBlank()) {
            throw new IllegalStateException("kafka-rpc.service." + serviceName + ".request-topic must be set");
        }
        return topic;
    }
}
