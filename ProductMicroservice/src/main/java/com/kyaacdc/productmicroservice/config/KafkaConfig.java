package com.kyaacdc.productmicroservice.config;

import com.kyaacdc.core.ProductCreatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String boostrapServers;
    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;
    @Value("${spring.kafka.producer.acks}")
    private String acks;
    @Value("${spring.kafka.producer.retries}")
    private String retries;
    @Value("${spring.kafka.producer.properties.retry.backoff.ms}")
    private String retryBackoffMs;
    @Value("${spring.kafka.producer.properties.delivery.timeout.ms}")
    private String deliveryTimeoutMs;
    @Value("${spring.kafka.producer.properties.linger.ms}")
    private String lingerMs;
    @Value("${spring.kafka.producer.properties.request.timeout.ms}")
    private String requestTimeoutMs;

    Map<String, Object> producerConfigs() {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.RETRIES_CONFIG, retries);
        config.put(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, retryBackoffMs);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        config.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return config;
    }

    @Bean
    ProducerFactory<String, ProductCreatedEvent> producerFactory() {
        return new DefaultKafkaProducerFactory<String, ProductCreatedEvent>(producerConfigs());
    }

    @Bean
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate(ProducerFactory<String, ProductCreatedEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    NewTopic createTopic() {
        return TopicBuilder.name("product-created-event-topic")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min.insync.replicas", "2"))
                .build();
    }

}
