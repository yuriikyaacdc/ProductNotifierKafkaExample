package com.kyaacdc.productmicroservice;

import com.kyaacdc.core.ProductCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class IdempotenceProducerConfigTest {

    @Autowired
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @MockitoBean
    KafkaAdmin kafkaAdmin;

    @Test
    void testProducer_whenIdempotentEnabled_assertIdempotentProps  () {
        // Arrange
        ProducerFactory<String, ProductCreatedEvent> producerFactory = kafkaTemplate.getProducerFactory();

        // Act
        Map<String, Object> configurationProperties = producerFactory.getConfigurationProperties();

        // Assert
        assertEquals(true, configurationProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", configurationProperties.get(ProducerConfig.ACKS_CONFIG));

        if (configurationProperties.containsKey(ProducerConfig.RETRIES_CONFIG)) {
            assertTrue(Integer.parseInt(configurationProperties.get(ProducerConfig.RETRIES_CONFIG).toString()) > 0);
        }

    }

}
