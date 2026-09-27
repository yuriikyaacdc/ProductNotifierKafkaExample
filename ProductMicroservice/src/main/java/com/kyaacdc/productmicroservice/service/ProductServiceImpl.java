package com.kyaacdc.productmicroservice.service;

import com.kyaacdc.core.ProductCreatedEvent;
import com.kyaacdc.productmicroservice.service.dto.CreateProductDto;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class ProductServiceImpl implements ProductService{

    private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductDto createProductDto) throws ExecutionException, InterruptedException {

        String productId = UUID.randomUUID().toString();
        //String productId = "qwert";
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId, createProductDto.getTitle(),
                createProductDto.getPrice(), createProductDto.getQuantity());

//        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate.send("product-created-event-topic", productId, productCreatedEvent);
//        future.whenComplete((result, exception) -> {
//           if(exception == null) {
//               LOGGER.info("Message sent {}", result.getRecordMetadata());
//           } else {
//               LOGGER.error("Failed to send meggage {}", exception.getMessage());
//           }
//        });

        ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>("product-created-event-topic", productId, productCreatedEvent);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());
        //record.headers().add("messageId", "qwert".getBytes());
        SendResult<String, ProductCreatedEvent> result = kafkaTemplate.send(record).get();



        RecordMetadata recordMetadata = result.getRecordMetadata();

        LOGGER.info("Topic {}", recordMetadata.topic());
        LOGGER.info("Partition {}", recordMetadata.partition());
        LOGGER.info("Offset {}", recordMetadata.offset());
        LOGGER.info("Result {}", productId);

        return productId;
    }
}
