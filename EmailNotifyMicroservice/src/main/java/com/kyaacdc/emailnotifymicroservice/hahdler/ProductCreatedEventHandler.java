package com.kyaacdc.emailnotifymicroservice.hahdler;

import com.kyaacdc.core.ProductCreatedEvent;
import com.kyaacdc.emailnotifymicroservice.exception.NonRetryableException;
import com.kyaacdc.emailnotifymicroservice.exception.RetryableException;
import com.kyaacdc.emailnotifymicroservice.persistence.ProcessedEventEntity;
import com.kyaacdc.emailnotifymicroservice.persistence.ProcessedEventRepository;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-event-topic")
public class ProductCreatedEventHandler {

    private Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private final RestTemplate restTemplate;
    private final ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaHandler
    @Transactional
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {

        LOGGER.info("Received event: {}", productCreatedEvent.getTitle());

        ProcessedEventEntity processedEventEntity = processedEventRepository.findByMessageId(messageId);

        if(processedEventEntity != null) {
            LOGGER.info("Duplicated messageId {}", messageId);
            return;
        }

        String url = "http://localhost:8090/response/200";

        try {
            ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            if(responseEntity.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received 200 response - {}", responseEntity.getBody());
            } else {
                LOGGER.info("Received response - {}", responseEntity.getBody());
            }
        } catch (ResourceNotFoundException | ResourceAccessException e) {
            LOGGER.info(e.getMessage());
            throw new RetryableException(e);
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
            throw new NonRetryableException(e);
        }


        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
        }catch (DataIntegrityViolationException e) {
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        }

    }
}
