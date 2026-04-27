package com.pravin.kafka.component;

import com.pravin.kafka.dto.ProductPrice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
@DependsOn("broker")
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "product-price-changes", groupId = "pravin")
    public void receive(ProductPrice productPrice) {
        LOGGER.info("Received in KafkaConsumer: {}", productPrice);
    }
}