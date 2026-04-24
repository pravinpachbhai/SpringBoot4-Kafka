package com.pravin.kafka.component;

import com.google.gson.Gson;
import com.pravin.kafka.dto.ProductPrice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    public void receive(String event) {
        Gson gson = new Gson();
        ProductPrice productPrice = gson.fromJson(event, ProductPrice.class);
        LOGGER.info("received payload='{}'", event);
        LOGGER.info("received payload='{}'", productPrice);
    }


}