package com.pravin.kafka.component;


import com.pravin.kafka.dto.ProductPrice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
    private final KafkaTemplate<String, ProductPrice> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, ProductPrice> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, String key, ProductPrice productPrice) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        LOGGER.info("sending payload='{}' to topic='{}'", productPrice, topic);
        kafkaTemplate.send(topic, key, productPrice).whenComplete((result, ex) -> {
            if (ex == null) {
                LOGGER.info("Sent to partition={}, offset={}",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                LOGGER.error("Failed to send message to topic='{}'", topic, ex);
            }
        });
    }
}