package com.pravin.kafka.component;

import com.pravin.kafka.dto.ProductPrice;
import com.pravin.kafka.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@DependsOn("broker")
class ProductPriceChangedEventHandler {

    private static final Logger log = LoggerFactory.getLogger(ProductPriceChangedEventHandler.class);
    private final ProductService productService;

    ProductPriceChangedEventHandler(ProductService productService) {
        this.productService = productService;
    }

    @KafkaListener(topics = "product-price-changes", groupId = "product")
    public void handle(ProductPrice productPrice) {
        try {
            log.info("Processing productCode={}", productPrice.productCode());
            productService.updateProductPrice(productPrice);
        } catch (Exception e) {
            log.error("Failed processing productCode={}", productPrice.productCode(), e);
            throw e;
        }
    }
}