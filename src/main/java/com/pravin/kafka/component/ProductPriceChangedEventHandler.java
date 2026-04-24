package com.pravin.kafka.component;

import com.google.gson.Gson;
import com.pravin.kafka.dto.ProductPrice;
import com.pravin.kafka.repository.ProductRepository;
import com.pravin.kafka.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional
@DependsOn("broker")
class ProductPriceChangedEventHandler {

    private static final Logger log = LoggerFactory.getLogger(ProductPriceChangedEventHandler.class);

    private final ProductService productService;

    ProductPriceChangedEventHandler(ProductService productService) {
        this.productService = productService;
    }

    @KafkaListener(topics = "product-price-changes", groupId = "product")
    public void handle(String event) {
        log.info("Received a ProductPriceChangedEvent with productCode:{}: ", event);
        Gson gson = new Gson();
        ProductPrice productPrice = gson.fromJson(event, ProductPrice.class);
        productService.updateProductPrice(productPrice);
    }
}