package com.pravin.kafka.service;

import com.pravin.kafka.component.KafkaProducer;
import com.pravin.kafka.dto.ProductPrice;
import com.pravin.kafka.entity.Product;
import com.pravin.kafka.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class ProductService {
    private static final Logger log = LoggerFactory.getLogger(ProductService.class);
    private final KafkaProducer kafkaProducer;
    private final ProductRepository productRepository;

    public ProductService(KafkaProducer kafkaProducer, ProductRepository productRepository){
        this.kafkaProducer = kafkaProducer;
        this.productRepository = productRepository;
    }

    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    @Transactional
    public void updateProductPrice(ProductPrice productPrice){
        productRepository.updateProductPrice(productPrice.productCode(), productPrice.price());
        kafkaProducer.send("product-price-changes", productPrice.productCode(), productPrice);
    }
}
