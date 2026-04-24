package com.pravin.kafka.service;

import com.pravin.kafka.dto.ProductPrice;
import com.pravin.kafka.entity.Product;
import com.pravin.kafka.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductService {
    private static final Logger log = LoggerFactory.getLogger(ProductService.class);

    private final ProductRepository productRepository;

    public ProductService(ProductRepository productRepository){
        this.productRepository = productRepository;
    }

    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }

    public void updateProductPrice(ProductPrice productPrice){
        productRepository.updateProductPrice(productPrice.productCode(), productPrice.price());
    }
}
