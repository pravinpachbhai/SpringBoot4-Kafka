package com.pravin.kafka.dto;

import java.math.BigDecimal;

public record ProductPriceChangedEvent(String productCode, BigDecimal price) {}