package com.aspectcapital.questiontwo.price.processor;

import java.math.BigDecimal;

public interface PriceProcessor {
    BigDecimal process(BigDecimal price);
}
