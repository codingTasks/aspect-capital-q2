package com.aspectcapital.questiontwo.price.processor;

import java.math.BigDecimal;

public class RewritingPriceProcessor implements PriceProcessor {
    public BigDecimal process(BigDecimal price) {
        return price;
    }
}
