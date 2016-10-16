package com.aspectcapital.questiontwo.price.processor;

import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class CountingOnePriceValuePriceProcessor extends DelayingPriceProcessor {
    public CountingOnePriceValuePriceProcessor(int processSleep, CountDownLatch latch, BigDecimal lastPrice) {
        super(processSleep);
        this.lastPrice = lastPrice;
        this.latch = latch;
    }

    @Override
    public BigDecimal process(BigDecimal price) {
        BigDecimal processed = super.process(price);

        if (latch != null && lastPrice != null && lastPrice.equals(price)) {
            latch.countDown();
        }

        return processed;
    }

}
