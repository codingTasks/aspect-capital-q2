package com.aspectcapital.questiontwo.price.processor;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class CountingAllInvocationsPriceProcessor extends DelayingPriceProcessor {
    public CountingAllInvocationsPriceProcessor(int processSleep, CountDownLatch latch) {
        super(processSleep);
        this.latch = latch;
    }

    @Override
    public BigDecimal process(BigDecimal price) {
        BigDecimal processed = super.process(price);

        if (latch != null) {
            latch.countDown();
        }

        return processed;
    }
}
