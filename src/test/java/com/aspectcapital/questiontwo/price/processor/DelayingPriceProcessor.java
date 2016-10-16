package com.aspectcapital.questiontwo.price.processor;

import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class DelayingPriceProcessor implements PriceProcessor {
    private static final Logger logger = Logger.getLogger(DelayingPriceProcessor.class);

    protected CountDownLatch latch;
    protected int processSleep;
    protected BigDecimal lastPrice;

    public DelayingPriceProcessor(int processSleep) {
        this.processSleep = processSleep;
    }

    @Override
    public BigDecimal process(BigDecimal price) {
        try {
            Thread.sleep(processSleep);
        } catch (InterruptedException e) {
            logger.warn("[INTERRUPTED] Thread interrupted while sleeping");
        }

        return price;
    }
}
