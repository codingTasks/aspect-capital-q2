package com.aspectcapital.questiontwo.price.processor;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class TestPriceProcessorFactory {
    public static DelayingPriceProcessor getPriceProcessor(int numberOfPutThreads, int processSleepMilliseconds, CountDownLatch latch, BigDecimal lastPrice) {
        return numberOfPutThreads == 1 ? new CountingOnePriceValuePriceProcessor(processSleepMilliseconds, latch, lastPrice) : new CountingAllInvocationsPriceProcessor(processSleepMilliseconds, latch);
    }
}
