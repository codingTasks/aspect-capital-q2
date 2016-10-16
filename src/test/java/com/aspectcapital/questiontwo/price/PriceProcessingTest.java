package com.aspectcapital.questiontwo.price;

import com.aspectcapital.questiontwo.price.processor.RewritingPriceProcessor;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PriceProcessingTest {
    private static final int MAX_PRICE = 10000;
    private static final int NUMBER_OF_ENTITIES = 1000;
    private static final int NUMBER_OF_PRICES = 1000;
    private static final int NUMBER_OF_PUT_THREADS = 10;
    private static final int NUMBER_OF_GET_THREADS = 10;
    private static final ExecutorService pool = Executors.newCachedThreadPool();
    private final Random random = new Random();
    private final CyclicBarrier barrier = new CyclicBarrier(NUMBER_OF_PUT_THREADS + NUMBER_OF_GET_THREADS + 1);
    private final PriceHolder priceHolder = new PriceHolder(new RewritingPriceProcessor());

    @Test
    public void testMultiplePutAndGetThreads() throws Exception {
        try {
            for (int i = 0; i < NUMBER_OF_PUT_THREADS; i++) {
                pool.execute(new PutThread());
            }

            for (int i = 0; i < NUMBER_OF_GET_THREADS; i++) {
                pool.execute(new GetThread());
            }

            barrier.await();
            barrier.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class PutThread implements Runnable {
        public void run() {
            try {
                barrier.await();
                for (int i = NUMBER_OF_PRICES; i > 0; --i) {
                    priceHolder.putPrice(String.valueOf(random.nextInt(NUMBER_OF_ENTITIES)), BigDecimal.valueOf(random.nextInt(MAX_PRICE)));
                }
                barrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class GetThread implements Runnable {
        public void run() {
            try {
                barrier.await();
                for (int i = NUMBER_OF_PRICES; i > 0; --i) {
                    try {
                        priceHolder.getPrice(String.valueOf(random.nextInt(NUMBER_OF_ENTITIES)));
                    } catch (Exception ignored) {
                    }
                }
                barrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
