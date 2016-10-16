package com.aspectcapital.questiontwo.price;

import com.aspectcapital.questiontwo.price.processor.RewritingPriceProcessor;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class);
    private static final int MAX_PRICE = 10000;
    private static final int NUMBER_OF_ENTITIES = 10;
    private static final Random random = new Random();

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(100);
    private static final PriceHolder priceHolder = new PriceHolder(new RewritingPriceProcessor());

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(Thread.currentThread()));

        scheduler.scheduleAtFixedRate(() -> priceHolder.putPrice(getEntityName(), BigDecimal.valueOf(random.nextInt(MAX_PRICE))), 1, 1, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(() -> {
            String entityName = getEntityName();
            System.out.println(String.format("Entity: %s, price: %f", entityName, priceHolder.getPrice(entityName)));
        }, 500, 500, TimeUnit.MILLISECONDS);
    }

    private static String getEntityName() {
        return String.valueOf(random.nextInt(NUMBER_OF_ENTITIES));
    }

    static class ShutdownHook extends Thread {
        private Thread thread;

        public ShutdownHook(Thread thread) {
            this.thread = thread;
        }

        public void run() {
            try {
                priceHolder.stopProcessing();
                scheduler.shutdown();
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.debug("Scheduled executor service cannot be terminated gracefully");
                    throw new RuntimeException();
                }

                thread.join();
                logger.debug("Application terminated.");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
