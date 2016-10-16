package com.aspectcapital.questiontwo.price;

import com.aspectcapital.questiontwo.price.processor.PriceProcessor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class PriceHolder {
    private static final Logger logger = Logger.getLogger(PriceHolder.class);
    private static final int DEFAULT_NUMBER_OF_THREADS = 4;
    private static final String processingPriceThreadNameFormat = "price-processing-%d";

    private final Map<String, Entity> entities = new ConcurrentHashMap<>();
    private final PriceProcessor priceProcessor;
    private final BlockingQueue<Entity> entitiesToProcess = new LinkedBlockingQueue<>();
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final int numberOfThreads;
    private ExecutorService processingExecutorService;
    private volatile boolean isRunning;

    public PriceHolder(PriceProcessor priceProcessor) {
        this(priceProcessor, DEFAULT_NUMBER_OF_THREADS);
    }

    public PriceHolder(PriceProcessor priceProcessor, int numberOfThreads) {
        this.priceProcessor = priceProcessor;
        this.numberOfThreads = numberOfThreads;
        startProcessing();
    }

    void startProcessing() {
        synchronized (reentrantLock) {
            if (!isRunning) {
                processingExecutorService = Executors.newFixedThreadPool(numberOfThreads, new ThreadFactoryBuilder()
                        .setNameFormat(processingPriceThreadNameFormat).setDaemon(true).build());
                isRunning = true;
                processPrices();
            }
        }
    }

    public void putPrice(final String entityName, final BigDecimal price) {
        logger.debug(String.format("[RECEIVED] entityName='%s', price=%f", entityName, price));
        checkForNull(entityName);

        Entity entity = getOrCreateEntity(entityName);

        synchronized (entity) {
            entity.setNextPriceToProcess(price);

            if (!entity.isInProcessing()) {
                addToProcessingQueue(entity);
            }
        }
    }

    private void addToProcessingQueue(Entity entity) {
        try {
            entity.setInProcessing(true);
            entitiesToProcess.put(entity);
            logger.debug(String.format("[QUEUED] %s", entity));
        } catch (Exception e) {
            logger.warn(e);
            entity.setInProcessing(false);
        }
    }

    public void processPrices() {
        for (int i = 0; i < numberOfThreads; i++) {
            processingExecutorService.execute(new PriceQueueProcessor());
        }
    }

    public void stopProcessing() {
        logger.warn("Stop processing invoked");

        synchronized (reentrantLock) {
            if (isRunning) {
                isRunning = false;
                shutDownProcessingExecutor();
            }
        }
    }

    private void shutDownProcessingExecutor() {
        processingExecutorService.shutdown();

        try {
            if (!processingExecutorService.awaitTermination(100, TimeUnit.MICROSECONDS)) {
                throw new RuntimeException("Cannot shutdown executor service");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.debug("[STOPPED] Processing prices stopped");
    }

    public BigDecimal getPrice(final String entityName) {
        logger.debug(String.format("[GET PRICE] %s", entityName));
        checkForNull(entityName);

        Entity entity = getEntity(entityName);

        return entity.getPrice();
    }

    public boolean hasPriceChanged(final String entityName) {
        logger.debug(String.format("[HAS PRICE CHANGED] %s", entityName));
        checkForNull(entityName);

        Entity entity = getEntity(entityName);

        return entity.hasPriceChanged();
    }

    private void checkForNull(String entityName) {
        if(entityName == null)
            throw new IllegalArgumentException("Entity name provided is null");
    }

    Entity getOrCreateEntity(String entityName) {
        return entities.computeIfAbsent(entityName, Entity::new);
    }

    private Entity getEntity(String entityName) {
        Entity entity = entities.get(entityName);

        if(entity == null)
            throw new IllegalArgumentException(String.format("Entity: \"%s\" does not exist", entityName));

        return entity;
    }


    private class PriceQueueProcessor implements Runnable {
        @Override
        public void run() {
            while (isRunning) {
                try {
                    Entity entity = processNextPrice();
                    requeueIfNeeded(entity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private Entity processNextPrice() throws InterruptedException {
            Entity entity = entitiesToProcess.take();
            logger.debug(String.format("[TOOK] %s", entity));

            BigDecimal processedPrice = priceProcessor.process(entity.getNextPriceToProcess());
            entity.setPrice(processedPrice);
            logger.debug(String.format("[PROCESSED] %s", entity));
            return entity;
        }

        private void requeueIfNeeded(Entity entity) throws InterruptedException {
            synchronized (entity) {
                if (entity.hasPriceToProcess()) {
                    entitiesToProcess.put(entity);
                    logger.debug(String.format("[REQUEUED] %s", entity));
                } else {
                    entity.setInProcessing(false);
                }
            }
        }

    }
}
