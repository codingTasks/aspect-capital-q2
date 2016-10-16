package com.aspectcapital.questiontwo.price;

import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class Entity {
    private final static Logger logger = Logger.getLogger(Entity.class);

    private final String name;
    private final ReentrantLock priceChangeLock = new ReentrantLock();
    private BigDecimal price;
    private BigDecimal nextPriceToProcess;
    private AtomicBoolean inProcessing = new AtomicBoolean();
    private ThreadLocal<BigDecimal> lastPriceRead = new ThreadLocal<>();

    Entity(String name) {
        this.name = name;
    }

    void setLastPriceRead(BigDecimal lastPriceRead) {
        synchronized (priceChangeLock) {
            this.lastPriceRead.set(lastPriceRead);
            logger.debug(String.format("[LAST PRICE READ SET] %s", this));
        }
    }

    public BigDecimal getPrice() {
        BigDecimal toReturn;

        synchronized (priceChangeLock) {
            toReturn = price;
            setLastPriceRead(toReturn);
        }

        return toReturn;
    }

    public synchronized void setPrice(BigDecimal price) {
        synchronized (priceChangeLock) {
            this.price = price;
        }
    }

    public BigDecimal getNextPriceToProcess() {
        return nextPriceToProcess;
    }

    public void setNextPriceToProcess(BigDecimal next) {
        nextPriceToProcess = next;
    }

    @Override
    public String toString() {
        return "Entity{" + super.toString() +
                " entityName='" + name + '\'' +
                ", price=" + price +
                ", nextPriceToProcess=" + nextPriceToProcess +
                ", inProcessing=" + inProcessing +
                '}';
    }

    public boolean isInProcessing() {
        return inProcessing.get();
    }

    public void setInProcessing(boolean inProcessing) {
        synchronized (priceChangeLock) {
            this.inProcessing.set(inProcessing);
            logger.debug(String.format("[IN PROCESSING]: %s, for: %s", inProcessing, this));
        }
    }

    public BigDecimal getLastPriceRead() {
        return lastPriceRead.get();
    }

    boolean hasPriceChanged() {
        return nullSafeIsEqual(price, lastPriceRead.get());
    }

    public boolean hasPriceToProcess() {
        return nullSafeIsEqual(price, nextPriceToProcess);
    }

    private boolean nullSafeIsEqual(BigDecimal price1, BigDecimal price2) {
        boolean priceChanged = true;

        if(price1 != null) {
            if(price2 != null)
                priceChanged = price1.compareTo(price2) != 0;
        }
        else
            priceChanged = price2 != null;

        return priceChanged;
    }
}
