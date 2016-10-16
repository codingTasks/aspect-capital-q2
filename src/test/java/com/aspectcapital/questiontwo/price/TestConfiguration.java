package com.aspectcapital.questiontwo.price;

public class TestConfiguration {
    private int numberOfEntities;
    private int numberOfPrices;
    private int processSleepMilliseconds;
    private int putSleepMilliseconds;
    private int numberOfPutThreads;
    private int numberOfGetThreads;

    private TestConfiguration(Builder builder) {
        this.numberOfEntities = builder.numberOfEntities;
        this.numberOfPrices = builder.numberOfPrices;
        this.processSleepMilliseconds = builder.processSleepMilliseconds;
        this.putSleepMilliseconds = builder.putSleepMilliseconds;
        this.numberOfPutThreads = builder.numberOfPutThreads;
        this.numberOfGetThreads = builder.numberOfGetThreads;
    }

    public int getNumberOfEntities() {
        return numberOfEntities;
    }

    public int getNumberOfPrices() {
        return numberOfPrices;
    }

    public int getProcessSleepMilliseconds() {
        return processSleepMilliseconds;
    }

    public int getPutSleepMilliseconds() {
        return putSleepMilliseconds;
    }

    public int getNumberOfPutThreads() {
        return numberOfPutThreads;
    }

    public int getNumberOfGetThreads() {
        return numberOfGetThreads;
    }


    public static class Builder {
        private int numberOfEntities = 1;
        private int numberOfPrices = 1;
        private int processSleepMilliseconds = 0;
        private int putSleepMilliseconds = 0;
        private int numberOfPutThreads = 1;
        private int numberOfGetThreads = 1;

        public Builder setNumberOfEntities(int numberOfEntities) {
            this.numberOfEntities = numberOfEntities;
            return this;
        }

        public Builder setNumberOfPrices(int numberOfPrices) {
            this.numberOfPrices = numberOfPrices;
            return this;
        }

        public Builder setProcessSleepMilliseconds(int processSleepMilliseconds) {
            this.processSleepMilliseconds = processSleepMilliseconds;
            return this;
        }

        public Builder setPutSleepMilliseconds(int putSleepMilliseconds) {
            this.putSleepMilliseconds = putSleepMilliseconds;
            return this;
        }

        public Builder setNumberOfPutThreads(int numberOfPutThreads) {
            this.numberOfPutThreads = numberOfPutThreads;
            return this;
        }

        public Builder setNumberOfGetThreads(int numberOfGetThreads) {
            this.numberOfGetThreads = numberOfGetThreads;
            return this;
        }

        public TestConfiguration build() {
            return new TestConfiguration(this);
        }
    }
}
