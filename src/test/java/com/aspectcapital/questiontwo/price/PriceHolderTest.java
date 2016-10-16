package com.aspectcapital.questiontwo.price;

import com.aspectcapital.questiontwo.price.processor.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

public class PriceHolderTest {
    private static final String ENTITY_NAME_A = "a";
    private static final String ENTITY_NAME_B = "b";
    private DelayingPriceProcessor priceProcessor;
    private PriceHolder priceHolder;
    private ExecutorService putPriceExecutorService;
    private ExecutorService getPriceExecutorService;

    @After
    public void tearDown() throws Exception {
        if(putPriceExecutorService != null)
            putPriceExecutorService.shutdown();
        if(getPriceExecutorService != null)
            getPriceExecutorService.shutdown();
    }

    @Test
    public void shouldSetLastPriceRedOnGetPriceInvoked() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        priceHolder = new PriceHolder(new CountingAllInvocationsPriceProcessor(0, latch));
        Entity entity = priceHolder.getOrCreateEntity(ENTITY_NAME_A);

        priceHolder.putPrice(ENTITY_NAME_A, new BigDecimal(10));
        latch.await();
        priceHolder.getPrice(ENTITY_NAME_A);

        assertThat(entity.getLastPriceRead(), is(equalTo(entity.getPrice())));
    }

    @Test
    public void nothingShouldHappenWhenStartProcessingCalledMultipleTimes() throws Exception {
        priceHolder = new PriceHolder(mock(RewritingPriceProcessor.class));

        priceHolder.startProcessing();
        priceHolder.startProcessing();
    }

    @Test
    public void shouldReturnLastPriceAvailableForOneEntity() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        BigDecimal lastPrice = new BigDecimal(12);
        priceHolder = new PriceHolder(new CountingOnePriceValuePriceProcessor(0, latch, lastPrice));

        priceHolder.putPrice(ENTITY_NAME_A, new BigDecimal(11));
        priceHolder.putPrice(ENTITY_NAME_A, lastPrice);
        latch.await();

        BigDecimal price = priceHolder.getPrice(ENTITY_NAME_A);
        assertThat(price, is(equalTo(lastPrice)));
    }

    @Test
    public void shouldReturnFalseWhenPriceHasNotChangedSinceLastRetrieval() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        BigDecimal lastPrice = new BigDecimal(10);
        priceHolder = new PriceHolder(new CountingOnePriceValuePriceProcessor(0, latch, lastPrice));

        priceHolder.putPrice(ENTITY_NAME_A, lastPrice);
        latch.await();
        priceHolder.getPrice(ENTITY_NAME_A);

        assertThat(priceHolder.hasPriceChanged(ENTITY_NAME_A), is(false));
    }

    @Test
    public void shouldReturnTrueWhenPriceHasChangedSinceLastRetrieval() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        BigDecimal lastPrice = new BigDecimal(11);
        priceHolder = new PriceHolder(new CountingOnePriceValuePriceProcessor(0, latch, lastPrice));

        priceHolder.getOrCreateEntity(ENTITY_NAME_A);
        priceHolder.getPrice(ENTITY_NAME_A);
        priceHolder.putPrice(ENTITY_NAME_A, lastPrice);
        latch.await();

        assertThat(priceHolder.hasPriceChanged(ENTITY_NAME_A), is(true));
    }

    @Test
    public void shouldReturnPriceWhenOnlyOnePriceWasPutForEachEntityByOneThread() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        BigDecimal lastPrice = new BigDecimal(11);
        priceHolder = new PriceHolder(new CountingOnePriceValuePriceProcessor(0, latch, lastPrice));

        priceHolder.putPrice(ENTITY_NAME_A, lastPrice);
        priceHolder.putPrice(ENTITY_NAME_B, lastPrice);
        latch.await();

        assertThat(priceHolder.getPrice(ENTITY_NAME_A), is(lastPrice));
        assertThat(priceHolder.getPrice(ENTITY_NAME_B), is(lastPrice));
    }

    @Test
    public void shouldReturnPriceWhenOnlyOnePriceWasPutForEachEntityByMultipleThreads() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        BigDecimal lastPrice = new BigDecimal(12);
        priceHolder = new PriceHolder(new CountingOnePriceValuePriceProcessor(0, latch, lastPrice));
        putPriceExecutorService = Executors.newFixedThreadPool(2);
        BigDecimal priceA1 = new BigDecimal(12);

        putPriceInSeparateThread(ENTITY_NAME_A, priceA1);
        putPriceInSeparateThread(ENTITY_NAME_B, lastPrice);
        latch.await();

        assertThat(priceHolder.getPrice(ENTITY_NAME_A), is(equalTo(priceA1)));
        assertThat(priceHolder.getPrice(ENTITY_NAME_B), is(equalTo(lastPrice)));
    }

    @Test
    public void shouldGetLastPriceForMultipleEntitiesAndMultiplePrices() throws InterruptedException, ExecutionException {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfEntities(100)
                .setNumberOfPrices(100)
                .build();

        runLoadTest(tc);
    }

    @Test
    public void shouldGetLastPriceForOneEntityAndMultiplePrices() throws InterruptedException, ExecutionException {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfPrices(100)
                .build();

        runLoadTest(tc);
    }

    @Test
    public void shouldGetLastPriceForMultipleEntitiesAndOnePrice() throws InterruptedException, ExecutionException {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfEntities(100)
                .build();

        runLoadTest(tc);
    }

    @Test
    public void shouldProcessTwoPricesWhenOneEntityManyPricesPutAndLongProcessingTimeOnePutThread() throws Exception {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfPrices(100)
                .setProcessSleepMilliseconds(3000)
                .build();

        runLoadTest(tc);
        verify(priceProcessor, times(2)).process(any());
    }

    @Test
    public void shouldProcessTwoPricesWhenOneEntityManyPricesPutAndLongProcessingTimeMultiplePutThread() throws Exception {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfPrices(100)
                .setProcessSleepMilliseconds(3000)
                .setNumberOfPutThreads(4)
                .build();

        runLoadTest(tc);
        verify(priceProcessor, times(2)).process(any());
    }

    @Test
    public void shouldProcessEveryPriceWhenOneEntityManyPricesPutDelayedMultiplePutThread() throws Exception {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfPrices(100)
                .setPutSleepMilliseconds(50)
                .setNumberOfPutThreads(4)
                .build();

        runLoadTest(tc);

        verify(priceProcessor, times(tc.getNumberOfPrices())).process(any());
    }

    @Test
    public void shouldProcessEveryPriceWhenMultipleEntitiesManyPricesPutDelayedMultiplePutThread() throws Exception {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfEntities(10)
                .setNumberOfPrices(10)
                .setPutSleepMilliseconds(50)
                .setNumberOfPutThreads(4)
                .build();

        runLoadTest(tc);
        verify(priceProcessor, times(tc.getNumberOfPrices() * tc.getNumberOfEntities())).process(any());
    }

    @Test
    public void shouldBeAbleToProcessPricesForMultipleEntitiesAndMultiplePutAndGetThreads() throws Exception {
        TestConfiguration tc = new TestConfiguration.Builder()
                .setNumberOfEntities(100)
                .setNumberOfPutThreads(100)
                .setNumberOfGetThreads(100)
                .build();

        runLoadTest(tc);
    }

    private void runLoadTest(TestConfiguration tc)
            throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(tc.getNumberOfEntities());
        BigDecimal lastPrice = BigDecimal.valueOf(tc.getNumberOfPrices());

        priceProcessor = spy(TestPriceProcessorFactory.getPriceProcessor(tc.getNumberOfPutThreads(), tc.getProcessSleepMilliseconds(), latch, lastPrice));
        priceHolder = spy(new PriceHolder(priceProcessor));

        putPriceExecutorService = Executors.newFixedThreadPool(tc.getNumberOfPutThreads());
        getPriceExecutorService = Executors.newFixedThreadPool(tc.getNumberOfGetThreads());

        for (int i = 0, j = 1; i < tc.getNumberOfPrices() * tc.getNumberOfEntities(); i++) {
            putPrice(tc.getNumberOfEntities(), i, j);

            if (i % tc.getNumberOfEntities() == (tc.getNumberOfEntities() - 1))
                j++;

            Thread.sleep(tc.getPutSleepMilliseconds());
        }

        latch.await();

        verify(priceHolder, times(tc.getNumberOfEntities() * tc.getNumberOfPrices())).putPrice(anyString(), any());

        if(canValidateLastPrices(tc.getNumberOfPrices(), tc.getNumberOfPutThreads())) {
            validateLastEntitiesPrices(tc.getNumberOfEntities(), tc.getNumberOfPrices());
        }
    }

    private boolean canValidateLastPrices(int numberOfPrices, int numberOfPutThreads) {
        return numberOfPutThreads == 1 || numberOfPrices == 1;
    }


    private void validateLastEntitiesPrices(int numberOfEntities, int numberOfPrices) throws InterruptedException, ExecutionException {
        for (int i = 1; i <= numberOfEntities; i++) {
            int finalI = i;
            Future<BigDecimal> price = getPriceInSeparateThread(String.valueOf(finalI));
            assertThat("Entity name: " + String.valueOf(i), price.get().intValue(), is(numberOfPrices));
        }
    }

    private void putPrice(int numberOfEntities, int i, int j) {
        String entityName = String.valueOf(i % numberOfEntities + 1);
        BigDecimal price = BigDecimal.valueOf(j);

        putPriceInSeparateThread(entityName, price);
    }

    private Future<?> putPriceInSeparateThread(String entityName, BigDecimal price) {
        return putPriceExecutorService.submit(() -> priceHolder.putPrice(entityName, price));
    }

    private Future<BigDecimal> getPriceInSeparateThread(String entityName) {
        return getPriceExecutorService.submit(() -> priceHolder.getPrice(entityName));
    }
}
