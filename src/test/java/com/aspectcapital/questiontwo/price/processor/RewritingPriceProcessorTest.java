package com.aspectcapital.questiontwo.price.processor;

import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RewritingPriceProcessorTest {
    private RewritingPriceProcessor rewritingPriceProcessor = new RewritingPriceProcessor();

    @Test
    public void shouldSetPriceOnEntity() throws Exception {
        BigDecimal price = new BigDecimal(10);

        BigDecimal processedPrice = rewritingPriceProcessor.process(price);

        assertThat(processedPrice, is(equalTo(price)));
    }
}