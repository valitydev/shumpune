package com.rbkmoney.shumpune.utils;

import com.rbkmoney.damsel.shumpune.VectorClock;
import org.junit.Assert;
import org.junit.Test;

public class VectorClockSerializerTest {

    public static final long CLOCK_TEST = 123L;

    @Test
    public void serialize() {
        VectorClock clock = VectorClockSerializer.serialize(CLOCK_TEST);

        Assert.assertNotNull(clock);

        long id = VectorClockSerializer.deserialize(clock);

        Assert.assertEquals(CLOCK_TEST, id);
    }
}