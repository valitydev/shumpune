package com.rbkmoney.shumpune.utils;

import com.google.common.primitives.Longs;
import com.rbkmoney.damsel.shumpune.VectorClock;

import java.nio.ByteBuffer;

public class VectorClockSerializer {

    public static VectorClock serialize(long clock) {
        return new VectorClock(ByteBuffer.wrap(Longs.toByteArray(clock)));
    }

    public static long deserialize(VectorClock clock) {
        return Longs.fromByteArray(clock.getState());
    }

}
