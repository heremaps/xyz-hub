package com.here.xyz.hub.util;

import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorUtils {

    public static boolean compareAndIncrementUpTo(int maxExpect, AtomicInteger i) {
        int currentValue = i.get();
        while (currentValue < maxExpect) {
            if (i.compareAndSet(currentValue, currentValue + 1)) {
                return true;
            }
            currentValue = i.get();
        }
        return false;
    }

    public static boolean compareAndDecrement(int minExpect, AtomicInteger i) {
        int currentValue = i.get();
        while (currentValue > minExpect) {
            if (i.compareAndSet(currentValue, currentValue - 1)) {
                return true;
            }
            currentValue = i.get();
        }
        return false;
    }

}
