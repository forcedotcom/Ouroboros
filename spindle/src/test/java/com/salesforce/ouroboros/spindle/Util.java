package com.salesforce.ouroboros.spindle;

import static junit.framework.Assert.*;

public class Util {
    public static void waitFor(String reason, Condition condition,
                               long timeout, long interval)
                                                           throws InterruptedException {
        long target = System.currentTimeMillis() + timeout;
        while (!condition.value()) {
            if (target < System.currentTimeMillis()) {
                fail(reason);
            }
            Thread.sleep(interval);
        }
    }

    public static interface Condition {
        boolean value();
    }
}
