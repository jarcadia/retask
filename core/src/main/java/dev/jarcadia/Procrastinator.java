package dev.jarcadia;

/**
 * This (enjoyably named) class is responsible for abstracting system-time related functions. This abstraction enables
 * unit testing of features that depend on timing
 */
class Procrastinator {

    public long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    public void sleepFor(long duration) throws InterruptedException {
        Thread.sleep(duration);
    }

    public void sleepUntil(long targetTimestamp) throws InterruptedException {
        long diff = targetTimestamp - System.currentTimeMillis();
        if (diff > 0) {
            Thread.sleep(diff);
        }
    }
}


