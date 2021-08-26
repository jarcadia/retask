package dev.jarcadia.exception;

import java.util.concurrent.TimeUnit;

public class RetryTaskException extends RuntimeException {

    private final long delay;
    private final TimeUnit timeUnit;

    public RetryTaskException(long delay, TimeUnit timeUnit)
    {
        this.delay = delay;
        this.timeUnit = timeUnit;
    }

    public long getDelay() {
        return timeUnit.toMillis(delay);
    }
}
