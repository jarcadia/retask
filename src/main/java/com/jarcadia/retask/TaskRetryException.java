package com.jarcadia.retask;

import java.util.concurrent.TimeUnit;

public class TaskRetryException extends RuntimeException {
    
    private final long duration;
    private final TimeUnit timeUnit;

    public TaskRetryException(long duration, TimeUnit timeUnit)
    {
        this.duration = duration;
        this.timeUnit = timeUnit;
    }
    
    protected long getDuration() {
        return timeUnit.toMillis(duration);
    }

}
