package com.jarcadia.retask;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RetaskService {
    
    private final RetaskDao dao;
    private final RetaskTaskPopper taskPopper;
    private final RetaskScheduledTaskPoller scheduledTaskPoller;

    public RetaskService(RetaskDao dao, RetaskTaskPopper taskPopper, RetaskScheduledTaskPoller scheduledTaskPoller) {
        this.dao = dao;
        this.taskPopper = taskPopper;
        this.scheduledTaskPoller = scheduledTaskPoller;
    }

    public void submit(Retask... tasks) {
        dao.submit(tasks);
    }

    public void verifyPermits(String permitKey, int numPermits) {
        dao.verifyPermits(permitKey, numPermits);
    }
    
    public int checkPermits(String permitKey) {
        return this.dao.checkPermits(permitKey);
    }

    public void shutdown(long timeout, TimeUnit unit) throws TimeoutException {
        this.taskPopper.close();
        this.scheduledTaskPoller.close();
        this.taskPopper.join(timeout, unit);
        this.scheduledTaskPoller.join(timeout, unit);
    }
}
