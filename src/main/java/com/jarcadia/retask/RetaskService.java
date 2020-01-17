package com.jarcadia.retask;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RetaskService {
    
    private final RetaskDao dao;
    private final RetaskTaskPopper taskPopper;
    private final RetaskScheduledTaskPoller scheduledTaskPoller;
    private final RetaskRecruiter recruiter;

    public RetaskService(RetaskDao dao, RetaskTaskPopper taskPopper, RetaskScheduledTaskPoller scheduledTaskPoller, RetaskRecruiter recruiter) {
        this.dao = dao;
        this.taskPopper = taskPopper;
        this.scheduledTaskPoller = scheduledTaskPoller;
        this.recruiter = recruiter;
    }
    
    public void start() {
        this.taskPopper.start();
        this.scheduledTaskPoller.start();
    }

    public void submit(Retask... tasks) {
        dao.submit(tasks);
    }
    
    public void revokeAuthority(String recurKey) {
        dao.revokeAuthority(recurKey);
    }

    public void setAvailablePermits(String permitKey, int numPermits) {
        dao.setAvailablePermits(permitKey, numPermits);
    }

    public int getAvailablePermits(String permitKey) {
        return this.dao.getAvailablePermits(permitKey);
    }

    public Set<String> verifyRoutes(Collection<String> requestedRoutes) {
        return this.recruiter.verifyRoutes(requestedRoutes);
    }

    public void shutdown(long timeout, TimeUnit unit) throws TimeoutException {
        this.taskPopper.close();
        this.scheduledTaskPoller.close();
        this.taskPopper.join(timeout, unit);
        this.scheduledTaskPoller.join(timeout, unit);
    }
}
