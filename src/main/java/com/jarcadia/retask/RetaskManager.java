package com.jarcadia.retask;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RetaskManager {
	
    private final RetaskTaskPopper taskPopper;
    private final RetaskScheduledTaskPoller scheduledTaskPoller;
    private final Retask retask;
    
    public RetaskManager(RetaskTaskPopper taskPopper, RetaskScheduledTaskPoller scheduledTaskPoller, Retask retask) {
    	this.taskPopper = taskPopper;
    	this.scheduledTaskPoller = scheduledTaskPoller;
    	this.retask = retask;
    }

    public void start() {
        this.taskPopper.start();
        this.scheduledTaskPoller.start();
    }

    public void start(RetaskStartupCallback callback) {
    	this.start();
    	callback.onStartup(retask);
    }
    
    public void start(Task task) {
    	this.start();
    	retask.submit(task);
    }
    
    public Retask getInstance() {
        return retask;
    }
    
    public void shutdown(long timeout, TimeUnit unit) throws TimeoutException {
        this.taskPopper.close();
        this.scheduledTaskPoller.close();
        this.taskPopper.join(timeout, unit);
        this.scheduledTaskPoller.join(timeout, unit);
    }
}
