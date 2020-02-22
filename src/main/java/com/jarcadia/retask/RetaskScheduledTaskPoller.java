package com.jarcadia.retask;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetaskScheduledTaskPoller implements Runnable, Closeable {

    private final Logger logger = LoggerFactory.getLogger(RetaskScheduledTaskPoller.class);

    private final RetaskRepository dao;
    private final RetaskProcrastinator procrastinator;
    private final Thread thread;
    private final CompletableFuture<Void> closedFuture;

    public RetaskScheduledTaskPoller(RetaskRepository retaskHelper, RetaskProcrastinator procrastinator) {
        this.dao = retaskHelper;
        this.procrastinator = procrastinator;
        this.closedFuture = new CompletableFuture<>();
        this.thread = new Thread(this, "retask-scheduled-poller");
    }
    
    public void start() {
        this.thread.start();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                long now = procrastinator.getCurrentTimeMillis();
                List<String> ready = dao.pollForScheduledTasks(now + 100);
                if (ready.size() > 0) {
                    dao.queueTaskIds(ready);
                }
                procrastinator.sleepFor(90);
            }
            catch (InterruptedException e) {
                logger.info("Scheduled task polling interrupted, exiting polling thread");
                break;
            }
            catch (Exception e) {
                logger.warn("Exception while polling for scheduled tasks", e);
                try {
					procrastinator.sleepFor(1000);
				} catch (InterruptedException e1) {
                    logger.info("Scheduled task polling interrupted while waiting for recovery, exiting polling thread");
                    break;
				}
            }
        }
        closedFuture.complete(null);
    }

    @Override
    public void close() {
        this.thread.interrupt();
    }

    protected void join(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            this.closedFuture.get(timeout, unit);
        }
        catch (InterruptedException | ExecutionException e) {
            logger.warn("Unexpected exception while waiting for retask scheduler thread to exit");
        }
    }
}
