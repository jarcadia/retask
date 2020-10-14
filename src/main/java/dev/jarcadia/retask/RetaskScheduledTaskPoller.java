package dev.jarcadia.retask;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetaskScheduledTaskPoller implements Runnable, Closeable {

    private final Logger logger = LoggerFactory.getLogger(RetaskScheduledTaskPoller.class);

    private final RetaskRepository retaskRepository;
    private final RetaskProcrastinator procrastinator;
    private final Thread thread;
    private final CountDownLatch terminatedLatch;

    public RetaskScheduledTaskPoller(RetaskRepository retaskHelper, RetaskProcrastinator procrastinator) {
        this.retaskRepository = retaskHelper;
        this.procrastinator = procrastinator;
        this.terminatedLatch = new CountDownLatch(1);
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
                List<String> ready = retaskRepository.pollForScheduledTasks(now + 100);
                if (ready.size() > 0) {
                    retaskRepository.queueTaskIds(ready);
                }
                procrastinator.sleepFor(90);
            }
            catch (InterruptedException e) {
                logger.debug("Scheduled task polling interrupted, exiting polling thread");
                break;
            }
            catch (Exception e) {
                logger.warn("Exception while polling for scheduled tasks", e);
                try {
					procrastinator.sleepFor(1000);
				} catch (InterruptedException e1) {
                    break;
				}
            }
        }
        terminatedLatch.countDown();
    }

    @Override
    public void close() {
        this.thread.interrupt();
    }

    protected CountDownLatch getTerminatedLatch() {
        return terminatedLatch;
    }

    protected void awaitTermination() {
        try {
            this.terminatedLatch.await();
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for ScheduledTaskPoller to close", e);
        }
    }

    protected void awaitTermination(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            boolean drained = this.terminatedLatch.await(timeout, unit);
            if (!drained) {
                throw new TimeoutException("Timeout while waiting for ScheduledTaskPoller to close");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for ScheduledTaskPoller to close", e);
        }
    }
}
