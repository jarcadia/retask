package dev.jarcadia;

import io.lettuce.core.RedisCommandInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class is responsible for popping scheduled entries that are due
 */
class SchedulePopper implements Runnable, Closeable {

    private final Logger logger = LoggerFactory.getLogger(SchedulePopper.class);

    private final SchedulePopperService schedulePopperService;
    private final Procrastinator procrastinator;
    private final Thread thread;
    private final Phaser active;

    public SchedulePopper(SchedulePopperService schedulePopperService,
            Procrastinator procrastinator) {
        this.schedulePopperService = schedulePopperService;
        this.procrastinator = procrastinator;
        this.active = new Phaser();
        this.thread = new Thread(this, "schedule-popper");
        this.thread.setDaemon(false);
    }

    public void start() {
        this.thread.start();
    }

    @Override
    public void run() {
        logger.info("Starting jarcadia schedule popper");

        /* Register an initial party force the Phaser to be active (non-zero) until shut down */
        active.register();

        while (!Thread.interrupted()) {
            try {
                schedulePopperService.schedulePop(procrastinator.getCurrentTimeMillis());
                procrastinator.sleepFor(90);
            } catch (RedisCommandInterruptedException ex) {
                // This exception is expected when Thread interrupted while polling schedule
                break;
            } catch (InterruptedException ex) {
                // This exception is expected when Thread interrupted during sleeping
                break;
            } catch (Throwable t) {
                logger.warn("Unexpected exception while schedule popping", t);
                try {
                    procrastinator.sleepFor(1000);
                }
                catch (InterruptedException e) {
                    break;
                }
            }
        }

        // Shutdown the scheduler popper service - no further calls will be made to popSchedule()
        schedulePopperService.shutdown();

        // Arrive/deregister the initial registration
        active.arriveAndDeregister();
    }

    @Override
    public void close() {
        this.thread.interrupt();
    }

    protected void awaitShutdown() {
        try {
            this.active.awaitAdvanceInterruptibly(1);
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for item processing to complete");
        }
    }

    protected void awaitShutdown(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            this.active.awaitAdvanceInterruptibly(1, timeout, unit);
        } catch(TimeoutException ex){
            logger.error("Timeout while waiting for item processing to complete");
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for item processing to complete");
        }
    }
}
