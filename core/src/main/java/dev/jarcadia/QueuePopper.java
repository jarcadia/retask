package dev.jarcadia;

import io.lettuce.core.RedisCommandInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class is responsible for popping entries from the queue and asynchronously processing them.
 *
 * Entries include inserts, updates, deletes, tasks
 */
class QueuePopper implements Runnable, Closeable {

    private final Logger logger = LoggerFactory.getLogger(QueuePopper.class);

    private final QueuePopperRepository taskPoppingRepository;
    private final QueueEntryHandler entryHandler;
    private final Procrastinator procrastinator;
    private final Thread thread;
    private final Phaser active;

    public QueuePopper(QueuePopperRepository poppingRepo, QueueEntryHandler popperService,
            Procrastinator procrastinator) {
        this.taskPoppingRepository = poppingRepo;
        this.entryHandler = popperService;
        this.procrastinator = procrastinator;

        this.active = new Phaser();

        this.thread = new Thread(this, "jarcadia-popper");
        this.thread.setDaemon(false);
    }

    public void start() {
        this.thread.start();
    }

    @Override
    public void run() {
        logger.info("Starting jarcadia queue popper");

        /* Register an initial party force the Phaser to be active (non-zero) until shut down */
        active.register();

        taskPoppingRepository.initialize();


        while (!Thread.interrupted()) {
            try {
                // Register a party to indicate in-progress pop operation (corresponding arrive occurs in finally)
                active.register();

                // Pop and handle queued items
                taskPoppingRepository.popItems().forEach(entry -> entryHandler.handle(entry, active));

            } catch(RedisCommandInterruptedException ex) {
                // This exception is thrown when Thread is interrupted while performing blocking pop
                break;
            } catch (Throwable t) {
                logger.warn("Unexpected exception while popping queue", t);
                try {
                    procrastinator.sleepFor(1000);
                }
                catch (InterruptedException e) {
                    break;
                }
            } finally {
                active.arriveAndDeregister();
            }
        }

        // Loop has exited so no new tasks will be popped. Draining has now begun
        logger.info("No further tasks will be claimed, waiting for active tasks to complete");

        // Close the queue popper repository - no further calls will be made to popItems
        taskPoppingRepository.close();

        /*
        Arrive/deregister the initial registration. There are two cases:

        1. There are some active tasks still running. If so, the following arrival will not trigger onAdvance because
        there are still registered parties. When the entry arrives, onAdvance will be triggered which will
        indicate that draining is complete.

        2. There are no active entries currently. If so, the following arrival will trigger onAdvance which
        will indicate that draining is complete (there was nothing to drain)
         */
        active.arriveAndDeregister();
    }

    @Override
    public void close() {
        this.thread.interrupt();
    }

    protected void awaitDrainComplete() {
        try {
            this.active.awaitAdvanceInterruptibly(1);
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for item processing to complete");
        }
    }

    protected void awaitDrainComplete(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            this.active.awaitAdvanceInterruptibly(1, timeout, unit);
        } catch(TimeoutException ex){
            logger.error("Timeout while waiting for item processing to complete");
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for item processing to complete");
        }
    }
}
