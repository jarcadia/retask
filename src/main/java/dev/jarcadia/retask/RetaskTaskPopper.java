package dev.jarcadia.retask;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisCommandInterruptedException;

/**
 * This class is responsible for pulling tasks from the queue and asynchronously passing them to a TaskHandler
 */
class RetaskTaskPopper implements Runnable, Closeable {

    private final Logger logger = LoggerFactory.getLogger(RetaskTaskPopper.class);

    private final RetaskTaskPopperRepository taskPoppingRepository;
    private final ExecutorService executor;
    private final RawTaskHandler handler;
    private final RetaskProcrastinator procrastinator;
    private final Thread thread;
    private final AtomicBoolean drainRequested;
    private final CountDownLatch drained;
    private final Phaser active;
    
    public RetaskTaskPopper(RetaskTaskPopperRepository taskPoppingRepository, ExecutorService executor,
            RawTaskHandler handler, RetaskProcrastinator procrastinator) {
        this.taskPoppingRepository = taskPoppingRepository;
        this.executor = executor;
        this.handler = handler;
        this.procrastinator = procrastinator;
        this.drainRequested = new AtomicBoolean(false);
        this.drained = new CountDownLatch(1);
        this.active = new Phaser() {
            protected boolean onAdvance(int phase, int parties) {
                if (drainRequested.get()) {
                    // If drainRequested the advancement of this phase indicates draining is complete
                    drained.countDown();
                    return true;
                } else {
                    return false;
                }
            }
        };

        this.thread = new Thread(this, "retask-popper");
        this.thread.setDaemon(false);
    }

    public void start() {
        this.thread.start();
    }

    @Override
    public void run() {
        logger.info("Starting blocking retask queue popper");

        while (!Thread.interrupted()) {
            try {
                active.register();

                String taskId = taskPoppingRepository.popTask();
                if (taskId == null) {
                    // Blocking timeout, no task popped
                    active.arriveAndDeregister();
                    continue;
                } else {
                    Map<String, String> metadata = taskPoppingRepository.getTaskMetadata(taskId);
                    logger.trace("Popped task {} {}", taskId, metadata);
                    Runnable taskRunner = () -> {
                        try {
                            handler.handle(taskId, metadata);
                        }
                        catch (Throwable t) {
                            logger.warn("Uncaught exception while processing task {} {}", taskId, metadata, t);
                        } finally {
                            active.arriveAndDeregister();
                        }
                    };
                    executor.execute(taskRunner);
                }
            } catch(RedisCommandInterruptedException ex) {
                // This exception is thrown when this.thread is interrupted while performing blocking pop
                active.arriveAndDeregister();
                break;
            } catch (Throwable t) {
                active.arriveAndDeregister();
                logger.warn("Unexpected exception while popping task queue", t);
                try {
                    procrastinator.sleepFor(1000);
                }
                catch (InterruptedException e) {
                    break;
                }
            }
        }
        // Loop has exited so no new tasks will be popped. Draining has now begun
        logger.debug("No further tasks will be claimed, waiting for active tasks to complete");
        taskPoppingRepository.close();

        // Signal to active Phaser that the next phase advance will indicate draining is complete
        drainRequested.set(true);

        /*
        Register/arrive to ensure onAdvance will be triggered eventually. There are two cases:

        1. There are some active tasks still running. If so, the following register/arrive will simply be a blip - it
        will not trigger onAdvance because there are still registered parties. When the final task does complete,
        onAdvance will be triggered which will indicate that draining is complete.

        2. There are no active tasks. If so, the following register/arrive is necessary to trigger a phase advance which
        will indicate that draining is complete (there was nothing to drain)
         */
        active.register();
        active.arriveAndDeregister();
    }
    
    @Override
    public void close() {
        this.thread.interrupt();
    }

    protected CountDownLatch getDrainedLatch() {
        return drained;
    }

    protected void awaitDrainComplete() {
        try {
            this.drained.await();
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for all active tasks to complete");
        }
    }

    protected void awaitDrainComplete(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            boolean drained = this.drained.await(timeout, unit);
            if (!drained) {
                throw new TimeoutException("Timeout while waiting for all tasks to complete");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for all active tasks to complete");
        }
    }
}
