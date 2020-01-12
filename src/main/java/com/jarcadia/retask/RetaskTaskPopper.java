package com.jarcadia.retask;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisCommandInterruptedException;

/**
 * This class is responsible for pulling tasks from the queue and asynchronously passing them to a TaskHandler
 */
class RetaskTaskPopper implements Runnable, Closeable {

    private final Logger logger = LoggerFactory.getLogger(RetaskTaskPopper.class);

    private final RetaskTaskPopperDao dao;
    private final ExecutorService executor;
    private final TaskHandler handler;
    private final Thread thread;
    private final CompletableFuture<Void> closedFuture;
    private final RetaskProcrastinator procrastinator;
    
    public RetaskTaskPopper(RetaskTaskPopperDao dao, ExecutorService executor, TaskHandler handler, RetaskProcrastinator procrastinator) {
        this.dao = dao;
        this.executor = executor;
        this.handler = handler;
        this.procrastinator = procrastinator;
        this.closedFuture = new CompletableFuture<>();

        this.thread = new Thread(this, "retask-popper");
        this.thread.setDaemon(true);
    }

    public void start() {
        this.thread.start();
    }

    @Override
    public void run() {
        logger.info("Starting blocking retask queue popper");

        while (!Thread.interrupted()) {
            try {
                String taskId = dao.popTask();
                if (taskId == null) {
                    continue; // Blocking timeout
                } else {
                    Map<String, String> metadata = dao.getTaskMetadata(taskId);
                    logger.trace("Popped task {} {}", taskId, metadata);

                    Runnable taskRunner = () -> {
                        try {
                            handler.handle(taskId, metadata);
                        }
                        catch (Throwable t) {
                            logger.warn("Uncaught exception while processing task {} {}", taskId, metadata, t);
                        }
                    };
                    executor.execute(taskRunner);
                }
            } catch(RedisCommandInterruptedException ex) {
                break;
            } catch (Throwable t) {
                logger.warn("Unexpected exception while polling task queue", t);
                try {
                    procrastinator.sleepFor(1000);
                }
                catch (InterruptedException e) {
                    break;
                }
            }
        }
        dao.close();
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
            logger.warn("Unexpected exception while waiting for exit");
        }
    }
}
