package dev.jarcadia.retask;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for replicating a task to multiple delegates and handling the return values.
 */
class RouteSplittingDelegate implements RetaskDelegate {
    
    private final Logger logger = LoggerFactory.getLogger(RouteSplittingDelegate.class);

    private final ExecutorService executor;
    private final List<RetaskDelegate> delegates;

    public RouteSplittingDelegate(ExecutorService executor, List<RetaskDelegate> delegates) {
        this.executor = executor;
        this.delegates = delegates;
    }

    @Override
    public Object invoke(String taskId, String routingKey, int attempt, int permit, String before, String after, String params, TaskBucket bucket) throws Throwable {
        final List<CompletableFuture<Object>> futures = delegates.stream().map(d -> new CompletableFuture<>()).collect(Collectors.toList());
        for (int i=0; i<delegates.size(); i++) {
            final RetaskDelegate delegate = delegates.get(i);
            final CompletableFuture<Object> future = futures.get(i);
            executor.submit(() -> {
                try {
                    Object returnValue = delegate.invoke(taskId, routingKey, attempt, permit, before, after, params, bucket);
                    future.complete(returnValue);
                }
                catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        }
        // Gather return values
        List<Object> results = new LinkedList<>();
        for (CompletableFuture<Object> future : futures) {
            try {
                results.add(future.get());
            }
            catch (InterruptedException | ExecutionException e) {
                logger.warn("Unhandled exception while processing task {} routingKey: {}, before: {}, after: {}, param: {}",
                        taskId, routingKey, before, after, params, e.getCause() != null ? e.getCause() : e);
            }
        }
        return results;
    }
}
