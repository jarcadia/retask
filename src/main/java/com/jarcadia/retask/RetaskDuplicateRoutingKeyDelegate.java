package com.jarcadia.retask;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * This class is responsible for replicating a task to multiple delegates and handling the return values.
 */
class RetaskDuplicateRoutingKeyDelegate implements RetaskDelegate {

    private final ExecutorService executor;
    private final RetaskDelegate[] delegates;

    public RetaskDuplicateRoutingKeyDelegate(ExecutorService executor, RetaskDelegate... delegates) {
        this.executor = executor;
        this.delegates = delegates;
    }

    @Override
    public Object invoke(String taskId, String routingKey, int attempt, int permit, String before, String after, String params) throws Throwable {
        // USE EXECUTOR, invoke delegates, how to do return values?
        Set<Object> returnValues = new HashSet<>();
        for (RetaskDelegate delegate : delegates) {
            returnValues.add(delegate.invoke(taskId, routingKey, attempt, permit, before, after, params));
        }
        return returnValues;
    }
}
