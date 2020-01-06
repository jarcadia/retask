package com.jarcadia.retask;

import java.util.HashSet;
import java.util.Set;

/**
 * This class is responsible for replicating a task to multiple delegates and handling the return values.
 */
class RetaskDuplicateRoutingKeyDelegate implements RetaskDelegate {
    
    private final RetaskDelegate[] delegates;

    public RetaskDuplicateRoutingKeyDelegate(RetaskDelegate... delegates) {
        this.delegates = delegates;
    }

    @Override
    public Object invoke(String taskId, String routingKey, int attempt, int permit, String before, String after, String params) throws Throwable {
        Set<Object> returnValues = new HashSet<>();
        for (RetaskDelegate delegate : delegates) {
            returnValues.add(delegate.invoke(taskId, routingKey, attempt, permit, before, after, params));
        }
        return returnValues;
    }
}
