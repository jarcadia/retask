package com.jarcadia.retask;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is responsible for reflectively invoking @RetaskHandler and @RetaskChangeHandler methods. It will match task properties and params
 * to the method parameters and inject them accordingly.
 */
class RetaskReflectiveTaskDelegate implements RetaskDelegate {

    private final AtomicReference<Object> worker;
    private final Method method;
    private final ParamsProducer paramsProducer;

    protected RetaskReflectiveTaskDelegate(AtomicReference<Object> worker, Method method, ParamsProducer paramsProducer) {
    	this.worker = worker;
        this.method = method;
        this.paramsProducer = paramsProducer;
    }

    @Override
    public Object invoke(String taskId, String routingKey, int attempt, int permit, String before, String after, String params, TaskBucket bucket) throws Throwable {
        try {
            Object[] methodParameters = paramsProducer.produceParams(taskId, routingKey, attempt, permit, before, after, params, bucket);
            return method.invoke(worker.get(), methodParameters);
        } catch (RetaskParamsException ex) {
            throw new RetaskException("Unable to produce parameters for " + method.getDeclaringClass().getSimpleName() + "." + method.getName(), ex);
        } catch (InvocationTargetException e) {
            throw e.getCause() == null ? e : e.getCause();
        } catch (IllegalAccessException | IllegalArgumentException e) {
            throw new RetaskException("Failed to invoke task handler reflectively for routing key " + routingKey + " Params: " + params, e);
        }
    }
}
