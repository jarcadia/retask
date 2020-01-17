package com.jarcadia.retask;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This class is responsible for reflectively invoking @RetaskHandler and @RetaskChangeHandler methods. It will match task properties and params
 * to the method parameters and inject them accordingly.
 */
class RetaskReflectiveTaskDelegate implements RetaskDelegate {

    private final Method targetMethod;
    private final MethodParamsProducer paramsProducer;
    private final RetaskWorkerInstanceProvider provider;
    private final Class<?> targetClass;
    private volatile Object targetInstance;

    protected RetaskReflectiveTaskDelegate(RetaskWorkerInstanceProvider provider, Class<?> targetClass, Method targetMethod, MethodParamsProducer paramsProducer) {
        this.provider = provider;
        this.targetClass = targetClass;
        this.targetMethod = targetMethod;
        this.paramsProducer = paramsProducer;
    }

    @Override
    public Object invoke(String taskId, String routingKey, int attempt, int permit, String before, String after, String params) throws Throwable {
        synchronized (this) {
            if (targetInstance == null) {
                targetInstance = provider.getInstance(targetClass);
                if (targetInstance == null) {
                    throw new RetaskException("No instance of worker (type " + targetClass.getName() + ") provided");
                } 
            }
        }
        try {
            Object[] methodParameters = paramsProducer.produceParameters(taskId, routingKey, attempt, permit, before, after, params);
            return targetMethod.invoke(targetInstance, methodParameters);
        }
        catch (InvocationTargetException e) {
            throw e.getCause() == null ? e : e.getCause();
        }
        catch (IllegalAccessException | IllegalArgumentException e) {
            throw new RetaskException("Failed to invoke task handler reflectively for routing key " + routingKey + " Params: " + params, e);
        }
        catch (IOException e) {
            throw new RetaskException("Unable to deserialize parameters for routing key " + routingKey + " with params " + params, e);
        }
    }
}
