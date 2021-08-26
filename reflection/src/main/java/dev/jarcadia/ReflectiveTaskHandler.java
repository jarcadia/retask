package dev.jarcadia;

import dev.jarcadia.iface.TaskHandler;

import java.lang.reflect.Method;

/**
 * This class is responsible for reflectively invoking @RetaskHandler
 * methods. It will match task properties and params to the method parameters and inject them accordingly.
 */
class ReflectiveTaskHandler implements TaskHandler {

    private final Object instance;
    private final Method method;
    private final ReflectiveTaskHandlerParamsProducer taskParamsProducer;

    protected ReflectiveTaskHandler(Object instance, Method method,
            ReflectiveTaskHandlerParamsProducer taskParamsProducer) {
        this.instance = instance;
        this.method = method;
        this.taskParamsProducer = taskParamsProducer;
    }

    @Override
    public Object execute(String taskId, String route, int attempt, int permit, TaskFields fields) throws Throwable {
        Object[] params = taskParamsProducer.produceParams(taskId, route, attempt, permit, fields);
        return method.invoke(instance, params);
    }

//    @Override
//    public Object invoke(String taskId, String routingKey, int attempt, int permit, String params, ReturnValues returns)
//            throws Throwable {
//        try {
//            Object[] methodParameters = taskParamsProducer.produceParams(taskId, routingKey, attempt, permit, params, returns);
//            return method.invoke(worker.get(), methodParameters);
//        } catch (ParamsException ex) {
//            throw new TaskException("Unable to produce parameters for " + method.getDeclaringClass().getSimpleName() + "." + method.getName(), ex);
//        } catch (InvocationTargetException e) {
//            throw e.getCause() == null ? e : e.getCause();
//        } catch (IllegalAccessException | IllegalArgumentException e) {
//            throw new TaskException("Failed to invoke task handler reflectively for routing key " + routingKey + " Params: " + params, e);
//        }
//    }

}
