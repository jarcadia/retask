package dev.jarcadia;

import dev.jarcadia.iface.DeleteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * This class is responsible for reflectively invoking @RetaskChangeHandler methods
 */
class ReflectiveDeleteHandler implements DeleteHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReflectiveDeleteHandler.class);

    private final Object instance;
    private final Method method;
    private final ReflectiveDeleteHandlerParamProducer paramProducer;

    protected ReflectiveDeleteHandler(Object instance, Method method,
            ReflectiveDeleteHandlerParamProducer paramProducer) {
        this.instance = instance;
        this.method = method;
        this.paramProducer = paramProducer;
    }

    @Override
    public Object apply(String id) throws Throwable {
        Object[] params = paramProducer.produceParams(id);
        return method.invoke(instance, params);
    }

//    @Override
//    public void apply(AppliedMerge mod) {
//        try {
//            ReturnValues returns = paramsProducer.hasReturnsParameter() ? new ReturnValues() : null;
//            Object[] methodParameters = paramsProducer.produceParams(mod, returns);
//            Object worker = workerRef.get();
//            if (worker != null) {
//                Object returned = method.invoke(worker, methodParameters);
//                returnValueHandlerManager.handle(returned, returns);
//            } else {
//                logger.warn("Unable to invoke change callback {}.{} - no worker instance available",
//                        method.getDeclaringClass().getSimpleName(), method.getName());
//            }
//        } catch (Throwable t) {
//            throw new TaskException(String.format("Failed to invoke change handler %s.%s reflectively",
//                    method.getDeclaringClass().getSimpleName(), method.getName()), t);
//        }
//    }


}
