package dev.jarcadia;

import dev.jarcadia.iface.StartHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * This class is responsible for reflectively invoking @OnStart methods
 */
class ReflectiveStartHandler implements StartHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReflectiveStartHandler.class);

    private final Object instance;
    private final Method method;
    private final ReflectiveStartHandlerParamProducer paramProducer;

    protected ReflectiveStartHandler(Object instance, Method method,
            ReflectiveStartHandlerParamProducer paramProducer) {
        this.instance = instance;
        this.method = method;
        this.paramProducer = paramProducer;
    }

    @Override
    public Object run() throws Throwable {
        Object[] params = paramProducer.produceParams();
        return method.invoke(instance, params);
    }
}
