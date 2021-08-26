package dev.jarcadia;

import dev.jarcadia.exception.ParamsException;

import java.lang.reflect.Method;

class ReflectiveDeleteHandlerParamProducer extends ReflectiveParamProducer {

    private final Method method;
    private final int idParamIndex;

    protected ReflectiveDeleteHandlerParamProducer(Jarcadia jarcadia, Method method) {
        super(jarcadia, method.getParameters());
        this.method = method;
        this.idParamIndex = discoverNamedParamIndex(method.getParameters(), "id");
    }

    protected Object[] produceParams(String id) throws ParamsException {
        try {
            Object[] result = super.produceParams();
            addIfPresent(result, idParamIndex, id);
            return result;
         } catch (Exception ex) {
            throw new ParamsException("Unable to produce parameters for " + method.getDeclaringClass().getSimpleName()
                    + "." + method.getName(), ex);
        }
    }
}

