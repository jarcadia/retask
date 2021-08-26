package dev.jarcadia;

import java.lang.reflect.Parameter;

class ReflectiveStartHandlerParamProducer extends ReflectiveParamProducer {

    protected ReflectiveStartHandlerParamProducer(Jarcadia jarcadia, Parameter[] parameters) {
        super(jarcadia, parameters);
    }

    protected Object[] produceParams(String id) {
        return super.produceParams();
    }
}

