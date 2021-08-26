package dev.jarcadia;

import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;

abstract class ReflectiveParamProducer {

    private final Jarcadia jarcadia;
    private final int numParams;
    protected final boolean[] accountedFor;
    private final int jarcadiaParamIndex;

    protected ReflectiveParamProducer(Jarcadia jarcadia, Parameter[] parameters) {
        this.jarcadia = jarcadia;
        this.numParams = parameters.length;
        // Create array to track which params are accounted for
        this.accountedFor = new boolean[parameters.length];
        this.jarcadiaParamIndex = discoverTypedParamIndex(parameters, Jarcadia.class);
    }

    protected Object[] produceParams() {
        Object[] params = new Object[numParams];
        addIfPresent(params, jarcadiaParamIndex, jarcadia);
        return params;
    }

    protected int discoverTypedParamIndex(Parameter[] parameters, Class<?> clazz) {
        for (int i = 0; i < parameters.length; i++) {
            if (!accountedFor[i] && clazz.equals(parameters[i].getType())) {
                accountedFor[i] = true;
                return i;
            }
        }
        return -1;
    }

    protected int discoverNamedParamIndex(Parameter[] parameters, String name) {
        for (int i = 0; i < parameters.length; i++) {
            if (!accountedFor[i] && matches(parameters[i], name)) {
                accountedFor[i] = true;
                return i;
            }
        }
        return -1;
    }

    protected int discoverAssignableParamIndex(Parameter[] parameters, Class<?> clazz) {
        for (int i = 0; i < parameters.length; i++) {
            if (!accountedFor[i] && clazz.isAssignableFrom(parameters[i].getType())) {
                accountedFor[i] = true;
                return i;
            }
        }
        return -1;
    }

    protected boolean isClass(Parameter parameter, Class<?> clazz) {
        return clazz.isAssignableFrom(parameter.getType());
    }

    protected boolean isAssignableFrom(Parameter parameter, Class<?> clazz) {
        return clazz.isAssignableFrom(parameter.getType());
    }

    protected boolean matches(Parameter parameter, String name) {
        return name.equals(getName(parameter));
    }

    protected String getName(Parameter parameter) {
//        Param annotation = parameter.getAnnotation(Param.class);
//        return annotation == null ? parameter.getName() : annotation.value();
        return parameter.getName();
    }

    private boolean isOptional(Parameter parameter) {
        Type type = parameter.getParameterizedType();
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return Optional.class.equals(parameterizedType.getRawType());
        } else {
            return false;
        }
    }

    protected boolean isOptionalOf(Parameter parameter, Class<?> inner) {
        Type type = parameter.getParameterizedType();
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return Optional.class.equals(parameterizedType.getRawType()) &&
                    inner.equals(parameterizedType.getActualTypeArguments()[0]);
        } else {
            return false;
        }
    }

    protected void addIfPresent(Object[] params, int index, Object param) {
        if (index >= 0) {
            params[index] = param;
        }
    }
}
