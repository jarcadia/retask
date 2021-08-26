package dev.jarcadia;

import java.lang.reflect.Method;

public class MethodRegister {

    private final Jarcadia jarcadia;
    private final Method method;
    private final Object instance;

    public MethodRegister(Jarcadia jarcadia, Method method, Object instance) {
        this.jarcadia = jarcadia;
        this.method = method;
        this.instance = instance;
    }

    public void asTaskHandler(String route) {
        jarcadia.registerTaskHandler(route,
                new ReflectiveTaskHandler(instance, method,
                        new ReflectiveTaskHandlerParamsProducer(jarcadia, jarcadia.getObjectMapper(), method)));
    }

    public void asInsertHandler(String type) {
        jarcadia.registerRecordInsertHandler(type,
                new ReflectiveMergeHandler(instance, method,
                        new ReflectiveMergeHandlerParamProducer(jarcadia, jarcadia.getObjectMapper(), method)));
    }

    public void asUpdateHandler(String type, String... fields) {
        ReflectiveMergeHandlerParamProducer paramProducer = new ReflectiveMergeHandlerParamProducer(jarcadia,
                jarcadia.getObjectMapper(), method);

        if (fields.length > 0 && fields[0].length() > 0) {
            jarcadia.registerRecordMergeHandler(type, fields,
                    new ReflectiveMergeHandler(instance, method, paramProducer));
        } else {
            jarcadia.registerRecordMergeHandler(type,
                    new ReflectiveMergeHandler(instance, method, paramProducer));
        }
    }

    public void asDeleteHandler(String type) {
        jarcadia.registerRecordDeleteHandler(type,
                new ReflectiveDeleteHandler(instance, method,
                        new ReflectiveDeleteHandlerParamProducer(jarcadia, method)));
    }

    public void asStartHandler() {
        jarcadia.registerStartHandler(
                new ReflectiveStartHandler(instance, method,
                        new ReflectiveStartHandlerParamProducer(jarcadia,
                                method.getParameters())));
    }
}
