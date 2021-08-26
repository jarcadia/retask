package dev.jarcadia;

import dev.jarcadia.annontation.OnTask;
import io.micronaut.context.BeanContext;

import java.lang.annotation.Annotation;
import java.util.LinkedList;
import java.util.List;

public class RetaskMicronautConfig {

    private final Retask retask;
    private final BeanContext beanContext;
    private final List<RegisteredTaskHandlerAnnotation<?>> taskHandlerAnnotations;

    protected RetaskMicronautConfig(Retask retask, BeanContext beanContext) {
        this.retask = retask;
        this.beanContext = beanContext;
        this.taskHandlerAnnotations = new LinkedList<>();
        this.taskHandlerAnnotations.add(new RegisteredTaskHandlerAnnotation<>(OnTask.class,
                (bd, method, annotation) -> annotation.stringValue("route").get()));
    }

    public <T extends Annotation> RetaskMicronautConfig registerTaskHandlerAnnotation(Class<T> type, RouteProducer<T> routeProducer) {
        this.taskHandlerAnnotations.add(new RegisteredTaskHandlerAnnotation(type, routeProducer));
        return this;
    }

    public RetaskRegistations apply() {
        return RetaskMicronaut.initialize(this);
    }

    protected Retask getJarcadia() {
        return retask;
    }

    protected BeanContext getBeanContext() {
        return beanContext;
    }

    protected List<RegisteredTaskHandlerAnnotation<?>> getRegisteredTaskHandlerAnnotations() {
        return taskHandlerAnnotations;
    }
}
