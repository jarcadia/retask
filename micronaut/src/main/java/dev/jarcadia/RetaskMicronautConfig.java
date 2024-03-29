package dev.jarcadia;

import dev.jarcadia.annontation.OnTask;
import dev.jarcadia.iface.CustomParamProvider;
import dev.jarcadia.iface.RouteProducer;
import io.micronaut.context.BeanContext;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RetaskMicronautConfig {

    private final Retask retask;
    private final BeanContext beanContext;
    private final List<RegisteredTaskHandlerAnnotation<?>> taskHandlerAnnotations;
    private final List<RegisteredCustomParamProvider<?>> customParamProviders;

    protected RetaskMicronautConfig(Retask retask, BeanContext beanContext) {
        this.retask = retask;
        this.beanContext = beanContext;
        this.taskHandlerAnnotations = new LinkedList<>();
        this.taskHandlerAnnotations.add(new RegisteredTaskHandlerAnnotation<>(OnTask.class,
                (bd, method, annotation) -> annotation.stringValue("route").get()));
        this.customParamProviders = new ArrayList<>();
    }

    public <T extends Annotation> RetaskMicronautConfig registerTaskHandlerAnnotation(Class<T> type,
            RouteProducer<T> routeProducer) {
        this.taskHandlerAnnotations.add(new RegisteredTaskHandlerAnnotation(type, routeProducer));
        return this;
    }

    public <T extends Annotation> RetaskMicronautConfig registerCustomParamProvider(Class<T> type,
            CustomParamProvider cpp) {
        this.customParamProviders.add(new RegisteredCustomParamProvider<>(type, cpp));
        return this;
    }

    public RetaskMicronautRegistations apply() {
        return RetaskMicronaut.initialize(this);
    }

    protected Retask getRetask() {
        return retask;
    }

    protected BeanContext getBeanContext() {
        return beanContext;
    }

    protected List<RegisteredTaskHandlerAnnotation<?>> getRegisteredTaskHandlerAnnotations() {
        return taskHandlerAnnotations;
    }

    protected List<RegisteredCustomParamProvider<?>> getRegisteredCustomParamProviders() {
        return customParamProviders;
    }
}
