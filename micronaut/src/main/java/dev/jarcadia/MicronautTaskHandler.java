package dev.jarcadia;

import dev.jarcadia.iface.TaskHandler;
import io.micronaut.context.BeanContext;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;

class MicronautTaskHandler<T> implements TaskHandler {

    private final BeanContext context;
    private final BeanDefinition<T> beanDefinition;
    private final ExecutableMethod<T, ?> executableMethod;
    private final MicronautTaskHandlerParamProducer paramProducer;
    private final RegisteredTaskHandlerAnnotation<?> annotation;

    protected MicronautTaskHandler(BeanContext context, BeanDefinition<T> beanDefinition,
            ExecutableMethod<T, ?> executableMethod, MicronautTaskHandlerParamProducer paramProducer,
            RegisteredTaskHandlerAnnotation<?> annotation) {
        this.context = context;
        this.beanDefinition = beanDefinition;
        this.executableMethod = executableMethod;
        this.paramProducer = paramProducer;
        this.annotation = annotation;
    }

    @Override
    public Object execute(String taskId, String route, int attempt, int permit, Fields fields) {
        return executableMethod.invoke(context.getBean(beanDefinition),
                paramProducer.produceParams(new Object[]{taskId, route, attempt, permit}, fields));
    }

    protected RegisteredTaskHandlerAnnotation<?> getAnnotation() {
        return annotation;
    }
}
