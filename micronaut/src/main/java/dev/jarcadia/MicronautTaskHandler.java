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

    protected MicronautTaskHandler(BeanContext context, BeanDefinition<T> beanDefinition,
            ExecutableMethod<T, ?> executableMethod,
            MicronautTaskHandlerParamProducer paramProducer) {
        this.context = context;
        this.beanDefinition = beanDefinition;
        this.executableMethod = executableMethod;
        this.paramProducer = paramProducer;
    }

    @Override
    public Object execute(String taskId, String route, int attempt, int permit, Fields fields) throws Throwable {
        return executableMethod.invoke(context.getBean(beanDefinition),
                paramProducer.produceParams(taskId, route, attempt, permit, fields));
    }
}
