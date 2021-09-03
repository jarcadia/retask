package dev.jarcadia;

import dev.jarcadia.iface.DmlEventHandler;
import io.micronaut.context.BeanContext;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;

class MicronautDmlEventHandler<T> implements DmlEventHandler {

    private final BeanContext context;
    private final BeanDefinition<T> beanDefinition;
    private final ExecutableMethod<T, ?> executableMethod;
    private final MicronautTaskHandlerParamProducer paramProducer;

    protected MicronautDmlEventHandler(BeanContext context, BeanDefinition<T> beanDefinition,
            ExecutableMethod<T, ?> executableMethod,
            MicronautTaskHandlerParamProducer paramProducer) {
        this.context = context;
        this.beanDefinition = beanDefinition;
        this.executableMethod = executableMethod;
        this.paramProducer = paramProducer;
    }

    @Override
    public Object apply(String table, Fields fields) {
        return executableMethod.invoke(context.getBean(beanDefinition),
                paramProducer.produceParams(new Object[]{table}, fields));
    }
}
