package dev.jarcadia;

import dev.jarcadia.iface.StartHandler;
import io.micronaut.context.BeanContext;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;

import java.util.Map;

class MicronautStartHandler<T> implements StartHandler {

    private final BeanContext context;
    private final BeanDefinition<T> beanDefinition;
    private final ExecutableMethod<T, ?> executableMethod;

    protected MicronautStartHandler(BeanContext context, BeanDefinition<T> beanDefinition,
            ExecutableMethod<T, ?> executableMethod) {
        this.context = context;
        this.beanDefinition = beanDefinition;
        this.executableMethod = executableMethod;
    }

    @Override
    public Object run() throws Throwable {
        return executableMethod.invoke(context.getBean(beanDefinition));
    }
}
