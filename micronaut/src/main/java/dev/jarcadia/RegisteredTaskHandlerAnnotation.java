package dev.jarcadia;

import dev.jarcadia.iface.RouteProducer;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;

import java.lang.annotation.Annotation;
import java.util.Optional;

record RegisteredTaskHandlerAnnotation<T extends Annotation>(Class<T> type, RouteProducer<T> routeProducer) {

    protected RegisteredTaskHandler<T> check(BeanDefinition<?> definition, ExecutableMethod<?, ?> method) {
        Optional<AnnotationValue<T>> annotation = method.findAnnotation(type);
        if (annotation.isPresent()) {
            String route = routeProducer.getRoute(definition, method, annotation.get());
            return new RegisteredTaskHandler<>(definition, annotation.get(), type, method, route);
        } else {
            return null;
        }
    }
}
