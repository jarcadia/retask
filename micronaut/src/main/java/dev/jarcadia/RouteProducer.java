package dev.jarcadia;

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;

import java.lang.annotation.Annotation;

@FunctionalInterface
public interface RouteProducer<T extends Annotation> {

    String getRoute(BeanDefinition<?> beanDef, ExecutableMethod<?,?> method, AnnotationValue<T> annotation);

}
