package dev.jarcadia;

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;

import java.lang.annotation.Annotation;

public record RegisteredTaskHandler<T extends Annotation>(BeanDefinition<?> beanDefinition,
                                                   AnnotationValue<T> annotationValue,
                                                   Class<T> annotationType,
                                                   ExecutableMethod<?, ?> method,
                                                   String route) {
}
