package dev.jarcadia.iface;

import dev.jarcadia.MethodRegister;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

@FunctionalInterface
public interface AnnotatedMethodHandler<A extends Annotation> {
    void register(MethodRegister register, A annotation, Method method, Object instance);
}
