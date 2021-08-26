package dev.jarcadia.iface;

import dev.jarcadia.Jarcadia;

import java.lang.annotation.Annotation;

@FunctionalInterface
public interface AnnotatedClassRegistrar<A extends Annotation> {
    void register(Jarcadia jarcadia, A annotation, Object instance);
}
