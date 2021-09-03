package dev.jarcadia.annontation;

import io.micronaut.context.annotation.Executable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Function;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Executable
public @interface OnDelete {

    String table();
}
