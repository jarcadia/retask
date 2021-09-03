package dev.jarcadia;

import com.fasterxml.jackson.databind.JavaType;
import io.micronaut.core.type.Argument;

record DynamicParam(int index, String name, Class<?> type, JavaType javaType) { }
