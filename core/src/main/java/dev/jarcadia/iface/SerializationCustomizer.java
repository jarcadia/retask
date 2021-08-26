package dev.jarcadia.iface;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;

@FunctionalInterface
public interface SerializationCustomizer {
    void customize(SimpleModule module, ObjectMapper objectMapper, TypeFactory typeFactory);
}
