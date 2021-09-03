package dev.jarcadia;

import com.fasterxml.jackson.databind.JavaType;
import io.micronaut.core.type.Argument;

public class SettableParam {

    private final String name;
    private final Class<?> type;
    public Object value;

    protected SettableParam(String name, Class<?> type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public Object get() {
        return value;
    }

    public boolean isNull() {
        return this.value == null;
    }

    public void set(Object value) {
        this.value = value;
    }
}
