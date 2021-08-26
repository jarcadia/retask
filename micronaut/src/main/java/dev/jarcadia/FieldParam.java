package dev.jarcadia;

import com.fasterxml.jackson.databind.JavaType;

public record FieldParam(int index, String name, JavaType type) { }
