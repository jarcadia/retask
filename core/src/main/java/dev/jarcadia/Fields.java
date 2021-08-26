package dev.jarcadia;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Fields {

    private final ObjectMapper objectMapper;
    private final byte[] source;
    private final Map<String, FieldLocation> fields;

    public static Fields empty() {
        return new Fields(null, null, Map.of());
    }

    public Fields(ObjectMapper objectMapper, byte[] source, Map<String, FieldLocation> fields) {
        this.objectMapper = objectMapper;
        this.source = source;
        this.fields = fields;
    }

    public Set<String> getNames() {
        return fields.keySet();
    }

    public <T> T getFieldAs(String name, Class<T> type) {
        FieldLocation tf = this.fields.get(name);
        if (tf == null) {
            if (Optional.class.equals(type)) {
                return (T) Optional.empty();
            } else {
                return null;
            }
        } else {
            try {
                return objectMapper.readValue(source, tf.start(), tf.length() - tf.start(), type);
            } catch (IOException e) {
                throw new RuntimeException("Unable to deserialize JSON", e);
            }
        }
    }

    public <T> T getFieldAs(String name, TypeReference<T> typeRef) {
        FieldLocation tf = fields.get(name);
        if (tf == null) {
            if (Optional.class.equals(typeRef.getType())) {
                return (T) Optional.empty();
            } else {
                return null;
            }
        } else {
            try {
                return objectMapper.readValue(source, tf.start(), tf.length() - tf.start(), typeRef);
            } catch (IOException e) {
                throw new RuntimeException("Unable to deserialize JSON", e);
            }
        }
    }

    public <T> T getFieldAs(String name, JavaType type) {
        FieldLocation tf = fields.get(name);
        if (tf == null) {
            if (Optional.class.equals(type.getRawClass())) {
                return (T) Optional.empty();
            }
            return null;
        } else {
            try {
                return objectMapper.readValue(source, tf.start(), tf.length() - tf.start(), type);
            } catch (IOException e) {
                throw new RuntimeException("Unable to deserialize JSON", e);
            }
        }
    }
}
