package dev.jarcadia;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.SerializationException;
import io.lettuce.core.ScriptOutputType;

public class RedisEval {

    private final RedisConnection redisConnection;
    private final ObjectMapper objectMapper;
    private String script;
    private final List<String> keys;
    private final List<String> args;
    private final String[] arrayRef = new String[0];
    private final TypeReference<List<String>> listTypeRef;

    protected RedisEval(RedisConnection redisConnection, ObjectMapper objectMapper) {
        this.redisConnection = redisConnection;
        this.objectMapper = objectMapper;
        this.keys = new ArrayList<>();
        this.args = new ArrayList<>();
        this.listTypeRef = new TypeReference<>() {};
    }

    public RedisEval cachedScript(String script) {
        this.script = script;
        return this;
    }

    public RedisEval appendScript(String script) {
        this.script = this.script == null ? script : this.script + script;
        return this;
    }

    public RedisEval addKey(String key) {
        keys.add(key);
        return this;
    }

    public RedisEval addKeys(String... keys) {
        for (String key : keys) {
            this.keys.add(key);
        }
        return this;
    }

    public RedisEval addKeys(Iterable<String> keys) {
        for (String key : keys) {
            this.keys.add(key);
        }
        return this;
    }

    public RedisEval addKeys(Stream<String> keys) {
        keys.forEach(key -> this.keys.add(key));
        return this;
    }

//    public RedisEval deserializeAndAddKeys(String serializedKeys) {
//        try {
//			keys.addAll(formatter.deserialize(serializedKeys, listTypeRef));
//		} catch (DeserializationException e) {
//			throw new PersistException("Unable to deserialize " + serializedKeys + " as List<String>");
//		}
//        return this;
//    }


    public RedisEval addArg(String arg) {
        this.args.add(arg);
        return this;
    }

    public RedisEval addArg(double arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }

    public RedisEval addArg(int arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }

    public RedisEval addArg(long arg) {
        this.args.add(String.valueOf(arg));
        return this;
    }

    public RedisEval addArgs(String... args) {
        for (String arg : args) {
            this.args.add(arg);
        }
        return this;
    }

    public RedisEval addArgs(Collection<String> args) {
        this.args.addAll(args);
        return this;
    }

    public RedisEval addArg(Object toSerialize) {
        try {
            this.args.add(objectMapper.writeValueAsString(toSerialize));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Unable to serialize argument", ex);
        }
        return this;
    }

    public int getLastKeyIndex() {
        return this.keys.size();
    }

    public int getLastArgIndex() {
        return this.args.size();
    }

    public String returnStatus() {
        return execute(ScriptOutputType.STATUS);
    }

    public String returnValue() {
        return execute(ScriptOutputType.VALUE);
    }

    public int returnInt() {
        Long value = execute(ScriptOutputType.INTEGER);
        return value.intValue();
    }

    public long returnLong() {
        return execute(ScriptOutputType.INTEGER);
    }

    public Integer returnNullableInt() {
        Long value = execute(ScriptOutputType.INTEGER);
        return value == null ? null : value.intValue();
    }

    public List<String> returnMulti() {
        return execute(ScriptOutputType.MULTI);
    }

    public boolean returnBoolean() {
        return execute(ScriptOutputType.BOOLEAN);
    }

    private <T> T execute(ScriptOutputType outputType) {
    	return redisConnection.executeScript(script, outputType, keys(), args());
    }

    protected String[] keys() {
        return this.keys.toArray(arrayRef);
    }

    protected String[] args() {
        return this.args.toArray(arrayRef);
    }
}
