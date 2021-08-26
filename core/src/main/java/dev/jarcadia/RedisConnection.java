package dev.jarcadia;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.RetaskException;
import dev.jarcadia.exception.SerializationException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisConnection implements Closeable {

    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final Map<String, String> scriptCache;
    private final ObjectMapper objectMapper;

    public RedisConnection(RedisClient client, ObjectMapper objectMapper) {
        this.connection = client.connect();
        this.objectMapper = objectMapper;
        this.commands = connection.sync();
        this.scriptCache = new ConcurrentHashMap<>();
    }

    public RedisCommands<String, String> commands() {
        return commands;
    }

    public RedisEval eval() {
        return new RedisEval(this, objectMapper);
    }

    protected <T> T executeScript(String script, ScriptOutputType outputType, String[] keys, String[] args) {
        String digest = scriptCache.computeIfAbsent(script, s -> commands.scriptLoad(s));
        try {
            return commands.evalsha(digest, outputType, keys, args);
        } catch (RedisNoScriptException ex) {
            scriptCache.remove(script);
            return executeScript(script, outputType, keys, args);
        } catch (RedisCommandExecutionException ex) {
            throw new RetaskException("Error executing " + script, ex);
        }
    }

    public void publish(String channel, Object message) {
        try {
            commands.publish(channel, objectMapper.writeValueAsString(message));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Unable to serialize message", ex);
        }
    }

    @Override
    public void close() {
        this.connection.close();
    }
}

