package dev.jarcadia;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.SerializationException;

public class TaskFinalizingRepository {

    private final RedisConnection rc;
    private final ObjectMapper objectMapper;

    public TaskFinalizingRepository(RedisConnection redisConnection, ObjectMapper objectMapper) {
        this.rc = redisConnection;
        this.objectMapper = objectMapper;
    }

    protected void unlockRecurKey(String route) {
        rc.commands().hdel(Keys.RECUR_DATA, route + ".lock");
    }

    protected void publishResponse(String responseChannel, Object returnValue) {
        try {
            rc.commands().publish(responseChannel, "1" + objectMapper.writeValueAsString(returnValue));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Unable to publish task response", ex);
        }
    }

    protected void publishResponse(String responseChannel, Throwable throwable) {
        try {
            rc.commands().publish(responseChannel, "0" + objectMapper.writeValueAsString(throwable));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Unable to publish task exception response", ex);
        }
    }

}