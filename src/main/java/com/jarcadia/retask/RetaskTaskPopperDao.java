package com.jarcadia.retask;

import java.util.Map;

import com.jarcadia.rcommando.RedisCommando;

import io.lettuce.core.KeyValue;

class RetaskTaskPopperDao {

    private final RedisCommando rcommando;

    public RetaskTaskPopperDao(RedisCommando rcommando) {
        this.rcommando = rcommando;
    }

    protected String popTask() {
        KeyValue<String, String> popped = rcommando.core().blpop(5, Key.TASKS);
        if (popped == null) {
            return null;
        } else {
            return popped.getValue();
        }
    }

    protected Map<String, String> getTaskMetadata(String taskId) {
        return rcommando.core().hgetall(taskId);
    }
    
    protected void close() {
        rcommando.close();
    }
}
