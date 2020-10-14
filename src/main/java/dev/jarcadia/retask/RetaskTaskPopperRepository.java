package dev.jarcadia.retask;

import java.util.Map;

import dev.jarcadia.redao.RedaoCommando;

import io.lettuce.core.KeyValue;

class RetaskTaskPopperRepository {

    private final RedaoCommando rcommando;

    public RetaskTaskPopperRepository(RedaoCommando rcommando) {
        this.rcommando = rcommando;
    }

    protected String popTask() {
        KeyValue<String, String> popped = rcommando.core().blpop(1, Key.TASKS);
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
