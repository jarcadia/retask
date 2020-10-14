package dev.jarcadia.retask;

import java.util.Map;

@FunctionalInterface
interface RawTaskHandler {
    public void handle(String taskId, Map<String, String> metadata) throws Throwable;
}