package dev.jarcadia.iface;

import dev.jarcadia.Fields;

@FunctionalInterface
public interface TaskHandler {
    Object execute(String taskId, String route, int attempt, int permit, Fields fields) throws Throwable;
}
