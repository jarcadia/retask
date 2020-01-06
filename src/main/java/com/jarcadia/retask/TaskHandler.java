package com.jarcadia.retask;

import java.util.Map;

@FunctionalInterface
interface TaskHandler {
    public void handle(String task, Map<String, String> metadata) throws Throwable;
}