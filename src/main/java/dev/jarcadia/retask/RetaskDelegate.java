package dev.jarcadia.retask;

@FunctionalInterface
interface RetaskDelegate {
    public Object invoke(String taskId, String routingKey, int attempt, int permit, String before, String after, String params, TaskBucket bucket) throws Throwable;
}
