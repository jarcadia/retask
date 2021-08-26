package dev.jarcadia;

class MicronautTaskHandlerParamProducer {

    private final int length;
    private final int taskIdIndex;
    private final int routeIndex;
    private final int attemptIndex;
    private final int permitIndex;
    private final FieldParam[] fieldParams;

    protected MicronautTaskHandlerParamProducer(int length, int taskIdIndex,
            int routeIndex, int attemptIndex, int permitIndex, FieldParam[] fieldParams) {
        this.length = length;
        this.taskIdIndex = taskIdIndex;
        this.routeIndex = routeIndex;
        this.attemptIndex = attemptIndex;
        this.permitIndex = permitIndex;
        this.fieldParams = fieldParams;
    }

    protected Object[] produceParams(String taskId, String route, int attempt, int permit, Fields fields) {
        Object[] result = new Object[length];
        if (taskIdIndex != -1) result[taskIdIndex] = taskId;
        if (routeIndex != -1) result[routeIndex] = route;
        if (attemptIndex != -1) result[attemptIndex] = attempt;
        if (permitIndex != -1) result[permitIndex] = permit;

        for (FieldParam jtf : fieldParams) {
            result[jtf.index()] = fields.getFieldAs(jtf.name(), jtf.type());
        }
        return result;
    }
}
