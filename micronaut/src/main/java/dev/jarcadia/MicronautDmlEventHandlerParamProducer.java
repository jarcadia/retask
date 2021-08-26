package dev.jarcadia;

class MicronautDmlEventHandlerParamProducer {

    private final int length;
    private final int tableIdIndex;
    private final FieldParam[] fieldParams;

    protected MicronautDmlEventHandlerParamProducer(int length, int tableIdIndex, FieldParam[] fieldParams) {
        this.length = length;
        this.tableIdIndex = tableIdIndex;
        this.fieldParams = fieldParams;
    }

    protected Object[] produceParams(String table, Fields fields) {
        Object[] result = new Object[length];
        if (tableIdIndex != -1) result[tableIdIndex] = table;

        for (FieldParam jtf : fieldParams) {
            result[jtf.index()] = fields.getFieldAs(jtf.name(), jtf.type());
        }
        return result;
    }
}
