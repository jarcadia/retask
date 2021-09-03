package dev.jarcadia;

enum StaticParamType {
    TASK(new Def[]{
            new Def("taskId", int.class),
            new Def("route", String.class),
            new Def("attempt", int.class),
            new Def("permit", int.class),
    }),
    DML_EVENT(new Def[] {
            new Def("table", String.class)
    });

    private final Def[] defs;

    StaticParamType(Def[] defs) {
        this.defs = defs;
    }

    protected Def[] getDefs() {
        return defs;
    }

    protected static record Def(String name, Class<?> type) {};
}

