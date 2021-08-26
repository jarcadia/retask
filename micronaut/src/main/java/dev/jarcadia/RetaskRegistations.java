package dev.jarcadia;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RetaskRegistations {

    private final List<RegisteredTaskHandler<?>> taskHandlers;

    public RetaskRegistations(List<RegisteredTaskHandler<?>> taskHandlers) {
        this.taskHandlers = taskHandlers;
    }

    public <T extends Annotation> List<RegisteredTaskHandler<T>> getRegisteredTaskHandlersOf(Class<T> type) {
        return taskHandlers.stream()
                .filter(rth -> type.equals(rth.annotationType()))
                .map(rth -> (RegisteredTaskHandler<T>) rth)
                .collect(Collectors.toList());
    }
}
