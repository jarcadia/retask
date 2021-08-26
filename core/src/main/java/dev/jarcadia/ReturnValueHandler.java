package dev.jarcadia;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public class ReturnValueHandler {

    private final TaskQueuingService taskQueuingService;

    public ReturnValueHandler(TaskQueuingService taskQueuingService) {
        this.taskQueuingService = taskQueuingService;
    }

    public void handle(Object returnValue) {
        if (returnValue instanceof Task.Builder) {
            Task.Builder task = (Task.Builder) returnValue;
            taskQueuingService.submitTask(task);
        } else if (returnValue instanceof Tasks) {
            for (Task.Builder task : ((Tasks) returnValue).getTasks()) {
                taskQueuingService.submitTask(task);
            }
        } else if (returnValue instanceof Collection) {
            Collection<?> collection = (Collection<?>) returnValue;
            for (Object item : collection) {
                handle(item);
            }
        } else if (returnValue instanceof Stream) {
            Stream<?> stream = (Stream<?>) returnValue;
            stream.forEach(obj -> handle(obj));
        } else if (returnValue instanceof Iterable) {
            Iterator<?> iterator = ((Iterable<?>) returnValue).iterator();
            while (iterator.hasNext()) {
                handle(iterator.next());
            }
        }
    }
}
