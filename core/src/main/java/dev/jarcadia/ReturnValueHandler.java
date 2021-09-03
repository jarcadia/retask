package dev.jarcadia;

import dev.jarcadia.iface.DmlEventReturnValueHandler;
import dev.jarcadia.iface.TaskReturnValueHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

class ReturnValueHandler {

    private final Logger logger = LoggerFactory.getLogger(ReturnValueHandler.class);

    private final TaskQueuingService taskQueuingService;
    private final TaskReturnValueHandler customTaskReturnValueHandler;
    private final DmlEventReturnValueHandler customDmlEventReturnValueHandler;

    protected ReturnValueHandler(TaskQueuingService taskQueuingService,
            TaskReturnValueHandler customTaskReturnValueHandler,
            DmlEventReturnValueHandler customDmlEventReturnValueHandler) {
        this.taskQueuingService = taskQueuingService;
        this.customTaskReturnValueHandler = customTaskReturnValueHandler;
        this.customDmlEventReturnValueHandler = customDmlEventReturnValueHandler;
    }

    public void handle(String taskId, String route, int attempt, int permit, Fields fields, Object returned) {
        boolean defaultHandle = true;
        if (customTaskReturnValueHandler != null) {
            try {
                defaultHandle = customTaskReturnValueHandler.handle(taskId, route, attempt, permit, fields, returned);
            } catch (Exception ex) {
                logger.warn("Exception thrown from custom TaskReturnValueHandler", ex);
            }
        }
        if (defaultHandle) {
            this.handle(returned);
        }
    }

    public void handle(String table, Fields fields, Object returned) {
        boolean defaultHandle = true;
        if (customDmlEventReturnValueHandler != null) {
            try {
                defaultHandle = customDmlEventReturnValueHandler.handle(table, fields, returned);
            } catch (Exception ex) {
                logger.warn("Exception thrown from custom DmlReturnValueHandler", ex);
            }
        }
        if (defaultHandle) {
            this.handle(returned);
        }
    }

    public void handle(Object returned) {
        if (returned instanceof Task.Builder) {
            Task.Builder task = (Task.Builder) returned;
            taskQueuingService.submitTask(task);
        } else if (returned instanceof Tasks) {
            for (Task.Builder task : ((Tasks) returned).getTasks()) {
                taskQueuingService.submitTask(task);
            }
        } else if (returned instanceof Collection) {
            Collection<?> collection = (Collection<?>) returned;
            for (Object item : collection) {
                handle(item);
            }
        } else if (returned instanceof Stream) {
            Stream<?> stream = (Stream<?>) returned;
            stream.forEach(obj -> handle(obj));
        } else if (returned instanceof Iterable) {
            Iterator<?> iterator = ((Iterable<?>) returned).iterator();
            while (iterator.hasNext()) {
                handle(iterator.next());
            }
        }
    }
}
