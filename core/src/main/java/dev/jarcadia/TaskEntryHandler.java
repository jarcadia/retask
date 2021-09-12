package dev.jarcadia;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.RetaskException;
import dev.jarcadia.exception.RetryTaskException;
import dev.jarcadia.iface.TaskHandler;
import dev.jarcadia.redis.RedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;

public class TaskEntryHandler {

    private final Logger logger = LoggerFactory.getLogger(Retask.class);

    private final Map<String, TaskHandler> taskConsumerMap;
    private final ExecutorService executorService;
    private ObjectMapper objectMapper;
    private final TypeReference<Map<String, String>> mapTypeReference;
    private final RedisConnection primaryRc;
    private final ReturnValueService returnValueService;
    private final TaskQueuingRepository taskQueuingRepository;
    private final PermitRepository permitRepository;
    private final TaskFinalizingRepository taskFinalizingRepository;

    public TaskEntryHandler(ExecutorService executorService, ObjectMapper objectMapper, RedisConnection primaryRc,
            ReturnValueService returnValueService, TaskQueuingRepository taskQueuingRepository,
            PermitRepository permitRepository, TaskFinalizingRepository taskFinalizingRepository) {
        this.executorService = executorService;
        this.objectMapper = objectMapper;
        this.primaryRc = primaryRc;
        this.returnValueService = returnValueService;
        this.taskQueuingRepository = taskQueuingRepository;
        this.permitRepository = permitRepository;
        this.taskFinalizingRepository = taskFinalizingRepository;
        this.taskConsumerMap = new ConcurrentHashMap<>();
        this.mapTypeReference = new TypeReference<>() { };
    }

    protected Runnable registerTaskHandler(String route, TaskHandler taskHandler) {
        TaskHandler existing = taskConsumerMap.putIfAbsent(route, taskHandler);
        if (existing != null) {
            throw new RetaskException("Cannot register multiple task handlers for " + route);
        }
        return () -> deregisterTaskHandler(route);
    }

    private void deregisterTaskHandler(String route) {
        this.taskConsumerMap.remove(route);
    }

    protected void dispatchTaskEntry(String taskId, Map<String, String> body, Phaser phaser) {

        executorService.execute(() -> {

            phaser.register();

            String route = body.get("route");
            String recurKey = body.get("recurKey");
            String permitKey = body.get("permitKey");
            String responseChannel = body.get("respondTo");
            int permit = -1;
            boolean retry = false;
            Object response = null;

            try {
                permit = permitKey == null ? -1 : getPermit(taskId, body);

                TaskHandler taskHandler = taskConsumerMap.get(route);
                if (taskHandler == null) {
                    logger.warn("No handler for route {} (task {})", route, taskId);
                } else {

                    Fields fields = getFields(taskId, body);
                    int attempt = getAttempt(body);

                    Object returnValue = taskHandler.execute(taskId, route, attempt, permit, fields);
                    if (responseChannel != null) {
                        taskFinalizingRepository.publishResponse(responseChannel, returnValue);
                    } else {
                        returnValueService.handle(returnValue);
                    }
                }
            } catch (Throwable t) {
                Throwable throwable = t instanceof InvocationTargetException ?
                        ((InvocationTargetException) t).getTargetException() : t;

                if (throwable instanceof RetryTaskException) {
                    long delay = ((RetryTaskException) throwable).getDelay();
                    try {
                        logger.info("Retrying task {} after {}", taskId, delay);
                        taskQueuingRepository.retryTask(taskId, delay);
                    } catch (Throwable t2) {
                        logger.error("Unable to retry task {}", taskId);
                    }
                    retry = true;
                } else {
                    // If respondTo PUBLISH T
                    if (responseChannel != null) {
                        try {
                            taskFinalizingRepository.publishResponse(responseChannel, throwable);
                        } catch (Throwable t2) {
                            logger.error("Unable to publish error response for task {}", taskId);
                        }
                    }
                    // TODO custom task error handler
                    logger.warn("Exception occurred while processing task {} ({})", taskId, route, throwable);
                }
            } finally {

                try {
                    if (!retry && permitKey != null && permit != -1) {
                        permitRepository.releasePermit(permitKey, permit);
                    }
                } catch (Throwable t) {
                    logger.error("Unable to release permit {} ({}) for task {}", permit, permitKey, taskId);
                }

                try {
                    if (!retry && recurKey != null) {
                        taskFinalizingRepository.unlockRecurKey(recurKey);
                    }
                } catch (Throwable t) {
                    logger.error("Unable to unlock route {} for task {}", route, taskId);
                }

                phaser.arriveAndDeregister();
            }
        });
    }

    private Fields getFields(String entryId, Map<String, String> body) {
        String serializedFields = body.get("fields");
        try {
            return serializedFields == null ? Fields.empty() :
                    objectMapper.readValue(serializedFields.getBytes(StandardCharsets.UTF_8), Fields.class);
        } catch (IOException ex) {
            throw new RetaskException("Task " + entryId + " has invalid fields " + serializedFields, ex);
        }
    }

    private int getPermit(String entryId, Map<String, String> body) {
        String permit = body.get("permit");
        if (permit == null) {
            throw new RetaskException("Task " + entryId + " is missing permit");
        } else {
            try {
                return Integer.parseInt(permit);
            } catch (NumberFormatException ex) {
                throw new RetaskException("Task " + entryId + " has invalid permit value " + permit, ex);
            }
        }
    }

    private int getAttempt(Map<String, String> body) {
        String attempt = body.get("attempt");
        return attempt == null ? 1 : Integer.parseInt(attempt);
    }
}

