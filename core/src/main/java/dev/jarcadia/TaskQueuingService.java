package dev.jarcadia;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.exception.CalledTaskException;
import dev.jarcadia.exception.SerializationException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class TaskQueuingService {

    private final ObjectMapper objectMapper;
    private final TaskQueuingRepository repo;

    private final StatefulRedisPubSubConnection<String, String> pubsubConnection;
    private final RedisPubSubAsyncCommands<String, String> pubsubCommands;
    private final Map<String, CompletableFuture<String>> pendingResponseMap;

    public TaskQueuingService(RedisClient redisClient, ObjectMapper objectMapper, TaskQueuingRepository repo) {
        this.objectMapper = objectMapper;
        this.repo = repo;

        this.pendingResponseMap = new ConcurrentHashMap<>();
        this.pubsubConnection = redisClient.connectPubSub();
        this.pubsubCommands = pubsubConnection.async();
        this.pubsubConnection.addListener(new TaskResponseListener());

    }

    private class TaskResponseListener extends RedisPubSubAdapter<String, String> {

        @Override
        public void message(String channel, String message) {
            CompletableFuture<String> future = pendingResponseMap.remove(channel);
            if (future != null) {
                if ('1' == message.charAt(0)) {
                    future.complete(message.substring(1));
                } else {
                    try {
                        Throwable cause = objectMapper.readValue(message.substring(1), Throwable.class);
                        future.completeExceptionally(new CalledTaskException("Task failed", cause));
                    } catch (Throwable ex) {
                        future.completeExceptionally(new CompletionException(
                                "Unable to deserialize task exception response", ex));
                    }
                }
            }
            pubsubCommands.unsubscribe(channel);
        }
    }

    protected CompletableFuture<String> callTask(Task.Builder task) {
        // Create future for response
        CompletableFuture<String> responseFuture = new CompletableFuture<>();

        // Create random response channel and subscribe to it
        String responseChannel = UUID.randomUUID().toString();
        pendingResponseMap.put(responseChannel, responseFuture);
        pubsubCommands.subscribe(responseChannel);

        // Set the task to respond to the created channel
        task.setRespondTo(responseChannel);

        // Submit the task
        this.submitTask(task);

        return responseFuture;
    }

    public <T> CompletableFuture<T> callTask(Task.Builder task, Class<T> type) {
        return parseResponse(callTask(task), type, null);
    }

    public <T> CompletableFuture<T> callTask(Task.Builder task, TypeReference<T> typeRef) {
        return parseResponse(callTask(task), null, typeRef);
    }

    private <T> CompletableFuture<T> parseResponse(CompletableFuture<String> future, Class<T> type,
            TypeReference<T> typeRef) {
        return future.thenApply(res -> {
            try {
                return type != null ? objectMapper.readValue(res, type) :
                        objectMapper.readValue(res, typeRef);
            } catch (JsonProcessingException e) {
                throw new CompletionException("Unable to parse task response as " + type.getSimpleName(), e);
            }
        });
    }

    public <T, V> CompletableFuture<Map<T, V>> callTaskForEach(Collection<T> input, Function<T, Task.Builder> tasker, TypeReference<V> typeRef) {
        final CompletableFuture<V>[] futures = new CompletableFuture[input.size()];

        List<T> orderedInput = new ArrayList<>(input);
        for (int i=0; i< futures.length; i++) {
            futures[i] = this.callTask(tasker.apply(orderedInput.get(i)), typeRef);
        }

        return CompletableFuture.allOf(futures)
                .thenApply(v -> {
                    Map<T, V> result = new HashMap<>();
                    for (int i=0; i<futures.length; i++) {
                        result.put(orderedInput.get(i), futures[i].join());
                    }
                    return result;
                });
    }


    protected void submitTask(Task.Builder task) {
        String sFields;
        try {
            sFields = objectMapper.writeValueAsString(task.getFields());
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Unable to serialize task fields", ex);
        }
        if (task.getTargetTimestamp() == null) {
            if (task.getRecurKey() == null) {
                if (task.getPermitKey() == null) {
                    // Task is !scheduled !recurring !permit
                    repo.queueTask(task.getRoute(), sFields, task.getRespondTo());
                } else {
                    // Task is !scheduled !recurring permit
                    repo.queueTaskWithRequiredPermit(task.getRoute(), sFields, task.getPermitKey(), task.getRespondTo());
                }
            } else {
                if (task.getPermitKey() == null) {
                    // Task is !scheduled, recurring, !permit
                    repo.queueRecurringTask(task.getRoute(), task.getRecurKey(), sFields, task.getRecurInterval());
                } else {
                    // Task is !scheduled, recurring, !permit
                    repo.queueRecurringTaskWithRequiredPermit(task.getRoute(), task.getRecurKey(), sFields,
                            task.getRecurInterval(), task.getPermitKey());
                }
            }
        } else { // Task is scheduled
            if (task.getRecurKey() == null) {
                if (task.getPermitKey() == null) {
                    // Task is scheduled, !recurring, !permit
                    repo.scheduleTask(task.getRoute(), task.getTargetTimestamp(), sFields, task.getRespondTo());
                } else {
                    // Task is scheduled, !recurring, permit
                    repo.scheduleTaskWithPermit(task.getRoute(), task.getTargetTimestamp(), sFields,
                            task.getPermitKey(), task.getRespondTo());
                }
            } else {
                // Task is scheduled, recurring, permit?
                repo.scheduleRecurringTask(task.getRoute(), task.getRecurKey(), task.getTargetTimestamp(), sFields,
                        task.getRecurInterval(), task.getPermitKey());
            }
        }
    }

    protected boolean submitDmlEvent(long eventId, String statement, String table, String data) {
        return repo.submitDmlEvent(eventId, statement, table, data);
    }

    /**
     * - Receive a notification
     *      - Attempt to claim table.eventId with getset on simple redis expiring key
     *      - If successful
     *          - Run Apply defined below
     *      - The idea here is that in the most common case (multiple listeners up and active)
     *        only one listener will execute Apply
     *
     * Apply:
     * - Compare version being applied with existing version
     *      - If applied version is existing + 1 OR existing does not exist it
     *          - Update version hash
     *          - Add task
     *          - Apply any future events that are pending and align with current version
     *          - return true (safe to delete from postgres)
     *      - If applied version is greater than expected
     *          - Store update in zset db.:table-events version upd{sFields}
     *      - If applied versions is before expected
     *          - Ignore it
     *
     * - On Startup
     *      - Load and attempt to apply any existing updates
     *
     */
}
