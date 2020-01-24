package com.jarcadia.retask;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is responsible for performing before/after actions related to running a task. This includes aligning to timestamps,
 * permitting, incrementing attempts and cleaning up. After necessary setup, this TaskHandler will delegate to a TaskDelegate. After
 * completion, it will handle the return value and cleanup.
 */
public class RetaskDelegatingTaskHandler implements RawTaskHandler {
    
    private final Logger logger = LoggerFactory.getLogger(RetaskDelegatingTaskHandler.class);
    
    private final RetaskDao dao;
    private final RetaskDelegate delegate;
    private final RetaskProcrastinator procrastinator;

    public RetaskDelegatingTaskHandler(RetaskDao dao, RetaskDelegate delegate, RetaskProcrastinator procrastinator) {
        this.dao = dao;
        this.delegate = delegate;
        this.procrastinator = procrastinator;
    }

    @Override
    public void handle(String taskId, Map<String, String> metadata) throws Throwable {
        String routingKey = metadata.get("routingKey");
        String params = metadata.getOrDefault("params", "{}");
        int attempt = getOrDefault(metadata, "attempt", Integer::parseInt, 0) + 1;
        boolean publishResponse = getOrDefault(metadata, "publishResponse", Boolean::parseBoolean, false);

        String targetTimestampStr = metadata.get("targetTimestamp");
        Long targetTimestamp = targetTimestampStr == null ? procrastinator.getCurrentTimeMillis() : Long.parseLong(targetTimestampStr);

        // Extract metadata specific to change handlers (null is expected for other handler types)
        String before = metadata.get("before");
        String after = metadata.get("after");

        // In most cases, params should be cleared after execution. Backlogging can alter this value
        boolean clearParams = true;

        // Setup recurrence if configured
        String recurKey = metadata.get("recurKey");
        if (recurKey != null) {
            
            String authorityKey = metadata.get("authorityKey");
            
            // Schedule recurrence
            boolean hasAuthority = dao.recur(recurKey, taskId, authorityKey, targetTimestamp, Long.parseLong(metadata.get("recurInterval")));
            if (!hasAuthority) {
                // Task no longer has authority, return immediately
                logger.info("Task {} lacks authority", taskId);
                return;
            }
        }

        String permitKey = null;
        int permit = -1;
        try {
            procrastinator.sleepUntil(targetTimestamp);

            permitKey = metadata.get("permitKey");
            if (permitKey != null) {
                Optional<Integer> acquired = dao.acquirePermitOrBacklog(taskId, permitKey);
                if (acquired.isPresent()) {
                    permit = acquired.get();
                } else {
                    // No permit acquired so task was backlogged, return immediately, DO NOT clear params
                    clearParams = false;
                    return;
                }
            }


            // Invoke handler
            TaskBucket bucket = new TaskBucket();
            Object result = delegate.invoke(taskId, routingKey, attempt, permit, before, after, params, bucket);
            
            // Publish response if requested
            if (publishResponse) {
            	dao.publishResponse(taskId, result);
            }
            
            // Process bucketed tasks
            for (Task task : bucket.getTasks()) {
            	dao.submit(task);
            }

            // Process result object
            this.handleDelegateReturnValue(result);
        } catch (RetaskRetryException ex) {
            dao.retry(taskId, ex.getDuration());
        } finally {
            // Release acquired permit
            if (permitKey != null && permit != -1) {
                dao.releasePermit(permitKey, permit);
            }

            // Delete task metadata
            if (clearParams) {
                dao.clearParams(taskId);
            }
        }
    }

    private void handleDelegateReturnValue(Object obj) {
        if (obj == null) {
            return;
        } else if (obj instanceof Task) {
            dao.submit((Task) obj);
        } else if (obj instanceof Task[]) {
            dao.submit((Task[]) obj);
        } else if (obj instanceof Optional<?>) {
            Optional<?> optional = (Optional<?>) obj;
            if (optional.isPresent()) {
                handleDelegateReturnValue(optional.get());
            }
        } else if (obj instanceof Collection) {
            Collection<?> list = (Collection<?>) obj;
            for (Object element : list) {
                handleDelegateReturnValue(element);
            }
        }
    }

    private <T> T getOrDefault(Map<String, String> metadata, String key, Function<String, T> mapper, T defaultValue) {
        String val = metadata.get(key);
        return val == null ? defaultValue : mapper.apply(val);
    }
}
