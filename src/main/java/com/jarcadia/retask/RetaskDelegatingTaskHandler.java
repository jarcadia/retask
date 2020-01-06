package com.jarcadia.retask;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisChangedValue;

/**
 * This class is responsible for performing before/after actions related to running a task. This includes aligning to timestamps,
 * permitting, incrementing attempts and cleaning up. After necessary setup, this TaskHandler will delegate to a TaskDelegate. After
 * completion, it will handle the return value and cleanup.
 */
public class RetaskDelegatingTaskHandler implements TaskHandler {
    
    private final Logger logger = LoggerFactory.getLogger(RetaskDelegatingTaskHandler.class);
    
    private final RetaskDao dao;
    private final RetaskDelegate delegate;
    private final RetaskProcrastinator procrastinator;

    public RetaskDelegatingTaskHandler(RetaskDao dao, RetaskDelegate delegate, RetaskProcrastinator sleeper) {
        this.dao = dao;
        this.delegate = delegate;
        this.procrastinator = sleeper;
    }

    @Override
    public void handle(String taskName, Map<String, String> metadata) throws Throwable {
        String routingKey = metadata.get("routingKey");
        String params = metadata.getOrDefault("params", "{}");
        int attempt = getOrDefault(metadata, "attempt", Integer::parseInt, 0) + 1;

        // Extract metadata specific to change handlers (will be null for regular handlers)
        String before = metadata.get("before");
        String after = metadata.get("after");

        String targetTimestampStr = metadata.get("targetTimestamp");
        Long targetTimestamp = targetTimestampStr == null ? 0 : Long.parseLong(targetTimestampStr);

        // In most cases, params should be cleared after execution. Exceptions can alter this value
        boolean clearParams = true;

        // Setup recurrence if configured
        String recurKey = metadata.get("recurKey");
        if (recurKey != null) {
            
            String authorityKey = metadata.get("authorityKey");
            
            // Schedule recurrence
            boolean hasAuthority = dao.recur(recurKey, taskName, authorityKey, targetTimestamp, Long.parseLong(metadata.get("recurInterval")));
            if (!hasAuthority) {
                // Task no longer has authority, return immediately
                logger.info("Task {} lacks authority", taskName);
                return;
            }
        }

        String permitKey = null;
        int permit = -1;
        try {
            procrastinator.sleepUntil(targetTimestamp);

            permitKey = metadata.get("permitKey");
            if (permitKey != null) {
                Optional<Integer> acquired = dao.acquirePermitOrBacklog(taskName, permitKey);
                if (acquired.isPresent()) {
                    permit = acquired.get();
                } else {
                    // No permit acquired so task was backlogged, return immediately, DO NOT clear params
                    clearParams = false;
                    return;
                }
            }

            // Invoke handler
            Object result = delegate.invoke(taskName, routingKey, attempt, permit, before, after, params);

            // Process result object
            this.handleDelegateReturnValue(result);

//            // Trigger dependent tasks if completed
//            if (metadata.containsKey("dependents")) {
//                helper.triggerDependents(metadata.get("dependents"));
//            }
        } catch (RetaskRetryException ex) {
            dao.retry(taskName, ex.getDuration());
        } finally {
            // Release acquired permit
            if (permitKey != null && permit != -1) {
                dao.releasePermit(permitKey, permit);
            } 

            // Delete task metadata
            if (clearParams) {
                dao.clearParams(taskName);
            }
        }
    }

    private void handleDelegateReturnValue(Object obj) {
        if (obj instanceof Retask) {
            dao.submit((Retask) obj);
        } else if (obj instanceof Retask[]) {
            dao.submit((Retask[]) obj);
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
