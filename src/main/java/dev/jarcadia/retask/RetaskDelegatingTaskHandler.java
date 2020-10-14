package dev.jarcadia.retask;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.jarcadia.retask.RetaskRepository.RecurResult;

/**
 * This class is responsible for performing before/after actions related to running a task. This includes aligning to timestamps,
 * permitting, incrementing attempts and cleaning up. After necessary setup, this TaskHandler will delegate to a TaskDelegate. After
 * completion, it will handle the return value and cleanup.
 */
public class RetaskDelegatingTaskHandler implements RawTaskHandler {
    
    private final Logger logger = LoggerFactory.getLogger(RetaskDelegatingTaskHandler.class);
    
    private final RetaskRepository retaskRepository;
    private final RetaskDelegate delegate;
    private final RetaskProcrastinator procrastinator;

    public RetaskDelegatingTaskHandler(RetaskRepository retaskRepository, RetaskDelegate delegate, RetaskProcrastinator procrastinator) {
        this.retaskRepository = retaskRepository;
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

        // Extract metadata specific to change handlers (before/after will be null for other task types)
        String before = metadata.get("before");
        String after = metadata.get("after");

        // In most cases, params should be cleared after execution. Backlogging can alter this value
        boolean clearParams = true;

        // Setup recurrence if configured
        String recurKey = metadata.get("recurKey");
        if (recurKey != null) {
            
            String authorityKey = metadata.get("authorityKey");
            
            // Schedule recurrence
            RecurResult recurResult = retaskRepository.recur(recurKey, taskId, authorityKey, targetTimestamp,
                    Long.parseLong(metadata.get("recurInterval")));
            if (recurResult == RecurResult.KEY_LOCKED) {
                logger.info("Task {} was skipped due to locked recurKey {}", taskId, recurKey);
                return;
            } else if (recurResult == RecurResult.KEY_LACKS_AUTHORITY) {
                logger.info("Task {} lacks authority for recurKey {}", taskId, recurKey);
                return;
            }
        }

        String permitKey = null;
        int permit = -1;
        try {
            procrastinator.sleepUntil(targetTimestamp);

            permitKey = metadata.get("permitKey");
            if (permitKey != null) {
                Optional<Integer> acquired = retaskRepository.acquirePermitOrBacklog(taskId, permitKey);
                if (acquired.isPresent()) {
                    permit = acquired.get();
                } else {
                    // No permit acquired so task was backlogged, return immediately, DO NOT clear params
                    clearParams = false;
                    return;
                }
            }

            TaskBucket bucket = new TaskBucket();
            Object result = delegate.invoke(taskId, routingKey, attempt, permit, before, after, params, bucket);
            
            if (publishResponse) {
            	retaskRepository.publishResponse(taskId, result);
            }
            
            for (Task task : bucket.getTasks()) {
            	retaskRepository.submit(task);
            }

            this.handleDelegateReturnValue(result);
        } catch (TaskRetryException ex) {
            retaskRepository.retry(taskId, ex.getDuration());
        } finally {
            try {
                if (permitKey != null && permit != -1) {
                    retaskRepository.releasePermit(permitKey, permit);
                }

                if (recurKey != null) {
                    retaskRepository.unlockRecurKey(recurKey);
                }

                if (clearParams) {
                    retaskRepository.clearParams(taskId);
                }
            } catch (Throwable t) {
                logger.error("Error occurred in task cleanup finally block! This is likely to cause issues with" +
                        "recurring tasks and/or tasks requiring permits", t);
            }
        }
    }

    private void handleDelegateReturnValue(Object obj) {
        if (obj == null) {
            return;
        } else if (obj instanceof Task) {
            retaskRepository.submit((Task) obj);
        } else if (obj instanceof Task[]) {
            retaskRepository.submit((Task[]) obj);
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
