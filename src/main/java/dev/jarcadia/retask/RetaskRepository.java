package dev.jarcadia.retask;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import io.lettuce.core.StreamScanCursor;
import io.lettuce.core.output.ScoredValueStreamingChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jarcadia.redao.RedaoCommando;
import dev.jarcadia.redao.Eval;


class RetaskRepository {
    
    private final Logger logger = LoggerFactory.getLogger(RetaskRepository.class);
    
    private final RedaoCommando rcommando;
    private final ObjectMapper objectMapper;
    private final TaskResponseListener responseListener;
    
    public RetaskRepository(RedaoCommando rcommando) {
        this.rcommando = rcommando;
        this.objectMapper = rcommando.getObjectMapper();
        this.responseListener = new TaskResponseListener(rcommando);
    }
    
    protected void submit(Task... tasks) {
        for (Task task : tasks) {
            submitTask(task);
            logger.debug("Submitted task {} {} {}", task.getId(), task.getMetadata(), task.getParams());
        }
    }
    
    private String submitTask(Task task) {
    	Eval eval = rcommando.eval()
    			.cachedScript(LuaScripts.SUBMIT_TASK)
    			.addKeys(Key.TASKS, Key.SCHEDULED, Key.RECUR_AUTH_KEY, task.getId());
    	for (Entry<String, String> entry : task.getMetadata().entrySet()) {
    		if (entry.getValue() != null) {
                eval.addArgs(entry.getKey(), entry.getValue());
    		}
    	}
    	if (task.hasParams()) {
    		eval.addArg("params");
    		eval.addArg(task.getParams());
    	}
    	return eval.returnStatus();
    }
    
    protected <T> Future<T> call(Task task, Class<T> clazz) {
    	CompletableFuture<T> future = responseListener.await(task.getId(), clazz);
    	task.shouldPublishResponse();
    	this.submit(task);
        return future;
    }

    protected <T> Future<T> call(Task task, TypeReference<T> typeRef) {
    	CompletableFuture<T> future = responseListener.await(task.getId(), typeRef);
    	task.shouldPublishResponse();
    	this.submit(task);
        return future;
    }
    
    protected void retry(String taskId, long millis) {
        String retryId = UUID.randomUUID().toString();
        rcommando.eval()
            .cachedScript(LuaScripts.RETRY_TASK)
            .addKeys(Key.SCHEDULED, taskId, retryId)
            .addArg(System.currentTimeMillis() + millis)
            .returnStatus();
    }
    
    protected RecurResult recur(String recurKey, String taskKey, String authorityKey, long currentTargetTimestamp,
            long recurInterval) {
        long now = System.currentTimeMillis();
        long nextTimestamp = currentTargetTimestamp + recurInterval;
        if (nextTimestamp < now) {
            long diff = now - nextTimestamp + 1;
            long skip = (long) Math.ceil(diff / recurInterval) + 1L;
            nextTimestamp+= skip * recurInterval;
        }

        // Ensure the next recurrence is not scheduled too close to this occurrence (prevent recurKey lock violation)
        if (nextTimestamp - now < Math.min(5000, recurInterval / 4)) {
            nextTimestamp += recurInterval;
        }

        String nextId = UUID.randomUUID().toString();
        logger.trace("Calculated recurrence for {} ({}) - now {}, next {}, diff {}", recurKey, nextId, now, nextTimestamp, nextTimestamp - now);
        int result = rcommando.eval()
                .cachedScript(LuaScripts.RECUR_TASK)
                .addKeys(Key.TASKS, Key.SCHEDULED, Key.RECUR_LOCK_KEY, Key.RECUR_AUTH_KEY, taskKey, nextId)
                .addArgs(recurKey, String.valueOf(currentTargetTimestamp), String.valueOf(nextTimestamp), authorityKey)
                .returnInt();
        
        if (result == 0) {
        	return RecurResult.PROCEED;
        } else if (result == 1) {
        	return RecurResult.KEY_LOCKED;
        } else if (result == 2) {
        	return RecurResult.KEY_LACKS_AUTHORITY;
        } else {
        	throw new RetaskException("Unexpected return value " + result + " from recur script");
        }
    }
    
    protected void unlockRecurKey(String recurKey) {
    	rcommando.core().hdel(Key.RECUR_LOCK_KEY,  recurKey);
    }

    protected void revokeAuthority(String recurKey) {
        rcommando.core().hdel(Key.RECUR_AUTH_KEY, recurKey);
    }

    protected void setAvailablePermits(String permitKey, int numPermits) {
        logger.debug("Initializing {} permits for {}", numPermits, permitKey);
        rcommando.eval()
                .cachedScript(LuaScripts.SET_AVAILABLE_PERMITS)
                .addKeys(permitKey + ".available", permitKey + ".assigned")
                .addArg(numPermits)
                .returnStatus();
    }

    protected Optional<Integer> acquirePermitOrBacklog(String task, String permitKey) {
        logger.debug("Trying to acquire permit in {}", permitKey);
        return Optional.ofNullable(rcommando.eval()
                .cachedScript(LuaScripts.ACQUIRE_PERMIT_OR_BACKLOG)
                .addKeys(permitKey + ".available", permitKey + ".assigned", permitKey + ".backlog", task)
                .returnNullableInt());
    }

    protected void releasePermit(String permitKey, int permit) {
        rcommando.eval()
                .cachedScript(LuaScripts.RELEASE_PERMIT)
                .addKeys(permitKey + ".available", permitKey + ".assigned", permitKey + ".backlog", Key.TASKS)
                .addArg(permit)
                .returnStatus();
        logger.debug("Released permit {} in {}", permit, permitKey); 
    }

    protected int getAvailablePermits(String permitKey) {
        return rcommando.eval()
                .cachedScript(LuaScripts.GET_AVAILABLE_PERMITS)
                .addKeys(permitKey + ".available", permitKey + ".backlog")
                .returnInt();
    }
    
    protected void publishResponse(String taskId, Object response) throws JsonProcessingException {
    	logger.debug("Publishing response for {} : {}", taskId, response);
    	String msg = response == null ? "" : objectMapper.writeValueAsString(response);
    	rcommando.core().publish("task.response." + taskId, msg);
    }

    protected void clearParams(String task) {
        rcommando.core().del(task);
    }

    protected List<String> pollForScheduledTasks(long cutoff) {
        return rcommando.eval()
            .cachedScript(LuaScripts.SCHEDULED_TASK_POLL)
            .addKeys(Key.SCHEDULED)
            .addArg(cutoff)
            .returnMulti();
    }

    protected long queueTaskIds(List<String> taskIds) {
        return rcommando.core().rpush(Key.TASKS, taskIds.toArray(new String[0]));
    }

    protected void checkForScheduledDuplicates() {
        final List<String> scheduledTaskIds = new LinkedList<>();
        ScoredValueStreamingChannel<String> channel = scoredValue -> scheduledTaskIds.add(scoredValue.getValue());
        StreamScanCursor cursor = rcommando.core().zscan(channel, Key.SCHEDULED);
        while(!cursor.isFinished()) {
            rcommando.core().zscan(channel, Key.SCHEDULED, cursor);
        }

        logger.info("Checking {} scheduled tasks for duplicate recur keys", scheduledTaskIds.size());

        Map<String, String> recurKeys = new HashMap<>();
        for (String taskId : scheduledTaskIds) {
            String recurKey = rcommando.core().hget(taskId, "recurKey");

            String existingTaskId = recurKeys.get(recurKey);
            if (existingTaskId != null) {
                logger.warn("Recur key {} is double scheduled as tasks {} and {}", existingTaskId, taskId);
            } else {
                logger.info("Recur key {} is scheduled for {}", recurKey, taskId);
                recurKeys.put(recurKey, taskId);
            }

        };

        logger.info("Done scanning scheduled tasks for duplicate recur keys");

}

    protected enum RecurResult {
    	PROCEED,
    	KEY_LOCKED,
    	KEY_LACKS_AUTHORITY,
    }
}
