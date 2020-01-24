package com.jarcadia.retask;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisEval;


class RetaskDao {
    
    private final Logger logger = LoggerFactory.getLogger(RetaskDao.class);
    
    private final RedisCommando rcommando;
    private final ObjectMapper objectMapper;
    private final TaskResponseListener responseListener;
    
    public RetaskDao(RedisCommando rcommando) {
        this.rcommando = rcommando;
        this.objectMapper = rcommando.getObjectMapper();
        this.responseListener = new TaskResponseListener(rcommando);
    }
    
    protected void submit(Task... tasks) {
        for (Task task : tasks) {
            submitTask(task);
            logger.trace("Submitted task {} {} {}", task.getId(), task.getMetadata(), task.getParams());
        }
    }
    
    private String submitTask(Task task) {
    	RedisEval eval = rcommando.eval()
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

    protected boolean recur(String recurKey, String taskKey, String authorityKey, long currentTargetTimestamp, long recurInterval) {
        long now = System.currentTimeMillis();
        long nextTimestamp = currentTargetTimestamp + recurInterval;
        if (nextTimestamp < now) {
            long diff = now - nextTimestamp + 1;
            long skip = (long) Math.ceil(diff / recurInterval) + 1L;
            nextTimestamp+= skip * recurInterval;
        }
        String nextId = UUID.randomUUID().toString();
        boolean authority = rcommando.eval()
                .cachedScript(LuaScripts.RECUR_TASK)
                .addKeys(Key.TASKS, Key.SCHEDULED, Key.RECUR_LOCK_KEY, Key.RECUR_AUTH_KEY, taskKey, nextId)
                .addArgs(recurKey, String.valueOf(currentTargetTimestamp), String.valueOf(nextTimestamp), authorityKey)
                .returnBoolean();
        
        return authority;
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
}
