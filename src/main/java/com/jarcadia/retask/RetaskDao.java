package com.jarcadia.retask;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisEval;


class RetaskDao {
    
    private final Logger logger = LoggerFactory.getLogger(RetaskDao.class);
    
    private final RedisCommando rcommando;
    
    public RetaskDao(RedisCommando rcommando) {
        this.rcommando = rcommando;
    }
    
    protected void submit(Retask... tasks) {
        for (Retask task : tasks) {
            if (task.isScheduled()) {
                scheduleTask(task).returnStatus();
            } else {
            	queueTask(task).returnStatus();
            }
            logger.trace("Submitted task {} {} {}", task.getId(), task.getMetadata(), task.getParams());
        }
    }
    
    private RedisEval queueTask(Retask task) {
    	RedisEval eval = rcommando.eval()
    			.addKeys(Key.TASKS, task.getId());
    	prepRecurrence(task, eval, LuaScripts.QUEUE_TASK, LuaScripts.QUEUE_TASK_WITH_RECUR);
    	prepMetadata(task, eval);
    	prepParams(task, eval);
    	return eval;
    }
    
    private RedisEval scheduleTask(Retask task) {
    	RedisEval eval = rcommando.eval()
    			.addKeys(Key.SCHEDULED, task.getId())
    			.addArg("targetTimestamp")
    			.addArg(task.getTargetTimestamp());
    	prepRecurrence(task, eval, LuaScripts.SCHEDULE_TASK, LuaScripts.SCHEDULE_TASK_WITH_RECUR);
    	prepMetadata(task, eval);
    	prepParams(task, eval);
    	return eval;
    }
    
    private void prepRecurrence(Retask task, RedisEval eval, String scriptWithoutRecurrence, String scriptWithRecurrence) {
    	if (task.isRecurring()) {
    		eval.cachedScript(scriptWithRecurrence);
    		eval.addKey(Key.RECUR_AUTH_KEY);
    		eval.addArgs("recurKey", task.getRecurKey(), "authorityKey", task.getAuthorityKey());
    	} else {
    		eval.cachedScript(scriptWithoutRecurrence);
    	}
    }
    
    private void prepMetadata(Retask task, RedisEval eval) {
    	for (Entry<String, String> entry : task.getMetadata().entrySet()) {
    		if (entry.getValue() != null) {
                eval.addArgs(entry.getKey(), entry.getValue());
    		}
    	}
    }
    
    private void prepParams(Retask task, RedisEval eval) {
    	if (task.hasParams()) {
    		eval.addArg("params");
    		eval.addArg(task.getParams());
    	}
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
        logger.info("Initializing {} permits for {}", numPermits, permitKey);
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
