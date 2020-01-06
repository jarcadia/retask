package com.jarcadia.retask;

import java.util.List;
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
//            if (task.hasTriggers()) {
//                setMetadataAndSetupTrigger(task);
            if (task.isTriggeredManually()) {
               setMetadataOnly(task); 
            } else if (task.isScheduled()) {
                setMetadataAndSchedule(task);
            } else {
                setMetadataAndQueue(task);
            }
            logger.trace("Queued task {} {} {}", task.getName(), task.getMetadata(), task.getParams());
        }
    }
    
    private void setMetadataOnly(Retask task) {
        prepareTaskMultiSet(task, rcommando.eval()).returnStatus();
    }
    
    private void setMetadataAndQueue(Retask task) {
        RedisEval eval = prepareTaskMultiSet(task, rcommando.eval())
                .addKey(Key.TASKS);
        eval.appendScript(String.format("redis.call('rpush', KEYS[%d], KEYS[1])", eval.getLastKeyIndex()))
                .returnStatus();
    }
    
    private void setMetadataAndSchedule(Retask task) {
        RedisEval eval = prepareTaskMultiSet(task, rcommando.eval())
                .addKey(Key.SCHEDULED)
                .addArg(task.isScheduled() ? task.getScheduledTimestamp() : System.currentTimeMillis());
        eval.appendScript(String.format("redis.call('zadd', KEYS[%d], ARGV[%d], KEYS[1])", eval.getLastKeyIndex(), eval.getLastArgIndex()))
                .returnStatus();
    }
    
//    private void setMetadataAndSetupTrigger(Retask task) {
//        Eval eval = prepareTaskMultiSet(task, rcommando.eval())
//                .addArg(task.getTriggers().size());
//        eval.appendScript(String.format("redis.call('hset', KEYS[1], 'cdl', ARGV[%d]);", eval.getLastArgIndex()));
//        for (String trigger : task.getTriggers()) {
//            int idx = eval.addKey(trigger).getLastKeyIndex();
//            eval.appendScript(String.format("local deps = redis.call('hget', KEYS[%d], 'dependents'); if (not deps) then deps = {} else deps = cjson.decode(deps) end table.insert(deps, KEYS[1]); redis.call('hset', KEYS[%d], 'dependents', cjson.encode(deps))", idx, idx));
//        }
//        eval.returnStatus();
//    }
    
    private RedisEval prepareTaskMultiSet(Retask task, RedisEval eval) {
        int keyIdx = eval.addKey(task.getName()).getLastKeyIndex();
        StringJoiner hmset = new StringJoiner(",", "redis.call(", ");")
                .add("'hmset'").add(String.format("KEYS[%d]", keyIdx));
        for (String key : task.getMetadata().keySet()) {
            String value = task.getMetadata().get(key);
            if (value != null) {
                eval.addArg(key);
                hmset.add(String.format("ARGV[%d]", eval.getLastArgIndex()));
                eval.addArg(task.getMetadata().get(key));
                hmset.add(String.format("ARGV[%d]", eval.getLastArgIndex()));
            }
        }
        
        if (task.hasParams()) {
            eval.addArg(task.getParams());
            hmset.add("'params'");
            hmset.add(String.format("ARGV[%d]", eval.getLastArgIndex()));
        }
        eval.appendScript(hmset.toString());
        
        if (task.isRecurring()) {
            eval.addKey(Key.RECUR_AUTH_KEY);
            eval.addArg(task.getRecurKey());
            eval.addArg(task.getAuthorityKey());
            eval.appendScript(String.format("redis.call('hset', KEYS[%d], ARGV[%d], ARGV[%d]);",
                    eval.getLastKeyIndex(), eval.getLastArgIndex() - 1, eval.getLastArgIndex()));
        }
        
        return eval;
    }
    
    protected void retry(String taskName, long millis) {
        String retryId = UUID.randomUUID().toString();
        rcommando.eval()
            .useScriptFile("retryTask")
            .addKeys(Key.SCHEDULED, taskName, retryId)
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
                .useScriptFile("recurTask")
                .addKeys(Key.TASKS, Key.SCHEDULED, Key.RECUR_LOCK_KEY, Key.RECUR_AUTH_KEY, taskKey, nextId)
                .addArgs(recurKey, String.valueOf(currentTargetTimestamp), String.valueOf(nextTimestamp), authorityKey)
                .returnBoolean();
        
        return authority;
    }
    
    protected void revokeAuthority(String recurKey) {
        rcommando.core().hdel(Key.RECUR_AUTH_KEY, recurKey);
    }

    protected void verifyPermits(String permitKey, int numPermits) {
        logger.info("Initializing {} permits for {}", numPermits, permitKey);
        rcommando.eval()
                .useScriptFile("verifyPermits")
                .addKeys(permitKey + ".available", permitKey + ".assigned")
                .addArg(numPermits)
                .returnStatus();
    }

    protected Optional<Integer> acquirePermitOrBacklog(String task, String permitKey) {
        logger.debug("Trying to acquire permit in {}", permitKey);
        return Optional.ofNullable(rcommando.eval()
                .useScriptFile("acquirePermitOrBacklogTask")
                .addKeys(permitKey + ".available", permitKey + ".assigned", permitKey + ".backlog", task)
                .returnNullableInt());
    }
    
    protected void releasePermit(String permitKey, int permit) {
        rcommando.eval()
                .useScriptFile("releaseTaskPermit")
                .addKeys(permitKey + ".available", permitKey + ".assigned", permitKey + ".backlog", Key.TASKS)
                .addArg(permit)
                .returnStatus();
        logger.debug("Released permit {} in {}", permit, permitKey); 
    }
    
    protected int checkPermits(String permitKey) {
        return rcommando.eval()
                .useScriptFile("checkPermit")
                .addKeys(permitKey + ".available", permitKey + ".backlog")
                .returnInt();
    }

    protected void clearParams(String task) {
        rcommando.core().del(task);
    }

    protected List<String> pollForScheduledTasks(long cutoff) {
        return rcommando.eval()
            .useScriptFile("scheduledTaskPoll")
            .addKeys(Key.SCHEDULED)
            .addArg(cutoff)
            .returnMulti();
    }

    protected long queueTaskIds(List<String> taskIds) {
        return rcommando.core().rpush(Key.TASKS, taskIds.toArray(new String[0]));
    }
}
