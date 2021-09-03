package dev.jarcadia;

import dev.jarcadia.redis.RedisConnection;
import dev.jarcadia.redis.RedisEval;
import io.lettuce.core.SetArgs;

public class TaskQueuingRepository {

    private final RedisConnection redisConnection;
    private final Procrastinator procrastinator;

    public TaskQueuingRepository(RedisConnection redisConnection, Procrastinator procrastinator) {
        this.redisConnection = redisConnection;
        this.procrastinator = procrastinator;
    }

    protected void queueTask(String route, String sFields, String respondTo) {
        if (respondTo == null) {
            redisConnection.commands().xadd(Keys.QUEUE, "route", route, "fields", sFields);
        } else {
            redisConnection.commands().xadd(Keys.QUEUE, "route", route, "fields", sFields, "respondTo", respondTo);
        }
    }

    protected int queueTaskWithRequiredPermit(String route, String sFields, String permitKey, String respondTo) {
        RedisEval eval = redisConnection.eval()
                .cachedScript(Scripts.QUEUE_TASK_WITH_REQUIRED_PERMIT)
                .addKeys(Keys.QUEUE, Keys.BACKLOG, permitKey + ".available", permitKey + ".backlog")
                .addArgs(route, sFields, permitKey);

        if (respondTo != null) {
            eval.addArg(respondTo);
        }

        return eval.returnInt();
    }

    protected void queueRecurringTask(String route, String recurKey, String sFields, long interval) {
        redisConnection.eval()
                .cachedScript(Scripts.QUEUE_RECURRING_TASK)
                .addKeys(Keys.QUEUE, Keys.SCHEDULE, Keys.RECUR_DATA)
                .addArgs(route, recurKey, sFields, String.valueOf(interval),
                        String.valueOf(procrastinator.getCurrentTimeMillis() + interval))
                .returnInt();
    }

    /**
     *
     * @param route
     * @param sFields
     * @param interval
     * @param permitKey
     * @return 0 if task is backlogged, 1 if task is queued
     */
    protected int queueRecurringTaskWithRequiredPermit(String route, String recurKey, String sFields, long interval, String permitKey) {
        return redisConnection.eval()
                .cachedScript(Scripts.QUEUE_RECURRING_TASK_WITH_REQUIRED_PERMIT)
                .addKeys(Keys.QUEUE, Keys.SCHEDULE, Keys.RECUR_DATA, Keys.BACKLOG, permitKey + ".available", permitKey + ".backlog")
                .addArgs(route, recurKey, sFields, String.valueOf(interval),
                        String.valueOf(procrastinator.getCurrentTimeMillis() + interval), permitKey)
                .returnInt();
    }


    protected void scheduleTask(String route, long targetTs, String sFields, String respondTo) {
        RedisEval eval = redisConnection.eval()
                .cachedScript(Scripts.SCHEDULE_TASK)
                .addKeys(Keys.SCHEDULE, Keys.FUTURES)
                .addArgs(route, sFields, String.valueOf(targetTs));

        if (respondTo != null) {
            eval.addArg(respondTo);
        }

        eval.returnInt();
    }

    protected void scheduleTaskWithPermit(String route, long targetTs, String sFields, String permitKey, String respondTo) {
        RedisEval eval = redisConnection.eval()
                .cachedScript(Scripts.SCHEDULE_TASK_WITH_PERMIT)
                .addKeys(Keys.SCHEDULE, Keys.FUTURES)
                .addArgs(route, sFields, String.valueOf(targetTs), permitKey);

        if (respondTo != null) {
            eval.addArg(respondTo);
        }

        eval.returnInt();
    }

    protected void scheduleRecurringTask(String route, String recurKey, long targetTs, String sFields,
            long interval, String permitKey) {
        RedisEval eval = redisConnection.eval()
                .cachedScript(Scripts.SCHEDULE_RECURRING_TASK)
                .addKeys(Keys.SCHEDULE, Keys.RECUR_DATA)
                .addArgs(route, recurKey, sFields, String.valueOf(interval), String.valueOf(targetTs));

        if (permitKey != null) {
            eval.addArg(permitKey);
        }

        eval.returnInt();
    }

    protected void retryTask(String taskId, long delay) {
        RedisEval eval = redisConnection.eval()
                .cachedScript(Scripts.RETRY_TASK)
                .addKeys(Keys.QUEUE, Keys.SCHEDULE, Keys.FUTURES)
                .addArg(taskId);

        if (delay > 0) {
            eval.addArg(String.valueOf(procrastinator.getCurrentTimeMillis() + delay));
        }

        eval.returnInt();
    }

    protected boolean submitDmlEvent(long eventId, String stmt, String table, String data) {
        String res = redisConnection.commands().set("dml." + eventId, "t", SetArgs.Builder.nx().ex(600));
        if (res != null) {
            redisConnection.commands().xadd(Keys.QUEUE, "eventId", String.valueOf(eventId), "stmt",
                    stmt, "table", table, "data", data);
            return true;
        } else {
            return false;
        }
    }
}
