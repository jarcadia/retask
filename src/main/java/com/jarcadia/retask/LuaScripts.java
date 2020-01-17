package com.jarcadia.retask;

public class LuaScripts {
	
	/**
	 * Sets task metadata and insert tasks into back of queue
	 * Keys: tasksHash, recurAuthHash, taskId
	 * Args: field, value [... field value]
	 */
	public static String QUEUE_TASK = """
        redis.call('hmset', KEYS[2], unpack(ARGV));
        redis.call('rpush', KEYS[1], KEYS[2])
	""";
	
	/**
	 * Sets recurring task metadata and insert tasks into back of queue
	 * Keys: tasksHash, taskId, recurAuthHash
	 * Args: 'recurKey', recurKey, 'authorityKey', authKey, field, value [... field value]
	 */
	public static String QUEUE_TASK_WITH_RECUR = """
        redis.call('hmset', KEYS[2], unpack(ARGV));
        redis.call('hset', KEYS[3], ARGV[2], ARGV[4])
        redis.call('rpush', KEYS[1], KEYS[2])
	""";
	
	/**
	 * Sets non-recurring task metadata and schedules task
	 * Keys: scheduledHash, taskId 
	 * Args: 'targetTimestamp', targetTimestamp, field, value [... field value]
	 */
	public static String SCHEDULE_TASK = """
        redis.call('hmset', KEYS[2], unpack(ARGV));
        redis.call('zadd', KEYS[1], ARGV[2], KEYS[2])
	""";
	
	/**
	 * Sets recurring task metadata and schedules task
	 * Keys: scheduledHash, taskId, recurAuthHash
	 * Args: 'targetTimestamp', targetTimestamp, 'recurKey', recurKey, 'authorityKey', authKey, field, value [... field value]
	 */
	public static String SCHEDULE_TASK_WITH_RECUR = """
        redis.call('hmset', KEYS[2], unpack(ARGV));
        redis.call('hset', KEYS[3], ARGV[4], ARGV[6])
        redis.call('zadd', KEYS[1], ARGV[2], KEYS[2])
	""";
	
	public static String TRIGGER_SUBSEQUENT_TASKS = """
		-- Trigger subsequent tasks (setup with .after(this.id))
        -- Keys: tasksList afterList
        local subsequentTasks = redis.call('lrange', KEYS[2], 0, -1)
        if (#subsequentTasks > 0) then
            redis.call('rpush', KEYS[1], unpack(subsequentTasks));
        end
	""";
	
	public static String RETRY_TASK = """
        -- Retry task after failure
        -- Keys: scheduledZSetKey taskId nextId
        -- Args: nextTimestamp
        redis.call('restore', KEYS[3], 0, redis.call('dump', KEYS[2]));
        redis.call('hincrby', KEYS[3], 'attempt', 1);
        redis.call('hset', KEYS[3], 'targetTimestamp', ARGV[1]);
        redis.call('hdel', KEYS[3], 'recurKey', 'authorityKey', 'recurInterval');
        redis.call('zadd', KEYS[1], ARGV[1], KEYS[3])
	""";

	public static String RECUR_TASK = """
        -- Recur a task
        -- Keys: taskQueue scheduledTaskZSet recurLockHashKey recurAuthorityHashKey recurKey taskId nextTaskId
        -- Args: targetTimestamp, nextTimestamp, authorityKey
        -- Returns: true or false

        local masterAuthKey = redis.call('hget', KEYS[4], ARGV[1])

        if (masterAuthKey == ARGV[4]) then

            -- Setup recurrence 
            redis.call('restore', KEYS[6], 0, redis.call('dump', KEYS[5]));
            redis.call('hset', KEYS[6], 'targetTimestamp', ARGV[3]);
            redis.call('hdel', KEYS[6], 'dependents');
            redis.call('zadd', KEYS[2], ARGV[3], KEYS[6])
             
            -- Update lock
            redis.call('hset', KEYS[3], ARGV[1], "1");

            return true;

        --redis.call('hset', KEYS[4], ARGV[1], ARGV[1]);
        else 
            -- this task is no longer the authority for this recurKey
            redis.call('del', KEYS[5]);
            return false;
        end
	""";

	public static String SET_AVAILABLE_PERMITS = """
        local function contains(list, val)
            for index, value in ipairs(list) do
                if (val == value) then
                    return true
                end
            end
            return false
        end
        local available = redis.call('lrange', KEYS[1], 0, -1);
        local assigned = redis.call('lrange', KEYS[2], 0, -1);
        local expected = {};
        for i=1,ARGV[1],1 do
            expected[i] = tostring(i);
        end
        for i, permit in ipairs(expected) do
            if (not contains(available, permit) and not contains(assigned, permit)) then
                redis.call('lpush', KEYS[1], permit);
            end
        end
        for i,permit in ipairs(available) do
            if (not contains(expected, permit)) then
                redis.call('lrem', KEYS[1], 0, permit);
            end
        end for i,permit in ipairs(assigned) do
            if (not contains(expected, permit)) then
                return redis.error_reply('Cannot remove permit that is currently in use');
            end
        end
        return redis.status_reply('OK');
	""";

	public static String ACQUIRE_PERMIT_OR_BACKLOG = """
        local permit = redis.call('rpoplpush', KEYS[1], KEYS[2]);
        if (permit) then
            return tonumber(permit);
        else
            redis.call('lpush', KEYS[3], KEYS[4]);
            return nil;
        end
	""";

	public static String RELEASE_PERMIT = """
        -- Remove permit from assigned
        redis.call('lrem', KEYS[2], -1, ARGV[1]);
        -- Add permit to available
        redis.call('lpush', KEYS[1], ARGV[1]);
        -- pop backlogged
        local backlogged = redis.call('rpop', KEYS[3]);
        -- push backlogged to front of task queue
        if (backlogged) then
            redis.call('rpush', KEYS[4], backlogged);
        end
	""";

	public static String GET_AVAILABLE_PERMITS = """
        -- Return the queue length for a permit. Negative number indicates how many permits are available, > 0 indicates backlog queue length
        -- Keys: available backlog
        local available = redis.call('llen', KEYS[1]);
        if (available > 0) then
            return available;
        else
            return -redis.call('llen', KEYS[2]);
        end
	""";
	
	public static String SCHEDULED_TASK_POLL = """
		local due = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1]);
        for i=1, #due do
            redis.call('zrem', KEYS[1], due[i]);
        end
        return due;
	""";

}
