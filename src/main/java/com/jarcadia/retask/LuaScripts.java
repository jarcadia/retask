package com.jarcadia.retask;

class LuaScripts {
	
	/**
	 * Submits task by setting required metadata and queueing or scheduling as required
	 * Keys: taskHash, scheduledHash, recurAuthHash, taskId
	 * Args: field, value [... field value]
	 */
	protected static String SUBMIT_TASK = """
		local m = {};
        for i=1, #ARGV, 2 do
			if ARGV[i] == 'targetTimestamp' or
			   ARGV[i] == 'recurKey' or
			   ARGV[i] == 'authorityKey' then
                m[ARGV[i]] = ARGV[i+1];
            end
        end
        if m.recurKey ~= nil and m.authorityKey ~= nil then
			redis.call('hset', KEYS[3], m.recurKey, m.authorityKey);
		end
        redis.call('hmset', KEYS[4], unpack(ARGV));
		if m.targetTimestamp ~= nil then
            redis.call('zadd', KEYS[2], m.targetTimestamp, KEYS[4])
        else
            redis.call('rpush', KEYS[1], KEYS[4])
        end
        return m.recurKey;
	""";
	
	protected static String TRIGGER_SUBSEQUENT_TASKS = """
		-- Trigger subsequent tasks (setup with .after(this.id))
        -- Keys: tasksList afterList
        local subsequentTasks = redis.call('lrange', KEYS[2], 0, -1)
        if (#subsequentTasks > 0) then
            redis.call('rpush', KEYS[1], unpack(subsequentTasks));
        end
	""";
	
	protected static String RETRY_TASK = """
        -- Retry task after failure
        -- Keys: scheduledZSetKey taskId nextId
        -- Args: nextTimestamp
        redis.call('restore', KEYS[3], 0, redis.call('dump', KEYS[2]));
        redis.call('hincrby', KEYS[3], 'attempt', 1);
        redis.call('hset', KEYS[3], 'targetTimestamp', ARGV[1]);
        redis.call('hdel', KEYS[3], 'recurKey', 'authorityKey', 'recurInterval');
        redis.call('zadd', KEYS[1], ARGV[1], KEYS[3])
	""";

	protected static String RECUR_TASK = """
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

	protected static String SET_AVAILABLE_PERMITS = """
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

	protected static String ACQUIRE_PERMIT_OR_BACKLOG = """
        local permit = redis.call('rpoplpush', KEYS[1], KEYS[2]);
        if (permit) then
            return tonumber(permit);
        else
            redis.call('lpush', KEYS[3], KEYS[4]);
            return nil;
        end
	""";

	protected static String RELEASE_PERMIT = """
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

	protected static String GET_AVAILABLE_PERMITS = """
        -- Return the queue length for a permit. Negative number indicates how many permits are available, > 0 indicates backlog queue length
        -- Keys: available backlog
        local available = redis.call('llen', KEYS[1]);
        if (available > 0) then
            return available;
        else
            return -redis.call('llen', KEYS[2]);
        end
	""";
	
	protected static String SCHEDULED_TASK_POLL = """
		local due = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1]);
        for i=1, #due do
            redis.call('zrem', KEYS[1], due[i]);
        end
        return due;
	""";
}
