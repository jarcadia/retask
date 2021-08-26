package dev.jarcadia;


class Scripts {

	protected static String QUEUE_TASK_WITH_REQUIRED_PERMIT = """
   		-- Keys: queue backlog <permit_key>.available <permit_key>.backlog
        -- Args: route sFields permitKey respondTo?
        
        -- Try to acquire permit
        local permit = redis.call('lpop', KEYS[3])
        
        local args = {'route', ARGV[1], 'fields', ARGV[2]}
        if #ARGV == 4 then
        	table.insert(args, 'respondTo')
        	table.insert(args, ARGV[4])
        end
        
        if permit == false then
        	local backlogId = redis.call('xadd', KEYS[2], '*', unpack(args))
            redis.call('rpush', KEYS[4], backlogId)
			return 0
        else
            -- Queue task
            redis.call('xadd', KEYS[1], '*', 'permitKey', ARGV[3], 'permit', permit,  unpack(args))
            return 1
        end
    """;

	protected static String QUEUE_RECURRING_TASK = """
        -- Keys: queue schedule recurData
        -- Args: route recurKey sFields interval recurTs
        
        -- Insert/update recur data
        redis.call('hmset', KEYS[3], ARGV[2] .. '.route', ARGV[1],
                                     ARGV[2] .. '.fields', ARGV[3],
        				             ARGV[2] .. '.interval', ARGV[4])
        
        -- Schedule recurrence
        redis.call('zadd', KEYS[2], tonumber(ARGV[5]), '*' .. ARGV[2])
        
		-- Queue task
		redis.call('xadd', KEYS[1], '*', 'route', ARGV[1], 'recurKey', ARGV[2], 'fields', ARGV[3])
		
		return 1
    """;

    protected static String QUEUE_RECURRING_TASK_WITH_REQUIRED_PERMIT = """
        -- Keys: queue schedule recurData backlog <permit_key>.available <permit_key>.backlog
        -- Args: route recurKey sFields interval recurTs permitKey
        
        -- Insert/update recur data
        redis.call('hmset', KEYS[3], ARGV[2] .. '.route', ARGV[1],
        						     ARGV[2] .. '.fields', ARGV[3],
        				             ARGV[2] .. '.interval', ARGV[4],
        				             ARGV[2] .. '.permitKey', ARGV[6])
        
        -- Schedule recurrence
        redis.call('zadd', KEYS[2], tonumber(ARGV[5]), '*' .. ARGV[2])
        
        -- Try to reserve permit
        local permit = redis.call('lpop', KEYS[5])
        
        if permit == false then
        	local backlogId = redis.call('xadd', KEYS[4], '*', 'route', ARGV[1], 'recurKey', ARGV[2], 'fields', ARGV[3])
            redis.call('rpush', KEYS[6], backlogId)
			return 0
        else
            -- Queue task
            redis.call('xadd', KEYS[1], '*', 'route', ARGV[1], 'recurKey', ARGV[2], 'fields', ARGV[3], 'permitKey', ARGV[6], 'permit', permit)
            return 1
        end
    """;

	protected static String SCHEDULE_TASK = """
        -- Keys: schedule futures
        -- Args: route sFields targetTs respondTo?
        
        local args = {'route', ARGV[1], 'fields', ARGV[2]}
        
        if #ARGV == 4 then
        	table.insert(args, 'respondTo')
        	table.insert(args, ARGV[4])
        end
        
        -- Insert future
        local futureId = redis.call('xadd', KEYS[2], '*', unpack(args))
        				             
        -- Schedule future
        redis.call('zadd', KEYS[1], tonumber(ARGV[3]), futureId)
        
        return 1
    """;

	protected static String SCHEDULE_TASK_WITH_PERMIT = """
        -- Keys: schedule futures
        -- Args: route sFields targetTs permitKey respondTo?
        
        local args = {'route', ARGV[1], 'permitKey', ARGV[4], 'fields', ARGV[2]}
        
        if #ARGV == 5 then
        	table.insert(args, 'respondTo')
        	table.insert(args, ARGV[5])
        end
        
        -- Insert future
        local futureId = redis.call('xadd', KEYS[2], '*', unpack(args))
        				             
        -- Schedule future
        redis.call('zadd', KEYS[1], tonumber(ARGV[3]), futureId)
        
        return 1
    """;

	protected static String SCHEDULE_RECURRING_TASK = """
        -- Keys: schedule recurData
        -- Args: route recurKey sFields interval targetTs permitKey?
        
        local args = {
        	ARGV[2] .. '.route', ARGV[1],
        	ARGV[2] .. '.fields', ARGV[3],
        	ARGV[2] .. '.interval', ARGV[4],
        }
        
        if #ARGV == 6 then
        	table.insert(args, ARGV[2] .. '.permitKey')
        	table.insert(args, ARGV[6])
        end
        
        -- Insert/update recur data
        redis.call('hmset', KEYS[2], unpack(args))
        				             
        -- Schedule task
        redis.call('zadd', KEYS[1], tonumber(ARGV[5]), '*' .. ARGV[2])
        
        return 1
    """;

	protected static String SET_PERMIT_CAP = """
	 -- Keys: permitCaps backlog <permit_key>.available <permit_key>.backlog queue
	 -- Args: permitKey cap
	
	 local cap = tonumber(ARGV[2])
	 local current = redis.call('hget', KEYS[1], ARGV[1])
	 if current == false then
		 current = 0
	 else
		 current = tonumber(current)
	end
	
	if current > cap then
	
		-- Update the cap
		if cap == 0 then
			redis.call('hdel', KEYS[1], ARGV[1])
		else
			redis.call('hset', KEYS[1], ARGV[1], cap)
		end
		
		-- Remove elements greater than cap from available list
		local permits = redis.call('lpop', KEYS[3], redis.call('llen', KEYS[3]))
		for i=1,#permits do
			if tonumber(permits[i]) < cap then
				redis.call('rpush', KEYS[3], permits[1])
			end
		end
		return cap - current
	elseif current < cap then
	
		-- Update the cap
		redis.call('hset', KEYS[1], ARGV[1], cap)
		
		-- For each new permit, pop backlog or add to available list
		for permit=current,cap-1 do
			local backlogId = redis.call('lpop', KEYS[4])
	
			if backlogId == false then	-- No task backlogged, return permit to available
				redis.call('rpush', KEYS[3], permit)
			else	-- Backlogged task popped
			
				local backlogEntry = redis.call('xrange', KEYS[2], backlogId, backlogId)
				-- TODO handle missing entry (data corruption)
		
				redis.call('xdel', KEYS[2], backlogId)
				local backlogged = backlogEntry[1][2]
		
				-- Add permitKey and permit to backlogged
				table.insert(backlogged, 'permitKey')
				table.insert(backlogged, ARGV[1])
				table.insert(backlogged, 'permit')
				table.insert(backlogged, permit)
		
				redis.call('xadd', KEYS[5], '*', unpack(backlogged))
			end
		end
		return cap - current
	else
		return 0;
	end
    """;

    protected static String RELEASE_PERMIT = """
        -- Keys: permitCaps queue backlog <permit_key>.available <permit_key>.backlog
        -- Args: permitKey permit
        
        local permit = tonumber(ARGV[2])
        
        -- Reconcile with cap, exiting early to destroy this permit (exceeds cap)
        local cap = redis.call('hget', KEYS[1], ARGV[1])
        if cap == false or tonumber(cap) <= permit then
            return -1
        end
		
        -- Pop from the backlog
        local backlogId = redis.call('lpop', KEYS[5])
		
        if backlogId == false then	-- No task backlogged, return permit to available
            redis.call('rpush', KEYS[4], ARGV[2])
            return 0
        else	-- Backlogged task popped
		
            local backlogEntry = redis.call('xrange', KEYS[3], backlogId, backlogId)
            -- TODO handle missing entry (data corruption)
			
            redis.call('xdel', KEYS[3], backlogId)
			
            local backlogged = backlogEntry[1][2]
			
            -- Add permitKey and permit to backlogged
            table.insert(backlogged, 'permitKey')
            table.insert(backlogged, ARGV[1])
            table.insert(backlogged, 'permit')
            table.insert(backlogged, ARGV[2])
			
            redis.call('xadd', KEYS[2], '*', unpack(backlogged))
			
            return 1
        end """;

    protected static String SCHEDULED_TASK_POP = """
  		-- Keys: queue schedule futures recur backlog [...available/backlog]?
  		-- Args: now, offset, limit, crons?
  		
  		local now = tonumber(ARGV[1])
  		local offset = tonumber(ARGV[2])
  		local cutoff = now + offset
  		local limit = tonumber(ARGV[3])
  		
		local due = redis.call('zrangebyscore', KEYS[2], 0, cutoff, 'WITHSCORES', 'LIMIT', 0, limit + 1);
		redis.log(redis.LOG_NOTICE, 'Found ' .. tostring(#due/2) .. ' futures due at cutoff ' .. tostring(cutoff))
		
		if #due == 0 then
			return
		end
		
		-- Build a lookup of permitKeys that are available in this call
		local permitKeys = {}
		for i=6,#KEYS,2 do
			permitKeys[KEYS[i]:sub(0, #KEYS[i]-10)] = true
		end
		
		-- Build a lookup of cron expressions that are available in this call
		local crons = {}
		if #ARGV == 4 then
			crons = cjson.decode(ARGV[4])
		end
		
		local missingPermitKeys = {}
		local cronReqs = {}
		
		for i=1,math.min(limit*2, #due),2 do
		    local member = due[i]
			redis.log(redis.LOG_NOTICE, 'Processing scheduled member' .. member)
			local targetTs = due[i+1]
			
			local task = {}
			local permitKey = nil -- nil means none, false means inaccessible
			
			if member:sub(1, 1) == '*' then
				local recurKey = member:sub(2)
				redis.log(redis.LOG_NOTICE, 'Processing recurring task ' .. recurKey .. ' (' .. targetTs .. ')')
				
				local recurData = redis.call('hmget', KEYS[4], recurKey .. '.route',
				 											   recurKey .. '.interval',
                                                               recurKey .. '.lock',
                                                               recurKey .. '.permitKey',
                                                               recurKey .. '.fields',
                                                               recurKey .. '.cron')

                if recurData[1] == false then
                	-- Route is no longer present in recurData for this recurKey (indicates recurrence was cancelled)
                	redis.call('zrem', KEYS[2], member)
                else
                
                	-- Determine nextTs using INTERVAL or CRON (one or the other must exist)
                	local nextTs = false
                	local interval = recurData[2]
                	
					redis.log(redis.LOG_NOTICE, 'Scheduling next recurrence of ' .. recurKey .. ' in ' .. tostring(targetTs) .. ' + ' .. tostring(interval))
					
                	if interval == false then	-- nextTs is based on CRON
                		local cronExpr = recurData[6]
                		
						redis.log(redis.LOG_NOTICE, 'Scheduling next cron ' .. recurKey .. ' (' .. cronExpr .. ')')
                		
                		-- Assign nextTs by looking up cron expression in crons map
                		nextTs = crons[cronExpr]
                		
                		if nextTs == nil then -- Cron expression wasn't passed in and hasn't been added to cronReqs
                		
                			-- Indicate nextTs cannot be calculated
                			nextTs = false
                			
                			-- Request cron calculation from backend
                			table.insert(cronReqs, cronExpr)
                			
                			-- Add to crons map as false so this block won't trigger again for the same cron expr
                			crons[cronExpr] = false
                		end
                	else 	-- nextTs is based on interval
						nextTs = targetTs + tonumber(interval)
                	end
                	
                	-- If permitKey is present, check accessibility and request if necessary
					if recurData[4] ~= false then
						permitKey = recurData[4]
						
						-- Check permitKey accessibility
						if permitKeys[permitKey] == false then	-- permitKey is known to be inaccessible
							permitKey = false
						elseif permitKeys[permitKey] == nil then -- permitKey is discovered to be inaccessible
							permitKeys[permitKey] = false -- set to false to prevent duplicates
							table.insert(missingPermitKeys, permitKey)
							permitKey = false
						end
					end
                	
                	if nextTs ~= false and permitKey ~= false then
						local lock = recurData[3]

						-- Schedule next recurrence
						if nextTs < now then
							local diff = now - nextTs + 1
							local skip = math.ceil(diff / interval) + 1
							nextTs = nextTs + skip * interval
						end
						redis.call('zadd', KEYS[2], nextTs, member)
						redis.log(redis.LOG_NOTICE, 'Scheduled recurrence ' .. recurKey .. ' at ' .. tostring(nextTs))
						
						-- If lock is available, claim it and populate task
						if lock == false then
							redis.call('hset', KEYS[4], recurKey .. '.lock', '')
							
							table.insert(task, 'route')
							table.insert(task, recurData[1])
							table.insert(task, 'recurKey')
							table.insert(task, recurKey)
							table.insert(task, 'targetTs')
							table.insert(task, targetTs)
							table.insert(task, 'fields')
							table.insert(task, recurData[5])
							
							if permitKey ~= nil then
								table.insert(task, 'permitKey')
								table.insert(task, permitKey)
							end
						end
					end
                end
			else
			
				local futureEntry = redis.call('xrange', KEYS[3], member, member)
				-- TODO handle if futureEntry is not found (would indicate a data corruption)
				
				-- Assign future key/vals to task
				task = futureEntry[1][2]
			
				-- If permitKey is present, check accessibility and request if necessary
				if task[3] == 'permitKey' then
					permitKey = task[4]
					
					-- Check if permitKey accessibility
					if permitKeys[permitKey] == false then	-- permitKey is known to be inaccessible
						permitKey = false
					elseif permitKeys[permitKey] == nil then -- permitKey is discovered to be inaccessible
						permitKeys[permitKey] = false -- set to false to prevent duplicates
						table.insert(missingPermitKeys, permitKey)
						permitKey = false
					end
				end
			
				if permitKey ~= false then	-- If permitKey is nil or accessible
					redis.call('zrem', KEYS[2], member)
					redis.call('xdel', KEYS[3], member)
					
					-- Populate task based on future stream entry
					task = futureEntry[1][2]
					table.insert(task, 'targetTs')
					table.insert(task, targetTs)
				end
			end
			
			if #task > 0 and permitKey ~= false then	-- task has been populated and permitKey is nil or accessible
				redis.log(redis.LOG_NOTICE, 'Will queue if ' .. table.concat(task, ' '))
				
				if permitKey == nil then	-- This task does not require a permit
				
					-- Queue the task
					redis.call('xadd', KEYS[1], '*', unpack(task))
				
				else	-- Task requires a permit
					
					local permit = redis.call('lpop', permitKey .. '.available')
					if permit == false then
						-- Backlog the task
						local backlogId = redis.call('xadd', KEYS[5], '*', unpack(task))
						redis.call('rpush', permitKey .. '.backlog', backlogId)
					else
						-- Add acquired permit to the task object
						table.insert(task, 'permit')
						table.insert(task, permit)
						
						-- Queue the task
						redis.call('xadd', KEYS[1], '*', unpack(task))
					end
				end
            end
		end
		
		local response = {}
		if (#due/2 > limit) then
			table.insert(response, '+')
		else
			table.insert(response, false)
		end
		
		if (#missingPermitKeys > 0 or #cronReqs > 0) then
			table.insert(response, tostring(#missingPermitKeys))
			
			for i,permitKey in ipairs(missingPermitKeys) do
				table.insert(response, permitKey)
			end
			
			for i,cronReq in ipairs(cronReqs) do
				table.insert(response, cronReq)
			end
		end
		
		return response
	""";

	protected static String RETRY_TASK = """
  		-- Keys: queue schedule futures
  		-- Args: taskId targetTs?
  		
  		local task = redis.call('xrange', KEYS[1], ARGV[1], ARGV[1])
  		-- TODO handle missing task (indicates data corruption)
  		local taskArgs = task[1][2]
  		
  		-- Find index of 'attempt' key (if exists) in taskArgs
  		local attemptIdx = -1
        for i=1,#taskArgs do
            if taskArgs[i] == 'attempt' then
				attemptIdx = i
				break
			end
		end
		
		-- Increment or add 'attempt' to taskArgs
		if attemptIdx == -1 then
			table.insert(taskArgs, 'attempt')
			table.insert(taskArgs, '2')
		else
			taskArgs[attemptIdx+1] = tonumber(taskArgs[attemptIdx+1]) + 1
		end
  		
  		-- Queue or schedule retry
  		if #ARGV == 1 then
  		  redis.call('xadd', KEYS[1], '*', unpack(taskArgs))
  		else
  			local futureId = redis.call('xadd', KEYS[3], '*', unpack(taskArgs))
  			redis.call('zadd', KEYS[2], tonumber(ARGV[2]), futureId)
  		end
  		
  		return 1
	""";

	protected static String SUBMIT_DML_EVENT = """
		-- Keys: queue dmlKey
  		-- Args: eventNum stmt table
  		
  		local submitted = tonumber(ARGS[1])
  		local current = redis.call('get', KEYS[2])
  		
  		if current + 1 == submitted then
  		
  		
  		elseif submitted > current then
  		
  		end
  		
  		

	""";

}
