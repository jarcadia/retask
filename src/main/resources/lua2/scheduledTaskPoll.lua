local due = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1]);
for i=1, #due do
    redis.call('zrem', KEYS[1], due[i]);
end
return due;



-- Keys: tasksHash, recurAuthHash, taskId
-- Args: 'recurKey', recurKey, 'authorityKey', authKey, field, value [... field value]

redis.call('hmset', KEYS[2], unpack(args));
redis.call('hset', KEYS[2] )
redis.call('rpush', KEYS[1], KEYS[3])