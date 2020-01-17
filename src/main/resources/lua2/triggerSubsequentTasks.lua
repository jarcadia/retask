-- Trigger subsequent tasks (setup with .after(this.id))
-- Keys: tasksList afterList
local subsequentTasks = redis.call('lrange', KEYS[2], 0, -1)
if (#subsequentTasks > 0) then
	redis.call('rpush', KEYS[1], unpack(subsequentTasks));
end