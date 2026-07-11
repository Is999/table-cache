local target = KEYS[1]
local temporary = KEYS[2]
local kind = ARGV[1]
local ttl = tonumber(ARGV[2])

if target == temporary then
	return redis.error_reply("temporary key equals target")
end
if ttl == nil or ttl < 0 then
	return redis.error_reply("invalid ttl")
end
if kind ~= "hash" and kind ~= "list" and kind ~= "set" and kind ~= "zset" then
	return redis.error_reply("unsupported collection type: " .. kind)
end
if kind == "hash" and ((#ARGV - 2) % 2) ~= 0 then
	return redis.error_reply("invalid hash arguments")
end
if kind == "zset" then
	if ((#ARGV - 2) % 2) ~= 0 then
		return redis.error_reply("invalid zset arguments")
	end
	for i = 3, #ARGV, 2 do
		local score = tonumber(ARGV[i])
		if score == nil or score ~= score then
			return redis.error_reply("invalid zset score")
		end
	end
end

redis.call("unlink", temporary)

local function flush(command, values)
	if #values > 0 then
		redis.call(command, temporary, unpack(values))
		-- 中途运行时错误只会留下短 TTL 临时 key，目标旧值保持不变。
		redis.call("pexpire", temporary, 60000)
	end
end

if kind == "hash" then
	local batch = {}
	for i = 3, #ARGV, 2 do
		batch[#batch + 1] = ARGV[i]
		batch[#batch + 1] = ARGV[i + 1]
		if #batch >= 256 then
			flush("hset", batch)
			batch = {}
		end
	end
	flush("hset", batch)
elseif kind == "list" then
	local batch = {}
	for i = 3, #ARGV do
		batch[#batch + 1] = ARGV[i]
		if #batch >= 256 then
			flush("rpush", batch)
			batch = {}
		end
	end
	flush("rpush", batch)
elseif kind == "set" then
	local batch = {}
	for i = 3, #ARGV do
		batch[#batch + 1] = ARGV[i]
		if #batch >= 256 then
			flush("sadd", batch)
			batch = {}
		end
	end
	flush("sadd", batch)
else
	local batch = {}
	for i = 3, #ARGV, 2 do
		batch[#batch + 1] = ARGV[i]
		batch[#batch + 1] = ARGV[i + 1]
		if #batch >= 256 then
			flush("zadd", batch)
			batch = {}
		end
	end
	flush("zadd", batch)
end

if #ARGV == 2 then
	redis.call("unlink", target)
	return 1
end
if ttl > 0 then
	redis.call("pexpire", temporary, ttl)
end
if redis.call("exists", temporary) ~= 1 then
	return redis.error_reply("temporary collection missing")
end
redis.call("unlink", target)
redis.call("rename", temporary, target)
if ttl == 0 then
	redis.call("persist", target)
end
return 1
