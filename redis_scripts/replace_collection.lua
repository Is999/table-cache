local key = KEYS[1]
local kind = ARGV[1]
local ttl = tonumber(ARGV[2])
if ttl == nil then
	return redis.error_reply("invalid ttl")
end

redis.call("unlink", key)

local function flush(command, values)
	if #values > 0 then
		redis.call(command, key, unpack(values))
	end
end

if kind == "hash" then
	if ((#ARGV - 2) % 2) ~= 0 then
		return redis.error_reply("invalid hash arguments")
	end
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
elseif kind == "zset" then
	if ((#ARGV - 2) % 2) ~= 0 then
		return redis.error_reply("invalid zset arguments")
	end
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
else
	return redis.error_reply("unsupported collection type: " .. kind)
end

if ttl > 0 then
	redis.call("pexpire", key, ttl)
end
return 1
