local key = KEYS[1]
local kind = ARGV[1]
local ttl = tonumber(ARGV[2])

if ttl == nil or ttl < 0 then
	return redis.error_reply("invalid ttl")
end
if kind ~= "hash" and kind ~= "set" and kind ~= "zset" then
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

local function apply_ttl()
	if ttl > 0 then
		redis.call("pexpire", key, ttl)
	else
		redis.call("persist", key)
	end
end

local function flush(command, values)
	if #values > 0 then
		redis.call(command, key, unpack(values))
		-- 后续批次发生运行时错误时，已写入数据仍保留调用方要求的TTL。
		apply_ttl()
	end
end

local command = "zadd"
local step = 1
if kind == "hash" then
	command = "hset"
	step = 2
elseif kind == "set" then
	command = "sadd"
else
	step = 2
end

local batch = {}
for i = 3, #ARGV, step do
	batch[#batch + 1] = ARGV[i]
	if step == 2 then
		batch[#batch + 1] = ARGV[i + 1]
	end
	if #batch >= 256 then
		flush(command, batch)
		batch = {}
	end
end
flush(command, batch)
apply_ttl()
return 1
