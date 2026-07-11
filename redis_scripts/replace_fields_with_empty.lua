-- 原子删除本次请求的 Hash 字段，并登记字段组合空值到期时间。

local guard_count = tonumber(ARGV[1]) or 0
if guard_count <= 0 or #KEYS < guard_count + 2 then
	return redis.error_reply("invalid lock guards")
end
for index = 1, guard_count do
	if ARGV[1 + index] == "" or redis.call("get", KEYS[index]) ~= ARGV[1 + index] then
		return redis.error_reply("tablecache lock lost")
	end
end

local hash_key = KEYS[guard_count + 1]
local registry_key = KEYS[guard_count + 2]
local member = ARGV[guard_count + 2]
local ttl = tonumber(ARGV[guard_count + 3]) or 0
local fields_start = guard_count + 4

if member == "" or ttl <= 0 or #ARGV < fields_start then
	return redis.error_reply("invalid fields empty marker")
end

local fields = {}
for index = fields_start, #ARGV do
	if ARGV[index] == "" then
		return redis.error_reply("invalid hash field")
	end
	fields[#fields + 1] = ARGV[index]
end

local function key_type(key)
	local kind = redis.call("type", key)
	if type(kind) == "table" then
		return kind["ok"]
	end
	return kind
end

local hash_type = key_type(hash_key)
if hash_type ~= "none" and hash_type ~= "hash" then
	return redis.error_reply("invalid hash key type")
end
local registry_type = key_type(registry_key)
if registry_type ~= "none" and registry_type ~= "zset" then
	return redis.error_reply("invalid fields registry type")
end

local now = redis.call("time")
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expires_at = now_ms + ttl

redis.call("hdel", hash_key, unpack(fields))
local expired = redis.call("zrangebyscore", registry_key, "-inf", now_ms, "limit", 0, 256)
if #expired > 0 then
	redis.call("zrem", registry_key, unpack(expired))
end
redis.call("zadd", registry_key, expires_at, member)
local latest = redis.call("zrevrange", registry_key, 0, 0, "withscores")
redis.call("pexpireat", registry_key, math.floor(tonumber(latest[2])))
return 1
