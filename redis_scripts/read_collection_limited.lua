-- 在同一 Redis 执行边界内检查集合规模并读取完整值，避免并发变更绕过读取上限。

local key = KEYS[1]
local kind = ARGV[1]
local limit = tonumber(ARGV[2])
local generation = false

if #KEYS < 1 or #KEYS > 2 then
	return redis.error_reply("invalid collection read keys")
end
if #KEYS == 2 then
	generation = redis.call("get", KEYS[2])
end

if limit == nil or limit <= 0 then
	return redis.error_reply("invalid collection read limit")
end

local size
if kind == "hash" then
	size = redis.call("hlen", key)
elseif kind == "list" then
	size = redis.call("llen", key)
elseif kind == "set" then
	size = redis.call("scard", key)
elseif kind == "zset" then
	size = redis.call("zcard", key)
else
	return redis.error_reply("unsupported collection type: " .. kind)
end

if size == 0 then
	return {"tablecache_collection_miss", generation}
end
if size > limit then
	return {"tablecache_collection_too_large", generation, size}
end

local value
if kind == "hash" then
	value = redis.call("hgetall", key)
elseif kind == "list" then
	value = redis.call("lrange", key, 0, -1)
elseif kind == "set" then
	value = redis.call("smembers", key)
else
	value = redis.call("zrange", key, 0, -1, "withscores")
end
return {"tablecache_collection_hit", generation, value}
