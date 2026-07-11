local guard_count = tonumber(ARGV[1]) or 0
if #KEYS ~= guard_count + 1 then
	return redis.error_reply("invalid lock guards")
end
for index = 1, guard_count do
	if ARGV[1 + index] == "" or redis.call("get", KEYS[index]) ~= ARGV[1 + index] then
		return redis.error_reply("tablecache lock lost")
	end
end

local offset = guard_count + 2
local shard = KEYS[guard_count + 1]
local ttl = tonumber(ARGV[offset])
local retention = tonumber(ARGV[offset + 1])
local require_sentinel = ARGV[offset + 2] == "1"
local prune = ARGV[offset + 3] == "1"
local sentinel = ARGV[offset + 4]
local sentinel_score = tonumber(ARGV[offset + 5])
local members_start = offset + 6

if ttl == nil or ttl <= 0 or retention == nil or retention <= 0 or sentinel_score == nil then
	return redis.error_reply("invalid prefix index ttl")
end
if require_sentinel and redis.call("zscore", shard, sentinel) == false then
	return redis.error_reply("prefix index shard missing sentinel")
end

local now = redis.call("time")
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local members = {sentinel_score, sentinel}
for i = members_start, #ARGV do
	members[#members + 1] = now_ms
	members[#members + 1] = ARGV[i]
end
redis.call("zadd", shard, unpack(members))
if prune or #ARGV >= members_start then
	local expired = redis.call("zrangebyscore", shard, "-inf", now_ms - retention, "limit", 0, 256)
	if #expired > 0 then
		redis.call("zrem", shard, unpack(expired))
	end
end
redis.call("pexpire", shard, ttl)
return now_ms
