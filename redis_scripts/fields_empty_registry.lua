if #KEYS ~= 1 or #ARGV ~= 1 or ARGV[1] == "" then
	return redis.error_reply("invalid fields empty lookup")
end

local key = KEYS[1]
local member = ARGV[1]
local now = redis.call("time")
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local result = 0
local score = redis.call("zscore", key, member)
if score ~= false and tonumber(score) > now_ms then
	result = 1
end

local latest = redis.call("zrevrange", key, 0, 0, "withscores")
if #latest == 0 then
	redis.call("unlink", key)
elseif tonumber(latest[2]) <= now_ms then
	redis.call("unlink", key)
else
	local expired = redis.call("zrangebyscore", key, "-inf", now_ms, "limit", 0, 256)
	if #expired > 0 then
		redis.call("zrem", key, unpack(expired))
	end
	redis.call("pexpireat", key, math.floor(tonumber(latest[2])))
end
return result
