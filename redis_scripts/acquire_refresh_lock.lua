local key = KEYS[1]
local owner = ARGV[1]
local ttl = tonumber(ARGV[2])

if owner == "" or ttl == nil or ttl <= 0 then
	return redis.error_reply("invalid refresh lock arguments")
end
local locked = redis.call("set", key, owner, "PX", ttl, "NX")
if locked then
	return {1, owner}
end
local current = redis.call("get", key)
if current == false then
	current = ""
end
if current == owner then
	redis.call("pexpire", key, ttl)
	return {1, owner}
end
return {0, current}
