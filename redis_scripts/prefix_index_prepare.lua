local guard_count = tonumber(ARGV[1]) or 0
if guard_count <= 0 or #KEYS ~= guard_count + 2 then
	return redis.error_reply("invalid lock guards")
end
for index = 1, guard_count do
	if ARGV[1 + index] == "" or redis.call("get", KEYS[index]) ~= ARGV[1 + index] then
		return redis.error_reply("tablecache lock lost")
	end
end

local offset = guard_count + 2
local active = KEYS[guard_count + 1]
local manifest = KEYS[guard_count + 2]
local ttl = tonumber(ARGV[offset])
local empty_bitmap = ARGV[offset + 1]
if ttl == nil or ttl <= 0 or empty_bitmap == nil or empty_bitmap == "" then
	return redis.error_reply("invalid prefix index prepare arguments")
end

local kind = redis.call("type", active)
if type(kind) == "table" then
	kind = kind["ok"]
end
local valid = kind == "string" and redis.call("strlen", active) >= 1 and redis.call("strlen", active) <= 8
if not valid then
	redis.call("unlink", active)
	redis.call("unlink", manifest)
	redis.call("set", active, empty_bitmap, "PX", ttl)
	return 1
end
redis.call("pexpire", active, ttl)
return 0
