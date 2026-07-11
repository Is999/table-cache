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
local active_ttl = tonumber(ARGV[offset])
local manifest_ttl = tonumber(ARGV[offset + 1])
local version = ARGV[offset + 2]

if active_ttl == nil or active_ttl <= 0 or manifest_ttl == nil or manifest_ttl <= 0 or version == nil or version == "" then
	return redis.error_reply("invalid prefix index commit arguments")
end

local kind = redis.call("type", active)
if type(kind) == "table" then
	kind = kind["ok"]
end
if kind ~= "string" then
	return 0
end
local size = redis.call("strlen", active)
if size < 1 or size > 8 then
	return 0
end

redis.call("pexpire", active, active_ttl)
redis.call("set", manifest, version, "PX", manifest_ttl)
return 1
