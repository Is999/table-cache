local guard_count = tonumber(ARGV[1]) or 0
if #KEYS ~= guard_count + 2 then
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

if ttl == nil or ttl <= 0 then
	return redis.error_reply("invalid prefix index active ttl")
end

local kind = redis.call("type", active)
if type(kind) == "table" then
	kind = kind["ok"]
end
local active_size = 0
if kind == "string" then
	active_size = redis.call("strlen", active)
end
if kind ~= "none" and (kind ~= "string" or active_size < 1 or active_size > 8) then
	redis.call("unlink", active)
	redis.call("unlink", manifest)
elseif kind == "none" and redis.call("exists", manifest) == 1 then
	redis.call("unlink", manifest)
end

local result = {}
for i = offset + 1, #ARGV do
	local shard = tonumber(ARGV[i])
	if shard == nil or shard < 0 or shard > 63 then
		return redis.error_reply("invalid prefix index shard")
	end
	result[#result + 1] = redis.call("setbit", active, shard, 1)
end
redis.call("pexpire", active, ttl)
return result
