local guard_count = tonumber(ARGV[1]) or 0
if guard_count <= 0 or #KEYS ~= guard_count + 2 then
	return redis.error_reply("invalid lock guards")
end
for index = 1, guard_count do
	if ARGV[1 + index] == "" or redis.call("get", KEYS[index]) ~= ARGV[1 + index] then
		return redis.error_reply("tablecache lock lost")
	end
end

local hash_key = KEYS[guard_count + 1]
local registry_key = KEYS[guard_count + 2]
local fields_start = guard_count + 2
if #ARGV < fields_start then
	return redis.error_reply("invalid hash fields")
end

local kind = redis.call("type", hash_key)
if type(kind) == "table" then
	kind = kind["ok"]
end
if kind ~= "none" and kind ~= "hash" then
	return redis.error_reply("invalid hash key type")
end

local fields = {}
for index = fields_start, #ARGV do
	if ARGV[index] == "" then
		return redis.error_reply("invalid hash field")
	end
	fields[#fields + 1] = ARGV[index]
end
local deleted = redis.call("hdel", hash_key, unpack(fields))
redis.call("unlink", registry_key)
return deleted
