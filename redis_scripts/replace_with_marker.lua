local guard_count = tonumber(ARGV[1]) or 0
if guard_count <= 0 or #KEYS <= guard_count then
	return redis.error_reply("invalid lock guards")
end
for index = 1, guard_count do
	if ARGV[1 + index] == "" or redis.call("get", KEYS[index]) ~= ARGV[1 + index] then
		return redis.error_reply("tablecache lock lost")
	end
end

local marker_index = guard_count + 1
local marker = KEYS[marker_index]
local value = ARGV[guard_count + 2]
local ttl = tonumber(ARGV[guard_count + 3])

if ttl == nil or ttl < 0 then
	return redis.error_reply("invalid marker ttl")
end
for i = marker_index + 1, #KEYS do
	if KEYS[i] == marker then
		return redis.error_reply("marker key cannot be deleted")
	end
end

-- SET 失败会立即终止脚本，旧 key 不会进入删除阶段。
if ttl > 0 then
	redis.call("set", marker, value, "PX", ttl)
else
	redis.call("set", marker, value)
end

local deleted = 0
local batch = {}
for i = marker_index + 1, #KEYS do
	batch[#batch + 1] = KEYS[i]
	if #batch >= 256 then
		deleted = deleted + redis.call("unlink", unpack(batch))
		batch = {}
	end
end
if #batch > 0 then
	deleted = deleted + redis.call("unlink", unpack(batch))
end
return deleted
