-- 原子校验锁 owner，并提交至多一个写入条目及同槽旧 key 删除。

local guard_count = tonumber(ARGV[1]) or 0
if guard_count <= 0 or #KEYS < guard_count then
	return redis.error_reply("invalid lock guards")
end
for index = 1, guard_count do
	if ARGV[1 + index] == "" or redis.call("get", KEYS[index]) ~= ARGV[1 + index] then
		return redis.error_reply("tablecache lock lost")
	end
end

local offset = guard_count + 2
local has_write = tonumber(ARGV[offset]) or 0
local kind = ARGV[offset + 1]
local ttl = tonumber(ARGV[offset + 2])
local overwrite = tonumber(ARGV[offset + 3]) or 0
local value_count = tonumber(ARGV[offset + 4]) or 0
local values_start = offset + 5
local target_index = guard_count + 1
local temporary_index = target_index
local delete_start = target_index

if has_write ~= 0 and has_write ~= 1 then
	return redis.error_reply("invalid guarded write flag")
end
if has_write == 1 then
	if ttl == nil or ttl < 0 or value_count < 0 or #ARGV ~= values_start + value_count - 1 then
		return redis.error_reply("invalid guarded write arguments")
	end
	if kind ~= "string" and kind ~= "hash" and kind ~= "list" and kind ~= "set" and kind ~= "zset" then
		return redis.error_reply("unsupported guarded write type")
	end
	if (kind == "string" or kind == "list") and overwrite ~= 1 then
		return redis.error_reply("unsupported incremental write type")
	end
	if kind == "string" and value_count ~= 1 then
		return redis.error_reply("invalid string arguments")
	end
	if (kind == "hash" or kind == "zset") and (value_count % 2) ~= 0 then
		return redis.error_reply("invalid paired collection arguments")
	end
	if kind == "zset" then
		for index = values_start, values_start + value_count - 1, 2 do
			local score = tonumber(ARGV[index])
			if score == nil or score ~= score then
				return redis.error_reply("invalid zset score")
			end
		end
	end
	if kind ~= "string" and overwrite == 1 then
		temporary_index = target_index + 1
		if KEYS[target_index] == KEYS[temporary_index] then
			return redis.error_reply("temporary key equals target")
		end
		delete_start = temporary_index + 1
	else
		delete_start = target_index + 1
	end
	for index = delete_start, #KEYS do
		if KEYS[index] == KEYS[target_index] or (temporary_index ~= target_index and KEYS[index] == KEYS[temporary_index]) then
			return redis.error_reply("guarded write key cannot be deleted")
		end
	end
elseif #ARGV ~= offset + 4 then
	return redis.error_reply("invalid guarded delete arguments")
end

local function apply_ttl(key)
	if ttl > 0 then
		redis.call("pexpire", key, ttl)
	else
		redis.call("persist", key)
	end
end

local function flush(command, key, values, temporary)
	if #values > 0 then
		redis.call(command, key, unpack(values))
		if temporary then
			-- 后续批次运行时错误时，临时 key 也会在短窗口内自动回收。
			redis.call("pexpire", key, 60000)
		else
			-- 增量写每批立即应用 TTL，避免后续批失败留下永久或旧 TTL 的部分数据。
			apply_ttl(key)
		end
	end
end

if has_write == 1 then
	local target = KEYS[target_index]
	if kind == "string" then
		if ttl > 0 then
			redis.call("set", target, ARGV[values_start], "PX", ttl)
		else
			redis.call("set", target, ARGV[values_start])
		end
	else
		local destination = target
		if overwrite == 1 then
			destination = KEYS[temporary_index]
			redis.call("unlink", destination)
		end
		local command = "zadd"
		local step = 1
		if kind == "hash" then
			command = "hset"
			step = 2
		elseif kind == "list" then
			command = "rpush"
		elseif kind == "set" then
			command = "sadd"
		else
			step = 2
		end
		local batch = {}
		for index = values_start, values_start + value_count - 1, step do
			batch[#batch + 1] = ARGV[index]
			if step == 2 then
				batch[#batch + 1] = ARGV[index + 1]
			end
			if #batch >= 256 then
				flush(command, destination, batch, overwrite == 1)
				batch = {}
			end
		end
		flush(command, destination, batch, overwrite == 1)
		if overwrite == 1 then
			if value_count == 0 then
				redis.call("unlink", target)
			else
				if redis.call("exists", destination) ~= 1 then
					return redis.error_reply("temporary collection missing")
				end
				redis.call("unlink", target)
				redis.call("rename", destination, target)
				apply_ttl(target)
			end
		else
			apply_ttl(target)
		end
	end
end

local deleted = 0
local batch = {}
for index = delete_start, #KEYS do
	batch[#batch + 1] = KEYS[index]
	if #batch >= 256 then
		deleted = deleted + redis.call("unlink", unpack(batch))
		batch = {}
	end
end
if #batch > 0 then
	deleted = deleted + redis.call("unlink", unpack(batch))
end
return deleted
