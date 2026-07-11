package tablecache

import (
	"context"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
)

const (
	// redisTopologySingle 表示客户端明确连接到非 Cluster Redis。
	redisTopologySingle int32 = iota + 1
	// redisTopologyCluster 表示客户端实际连接到 Cluster 节点。
	redisTopologyCluster
)

// guardedMutationScript 在同一 Lua 边界内校验锁 owner 并执行真实写删。
var guardedMutationScript = redis.NewScript(guardedMutationLua)

// cleanLockGuards 清理并校验锁 guard，避免重复 key 携带冲突 owner。
func cleanLockGuards(guards []LockGuard) ([]LockGuard, error) {
	if len(guards) == 0 {
		return nil, nil
	}
	if len(guards) == 1 {
		key := strings.TrimSpace(guards[0].Key)
		owner := strings.TrimSpace(guards[0].Owner)
		if key == "" || owner == "" {
			return nil, errors.Errorf("缓存提交锁guard不能为空")
		}
		if key == guards[0].Key && owner == guards[0].Owner {
			return guards, nil
		}
		return []LockGuard{{Key: key, Owner: owner}}, nil
	}
	result := make([]LockGuard, len(guards))
	for index, guard := range guards {
		key := strings.TrimSpace(guard.Key)
		owner := strings.TrimSpace(guard.Owner)
		if key == "" || owner == "" {
			return nil, errors.Errorf("缓存提交锁guard不能为空")
		}
		result[index] = LockGuard{Key: key, Owner: owner}
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Key < result[right].Key
	})
	write := 0
	for _, guard := range result {
		if write > 0 && result[write-1].Key == guard.Key {
			if result[write-1].Owner != guard.Owner {
				return nil, errors.Errorf("缓存提交锁[%s]包含冲突owner", guard.Key)
			}
			continue
		}
		result[write] = guard
		write++
	}
	return result[:write], nil
}

// rejectGuardKeyOverlap 拒绝业务、索引或元信息 key 覆盖本次提交的锁 key。
func rejectGuardKeyOverlap(guards []LockGuard, keys ...string) error {
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		for _, guard := range guards {
			if key == guard.Key {
				return errors.Errorf("缓存变更key不能与锁guard重叠")
			}
		}
	}
	return nil
}

// guardedMutationArgs 构造 Lua 使用的 guard、写入和值参数。
func guardedMutationArgs(guards []LockGuard, entry *preparedEntry) ([]string, []any) {
	valueCount := 0
	keyCount := len(guards)
	if entry != nil {
		keyCount++
		switch entry.typ {
		case TypeString:
			valueCount = 1
		case TypeHash:
			valueCount = len(entry.mapValues) * 2
		case TypeZSet:
			valueCount = len(entry.zValues) * 2
		default:
			valueCount = len(entry.values)
		}
		if entry.typ != TypeString && entry.overwrite {
			keyCount++
		}
	}
	keys := make([]string, 0, keyCount)
	args := make([]any, 0, len(guards)+6+valueCount)
	args = append(args, len(guards))
	for _, guard := range guards {
		keys = append(keys, guard.Key)
		args = append(args, guard.Owner)
	}
	if entry == nil {
		return keys, append(args, 0, "", 0, 0, 0)
	}
	keys = append(keys, entry.key)
	if entry.typ != TypeString && entry.overwrite {
		keys = append(keys, collectionReplaceTempKey(entry.key))
	}
	overwrite := 0
	if entry.overwrite {
		overwrite = 1
	}
	args = append(args, 1, string(entry.typ), ttlMilliseconds(entry.ttl), overwrite, valueCount)
	switch entry.typ {
	case TypeString:
		args = append(args, entry.stringValue)
	case TypeHash:
		fields := make([]string, 0, len(entry.mapValues))
		for field := range entry.mapValues {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			args = append(args, field, entry.mapValues[field])
		}
	case TypeZSet:
		for _, value := range entry.zValues {
			args = append(args, value.Score, value.Member)
		}
	default:
		args = append(args, entry.values...)
	}
	return keys, args
}

// runGuardedMutation 原子提交至多一个写入条目和一批同槽删除 key。
func (s *RedisStore) runGuardedMutation(ctx context.Context, client redis.UniversalClient, guards []LockGuard, entry *preparedEntry, deleteKeys []string) (int64, error) {
	keys, args, err := s.prepareGuardedMutation(guards, entry, deleteKeys)
	if err != nil {
		return 0, errors.Tag(err)
	}
	result, err := guardedMutationScript.Run(ctx, client, keys, args...).Int64()
	if err != nil {
		return 0, errors.Tag(normalizeLockGuardError(err))
	}
	return result, nil
}

// prepareGuardedMutation 完成 guarded Lua 的参数清理与同槽校验。
func (s *RedisStore) prepareGuardedMutation(guards []LockGuard, entry *preparedEntry, deleteKeys []string) ([]string, []any, error) {
	guards, err := cleanLockGuards(guards)
	if err != nil {
		return nil, nil, errors.Tag(err)
	}
	if len(guards) == 0 {
		return nil, nil, errors.Errorf("缓存提交缺少锁guard")
	}
	deleteKeys = uniqueSortedKeys(cleanRedisKeys(deleteKeys))
	keys, args := guardedMutationArgs(guards, entry)
	keys = append(keys, deleteKeys...)
	if err := rejectGuardKeyOverlap(guards, keys[len(guards):]...); err != nil {
		return nil, nil, errors.Tag(err)
	}
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot(keys); err != nil {
			return nil, nil, errors.Tag(err)
		}
	}
	return keys, args, nil
}

// writePreparedEntriesGuarded 用 Pipeline 批量发送逐条原子 guard 脚本。
func (s *RedisStore) writePreparedEntriesGuarded(ctx context.Context, guards []LockGuard, entries []preparedEntry) error {
	if len(entries) == 0 {
		return nil
	}
	var err error
	guards, err = cleanLockGuards(guards)
	if err != nil {
		return errors.Tag(err)
	}
	if len(guards) == 0 {
		return errors.Errorf("缓存提交缺少锁guard")
	}
	for start := 0; start < len(entries); start += defaultWriteBatchChunkSize {
		end := min(start+defaultWriteBatchChunkSize, len(entries))
		if err := s.writePreparedEntriesGuardedChunk(ctx, guards, entries[start:end], true); err != nil {
			if !strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT") {
				return errors.Tag(normalizeLockGuardError(err))
			}
			// 全量替换只允许 overwrite，脚本缓存丢失后整批原文重放是幂等的。
			if retryErr := s.writePreparedEntriesGuardedChunk(ctx, guards, entries[start:end], false); retryErr != nil {
				return errors.Tag(normalizeLockGuardError(retryErr))
			}
		}
	}
	return nil
}

// writePreparedEntriesGuardedChunk 批量发送逐条原子脚本；正常路径只传一次 Lua 原文。
func (s *RedisStore) writePreparedEntriesGuardedChunk(ctx context.Context, guards []LockGuard, entries []preparedEntry, useSHA bool) error {
	pipe := s.client.Pipeline()
	cmds := make([]pipelineCmd, 0, len(entries))
	for index, entry := range entries {
		keys, args := guardedMutationArgs(guards, &entry)
		if err := rejectGuardKeyOverlap(guards, keys[len(guards):]...); err != nil {
			return errors.Tag(err)
		}
		if s.knownDistributedClient() {
			if err := validateSameRedisSlot(keys); err != nil {
				return errors.Tag(err)
			}
		}
		var cmd redis.Cmder
		if useSHA && index > 0 {
			cmd = pipe.EvalSha(ctx, guardedMutationScript.Hash(), keys, args...)
		} else {
			cmd = pipe.Eval(ctx, guardedMutationLua, keys, args...)
		}
		cmds = append(cmds, pipelineCmd{key: entry.key, cmd: cmd})
	}
	if err := execAndCheckPipeline(ctx, pipe, cmds); err != nil {
		return errors.Tag(&PipelineExecError{
			Operation: "guarded_write_batch", FailedKeys: failedPipelineKeys(cmds), Cause: err,
		})
	}
	return nil
}

// normalizeLockGuardError 把 Lua owner 不匹配收口为稳定业务错误。
func normalizeLockGuardError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "tablecache lock lost") {
		return errors.Wrapf(ErrRefreshLockLost, "缓存提交锁owner已失效")
	}
	return err
}

// knownDistributedClient 判断无需探测即可识别且需要 slot 约束的分布式客户端。
func (s *RedisStore) knownDistributedClient() bool {
	if s == nil || s.client == nil {
		return false
	}
	switch s.client.(type) {
	case *redis.ClusterClient, redisMasterScanner:
		return true
	default:
		return false
	}
}

// isRingTopology 识别必须在 Store 校验期拒绝的 Ring 拓扑；ClusterClient 同时实现两个遍历接口，不能误判。
func (s *RedisStore) isRingTopology() bool {
	if s == nil || s.client == nil {
		return false
	}
	if _, ring := s.client.(*redis.Ring); ring {
		return true
	}
	_, shardScanner := s.client.(redisShardScanner)
	_, masterScanner := s.client.(redisMasterScanner)
	return shardScanner && !masterScanner
}

// AllowsPrefixMutation 判断前缀在当前明确拓扑下是否具备原子 guard 条件。
func (s *RedisStore) AllowsPrefixMutation(prefix string) bool {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return false
	}
	if s.isRingTopology() {
		return false
	}
	if _, single := s.client.(*redis.Client); single {
		return true
	}
	if s.knownDistributedClient() {
		return hasRedisClusterHashTag(prefix)
	}
	return false
}

// ValidatePrefixMutation 在 Store 拓扑校验后拒绝 Cluster 普通跨槽前缀。
func (s *RedisStore) ValidatePrefixMutation(ctx context.Context, prefix string) error {
	if err := s.validate(); err != nil {
		return errors.Tag(err)
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return errors.Errorf("缓存前缀不能为空")
	}
	if _, single := s.client.(*redis.Client); single {
		cluster, err := s.detectClusterTopology(ctx)
		if err != nil {
			return errors.Tag(err)
		}
		if cluster {
			return errors.Wrapf(ErrRedisTopologyUnsupported, "Redis单节点客户端连接了Cluster节点，前缀[%s]必须改用ClusterClient", prefix)
		}
		return nil
	}
	if s.AllowsPrefixMutation(prefix) {
		return nil
	}
	if s.knownDistributedClient() {
		return errors.Wrapf(ErrDistributedPrefixHashTagRequired, "Redis Cluster前缀[%s]必须包含固定hash tag", prefix)
	}
	return errors.Wrapf(ErrRedisTopologyUnsupported, "无法确认Redis客户端拓扑，拒绝前缀[%s]执行强一致操作", prefix)
}

// detectClusterTopology 探测未暴露节点遍历能力的客户端是否实际连接到 Redis Cluster；未知响应按故障安全返回。
func (s *RedisStore) detectClusterTopology(ctx context.Context) (bool, error) {
	switch s.topologyState.Load() {
	case redisTopologySingle:
		return false, nil
	case redisTopologyCluster:
		return true, nil
	}
	info, err := s.client.ClusterInfo(ctx).Result()
	if err == nil {
		cluster := strings.Contains(strings.ToLower(info), "cluster_state:")
		if !cluster {
			return false, errors.Wrapf(ErrRedisTopologyUnsupported, "Redis CLUSTER INFO响应无法确认拓扑")
		}
		s.topologyState.Store(redisTopologyCluster)
		return true, nil
	}
	if isClusterDisabledError(err) {
		s.topologyState.Store(redisTopologySingle)
		return false, nil
	}
	return false, errors.Tag(err)
}

// DeletePatternGuarded 对每批 SCAN 结果原子校验 owner 后删除。
func (s *RedisStore) DeletePatternGuarded(ctx context.Context, pattern string, count int64, guards []LockGuard) (int64, error) {
	if err := s.validate(); err != nil {
		return 0, errors.Tag(err)
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return 0, nil
	}
	count = normalizeScanCount(count)
	var deleted atomic.Int64
	for {
		var candidates atomic.Int64
		err := s.forEachScanNode(ctx, func(nodeCtx context.Context, client redis.UniversalClient) error {
			var cursor uint64
			for {
				keys, next, err := client.Scan(nodeCtx, cursor, pattern, count).Result()
				if err != nil {
					return errors.Tag(err)
				}
				candidates.Add(int64(len(keys)))
				for start := 0; start < len(keys); start += s.unlinkChunkSize {
					end := min(start+s.unlinkChunkSize, len(keys))
					actual, err := s.runGuardedMutation(nodeCtx, client, guards, nil, keys[start:end])
					deleted.Add(actual)
					if err != nil {
						return errors.Tag(err)
					}
				}
				cursor = next
				if cursor == 0 {
					return nil
				}
			}
		})
		if err != nil {
			return deleted.Load(), errors.Tag(err)
		}
		if candidates.Load() == 0 {
			return deleted.Load(), nil
		}
	}
}
