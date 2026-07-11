package tablecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	// prefixIndexShardCount 固定分片数，控制 Cluster 槽位分布和单分片容量上界。
	prefixIndexShardCount = 64
	// prefixIndexManifestValue 是索引完成一次完整重建后的可信标记值。
	prefixIndexManifestValue = "1"
	// prefixIndexShardConcurrency 是索引遍历并发上限，避免 64 分片串行 RTT 或无界并发。
	prefixIndexShardConcurrency = 8
	// prefixIndexPipelineCommandLimit 限制单个索引 Pipeline 的 Lua 命令数，避免全量快照生成超大请求。
	prefixIndexPipelineCommandLimit = 64
	// prefixIndexSentinel 保持活跃分片 ZSet 存在，精确删除最后一个业务成员时不会丢失完整性证据。
	prefixIndexSentinel = "\x00tcm:pidx"
	// prefixIndexSentinelScore 使用 double 可精确表达的最大整数，避免自然过期清理删除哨兵。
	prefixIndexSentinelScore = float64(1<<53 - 1)
)

// ErrPrefixIndexUntrusted 表示前缀索引完整性证据缺失，调用方必须撤销 ready 并降级全节点 SCAN。
var ErrPrefixIndexUntrusted = errors.New("tablecache prefix index untrusted")

// prefixIndexActivateScript 原子读取旧位并登记本次活跃分片，减少索引写路径网络往返。
var prefixIndexActivateScript = redis.NewScript(prefixIndexActivateLua)

// prefixIndexCommitScript 仅在活跃位图仍可信时原子续期并提交 manifest。
var prefixIndexCommitScript = redis.NewScript(prefixIndexCommitLua)

// prefixIndexPrepareScript 在锁栅栏内初始化重建所需的活跃位图，并撤销不再有完整性依据的 manifest。
var prefixIndexPrepareScript = redis.NewScript(prefixIndexPrepareLua)

// redisMasterScanner 表示可遍历 Redis Cluster 全部 master 的客户端能力。
type redisMasterScanner interface {
	ForEachMaster(context.Context, func(context.Context, *redis.Client) error) error
}

// redisShardScanner 表示只有分片遍历能力的客户端，用于识别并拒绝 Ring 类拓扑。
type redisShardScanner interface {
	ForEachShard(context.Context, func(context.Context, *redis.Client) error) error
}

// RedisPrefixPattern 把 Redis key 前缀转换成只匹配该字面前缀的 glob pattern。
func RedisPrefixPattern(prefix string) string {
	return EscapeRedisGlobLiteral(prefix) + "*"
}

// EscapeRedisGlobLiteral 转义 Redis glob 元字符，避免业务前缀被解释成通配表达式。
func EscapeRedisGlobLiteral(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for _, char := range value {
		switch char {
		case '*', '?', '[', ']', '\\':
			builder.WriteByte('\\')
		}
		builder.WriteRune(char)
	}
	return builder.String()
}

// addPrefixIndexKeysGuarded 校验索引输入，并在 guards 非空时阻止失锁 owner 改写完整性证据。
func (s *RedisStore) addPrefixIndexKeysGuarded(ctx context.Context, indexKey string, ttl time.Duration, guards []LockGuard, keys ...string) error {
	indexKey = strings.TrimSpace(indexKey)
	cleanKeys := uniqueSortedKeys(cleanRedisKeys(keys))
	if indexKey == "" || len(cleanKeys) == 0 {
		return nil
	}
	if ttl < time.Millisecond {
		return errors.Errorf("前缀索引TTL不能小于1ms")
	}
	return s.addPrefixIndexKeysGuardedInternal(ctx, indexKey, ttl, cleanKeys, guards)
}

// DeletePrefixIndexKeys 分页删除索引内的真实 key，并返回实际删除数量。
func (s *RedisStore) DeletePrefixIndexKeys(ctx context.Context, indexKey string, count int64, guards []LockGuard) (int64, error) {
	if err := s.validate(); err != nil {
		return 0, errors.Tag(err)
	}
	indexKey = strings.TrimSpace(indexKey)
	if indexKey == "" {
		return 0, nil
	}
	count = normalizeScanCount(count)
	shards, err := s.trustedPrefixIndexShards(ctx, indexKey)
	if err != nil {
		return 0, errors.Tag(err)
	}
	deleted, err := s.deletePrefixIndexKeysGuarded(ctx, indexKey, count, shards, guards)
	if err != nil {
		return deleted, errors.Tag(err)
	}
	return deleted, nil
}

// ReplacePrefix 安全替换前缀快照；任一步失败时不会先删除旧快照。
func (s *RedisStore) ReplacePrefix(ctx context.Context, mutation PrefixReplaceMutation) (int64, error) {
	if err := s.validate(); err != nil {
		return 0, errors.Tag(err)
	}
	mutation.IndexKey = strings.TrimSpace(mutation.IndexKey)
	mutation.ReadyKey = strings.TrimSpace(mutation.ReadyKey)
	mutation.Prefix = strings.TrimSpace(mutation.Prefix)
	guards, err := cleanLockGuards(mutation.Guards)
	if err != nil {
		return 0, errors.Tag(err)
	}
	if len(guards) == 0 {
		return 0, errors.Errorf("安全前缀替换缺少锁guard")
	}
	if err := s.ValidatePrefixMutation(ctx, mutation.Prefix); err != nil {
		return 0, errors.Tag(err)
	}
	indexEnabled := mutation.IndexKey != ""
	if indexEnabled && mutation.IndexTTL < time.Millisecond {
		return 0, errors.Errorf("安全前缀替换的索引TTL不能小于1ms")
	}
	entries := mutation.Entries
	prepared, err := s.prepareWriteEntries(entries)
	if err != nil {
		return 0, errors.Tag(err)
	}
	for _, entry := range prepared {
		if !entry.overwrite {
			return 0, errors.Wrapf(ErrInvalidConfig, "安全前缀替换不能增量写入key[%s]", entry.key)
		}
	}
	keepKeys := prefixReplaceKeepKeys(entries, mutation.KeepKeys)
	keepSet := make(map[string]struct{}, len(keepKeys))
	for _, key := range keepKeys {
		keepSet[key] = struct{}{}
	}
	var ready bool
	if indexEnabled {
		ready, err = s.prefixIndexReady(ctx, mutation.ReadyKey, mutation.IndexKey)
		if err != nil {
			return 0, errors.Tag(err)
		}
		if ready {
			if _, trustErr := s.trustedPrefixIndexShards(ctx, mutation.IndexKey); trustErr != nil {
				if !errors.Is(trustErr, ErrPrefixIndexUntrusted) {
					return 0, errors.Tag(trustErr)
				}
				ready = false
			}
		}
	}
	patterns := uniqueSortedKeys(cleanRedisKeys(mutation.DeletePatterns))
	if len(patterns) == 0 {
		return 0, errors.Errorf("安全前缀替换需要有效DeletePatterns完成旧快照对账")
	}
	mutation.DeletePatterns = patterns
	if mutation.ReadyKey != "" {
		// 先撤销可信标记；后续任一步失败都会强制下一次删除走全节点 SCAN，避免误信不完整索引。
		if _, err := s.runGuardedMutation(ctx, s.client, guards, nil, []string{mutation.ReadyKey}); err != nil {
			return 0, errors.Tag(err)
		}
	}
	if indexEnabled {
		// 新 key 必须先进入索引；后续写入失败只会留下多余索引，不会形成漏记。
		if err := s.addPrefixIndexKeysGuardedInternal(ctx, mutation.IndexKey, mutation.IndexTTL, keepKeys, guards); err != nil {
			return 0, errors.Tag(err)
		}
		if ready {
			manifestExists, err := s.Exists(ctx, mutation.IndexKey)
			if err != nil {
				return 0, errors.Tag(err)
			}
			if !manifestExists {
				ready = false
			}
		}
	}
	if err := s.writePreparedEntriesGuarded(ctx, guards, prepared); err != nil {
		return 0, errors.Tag(err)
	}
	count := normalizeScanCount(mutation.ScanCount)
	var deleted int64
	if !indexEnabled {
		deleted, err = s.deletePatternsExceptGuarded(ctx, mutation.DeletePatterns, count, keepSet, guards)
	} else if ready {
		deleted, err = s.deletePrefixIndexStaleGuarded(ctx, mutation.IndexKey, count, keepSet, guards)
		if errors.Is(err, ErrPrefixIndexUntrusted) {
			var scanDeleted int64
			scanDeleted, err = s.deletePatternsExceptGuarded(ctx, mutation.DeletePatterns, count, keepSet, guards)
			deleted += scanDeleted
		}
	} else {
		deleted, err = s.deletePatternsExceptGuarded(ctx, mutation.DeletePatterns, count, keepSet, guards)
	}
	if err != nil {
		return deleted, errors.Tag(err)
	}
	if indexEnabled {
		if err := s.rebuildPrefixIndexAfterCleanup(ctx, mutation.IndexKey, mutation.IndexTTL, keepKeys, guards); err != nil {
			return deleted, errors.Tag(err)
		}
	}
	return deleted, nil
}

// prefixIndexReady 判断 ready 与 manifest 是否同时存在。
func (s *RedisStore) prefixIndexReady(ctx context.Context, readyKey string, indexKey string) (bool, error) {
	if readyKey == "" {
		return false, nil
	}
	values, err := s.ExistsMulti(ctx, readyKey, indexKey)
	if err != nil {
		return false, errors.Tag(err)
	}
	return values[readyKey] && values[indexKey], nil
}

// prefixReplaceKeepKeys 合并显式保留 key 与本轮写入 key。
func prefixReplaceKeepKeys(entries []Entry, keys []string) []string {
	result := make([]string, 0, len(entries)+len(keys))
	result = append(result, keys...)
	for _, entry := range entries {
		result = append(result, entry.Key)
	}
	return uniqueSortedKeys(cleanRedisKeys(result))
}

// addPrefixIndexKeysGuardedInternal 按成员哈希写入索引；guards 非空时禁止失锁 owner 修复索引可信状态。
func (s *RedisStore) addPrefixIndexKeysGuardedInternal(ctx context.Context, indexKey string, ttl time.Duration, keys []string, guards []LockGuard) error {
	if len(keys) == 0 {
		return nil
	}
	groups := make(map[int][]string)
	for _, key := range keys {
		shard := prefixIndexMemberShard(key)
		groups[shard] = append(groups[shard], key)
	}
	shards := make([]int, 0, len(groups))
	for shard := range groups {
		shards = append(shards, shard)
	}
	// 活跃位必须先于成员写入；脚本同时返回旧位，供分片Lua判断哨兵完整性。
	previous, err := s.activatePrefixIndexShards(ctx, indexKey, ttl, shards, guards)
	if err != nil {
		return errors.Tag(err)
	}
	if err := s.writePrefixIndexGroups(ctx, indexKey, ttl, groups, previous, guards, false); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "missing sentinel") {
			return errors.Tag(err)
		}
		// 已登记分片缺失说明索引被淘汰；先撤销manifest，再重建当前成员，强制Manager后续SCAN对账。
		if len(guards) > 0 {
			if _, unlinkErr := s.runGuardedMutation(ctx, s.client, guards, nil, []string{indexKey}); unlinkErr != nil {
				return errors.Tag(unlinkErr)
			}
		} else if unlinkErr := s.client.Unlink(ctx, indexKey).Err(); unlinkErr != nil {
			return errors.Tag(unlinkErr)
		}
		if err := s.writePrefixIndexGroups(ctx, indexKey, ttl, groups, nil, guards, false); err != nil {
			return errors.Tag(err)
		}
	}
	return nil
}

// activatePrefixIndexShards 原子登记活跃分片并返回各分片旧状态。
func (s *RedisStore) activatePrefixIndexShards(ctx context.Context, indexKey string, ttl time.Duration, shards []int, guards []LockGuard) (map[int]bool, error) {
	guards, err := cleanLockGuards(guards)
	if err != nil {
		return nil, errors.Tag(err)
	}
	activeKey := prefixIndexActiveKey(indexKey)
	keys := make([]string, 0, len(guards)+2)
	args := make([]any, 0, len(guards)+len(shards)+2)
	args = append(args, len(guards))
	for _, guard := range guards {
		keys = append(keys, guard.Key)
		args = append(args, guard.Owner)
	}
	keys = append(keys, activeKey, indexKey)
	if err := rejectGuardKeyOverlap(guards, activeKey, indexKey); err != nil {
		return nil, errors.Tag(err)
	}
	args = append(args, ttlMilliseconds(scaleDuration(ttl, 3)))
	for _, shard := range shards {
		args = append(args, shard)
	}
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot(keys); err != nil {
			return nil, errors.Tag(err)
		}
	}
	values, err := prefixIndexActivateScript.Run(ctx, s.client, keys, args...).Slice()
	if err != nil {
		return nil, errors.Tag(normalizeLockGuardError(err))
	}
	if len(values) != len(shards) {
		return nil, errors.Errorf("前缀索引活跃分片脚本返回值无效")
	}
	result := make(map[int]bool, len(shards))
	for index, shard := range shards {
		result[shard] = fmt.Sprint(values[index]) == "1"
	}
	return result, nil
}

// writePrefixIndexGroups 分批写入已按分片聚合的索引成员。
func (s *RedisStore) writePrefixIndexGroups(ctx context.Context, indexKey string, ttl time.Duration, groups map[int][]string, require map[int]bool, guards []LockGuard, prune bool) error {
	guards, err := cleanLockGuards(guards)
	if err != nil {
		return errors.Tag(err)
	}
	shards := make([]int, 0, len(groups))
	for shard := range groups {
		shards = append(shards, shard)
	}
	sort.Ints(shards)

	pipe := s.client.Pipeline()
	cmds := make([]pipelineCmd, 0, prefixIndexPipelineCommandLimit)
	flush := func() error {
		if len(cmds) == 0 {
			return nil
		}
		if err := execAndCheckPipeline(ctx, pipe, cmds); err != nil {
			return errors.Tag(&PipelineExecError{Operation: "prefix_index_add", FailedKeys: failedPipelineKeys(cmds), Cause: err})
		}
		pipe = s.client.Pipeline()
		cmds = make([]pipelineCmd, 0, prefixIndexPipelineCommandLimit)
		return nil
	}

	for _, shard := range shards {
		members := groups[shard]
		batchCount := max(1, (len(members)+defaultWriteBatchChunkSize-1)/defaultWriteBatchChunkSize)
		for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
			start := min(batchIndex*defaultWriteBatchChunkSize, len(members))
			end := min(start+defaultWriteBatchChunkSize, len(members))
			batch := members[start:end]
			keys := make([]string, 0, len(guards)+1)
			args := make([]any, 0, len(guards)+len(batch)+7)
			args = append(args, len(guards))
			for _, guard := range guards {
				keys = append(keys, guard.Key)
				args = append(args, guard.Owner)
			}
			args = append(args,
				ttlMilliseconds(scaleDuration(ttl, 3)),
				ttlMilliseconds(scaleDuration(ttl, 2)),
				boolInt(prefixIndexBatchRequiresSentinel(require, shard, batchIndex)),
				boolInt(prune),
				prefixIndexSentinel,
				prefixIndexSentinelScore,
			)
			for _, member := range batch {
				args = append(args, member)
			}
			shardKey := prefixIndexShardKey(indexKey, shard)
			keys = append(keys, shardKey)
			if err := rejectGuardKeyOverlap(guards, shardKey); err != nil {
				return errors.Tag(err)
			}
			if s.knownDistributedClient() {
				if err := validateSameRedisSlot(keys); err != nil {
					return errors.Tag(err)
				}
			}
			cmds = append(cmds, pipelineCmd{key: shardKey, cmd: pipe.Eval(ctx, prefixIndexAddScript, keys, args...)})
			if len(cmds) == prefixIndexPipelineCommandLimit {
				if err := flush(); err != nil {
					return errors.Tag(err)
				}
			}
		}
	}
	return errors.Tag(flush())
}

// prefixIndexBatchRequiresSentinel 只允许每个分片首批按旧位图决定是否重建，后续批次必须证明前批仍存在。
func prefixIndexBatchRequiresSentinel(require map[int]bool, shard int, batchIndex int) bool {
	return batchIndex > 0 || (require != nil && require[shard])
}

// restorePrefixIndexSentinels 为位图声明的分片补哨兵、续期并剪枝过期成员，不缩减活跃位图。
func (s *RedisStore) restorePrefixIndexSentinels(ctx context.Context, indexKey string, ttl time.Duration, guards []LockGuard) error {
	shards, err := s.prefixIndexActiveShards(ctx, indexKey)
	if err != nil {
		return errors.Tag(err)
	}
	groups := make(map[int][]string, len(shards))
	for _, shard := range shards {
		groups[shard] = nil
	}
	return errors.Tag(s.writePrefixIndexGroups(ctx, indexKey, ttl, groups, nil, guards, true))
}

// rebuildPrefixIndexAfterCleanup 在业务快照完成对账后重建索引，并在锁栅栏和可信校验通过后提交 manifest。
func (s *RedisStore) rebuildPrefixIndexAfterCleanup(ctx context.Context, indexKey string, ttl time.Duration, keepKeys []string, guards []LockGuard) error {
	if err := s.preparePrefixIndexRebuild(ctx, indexKey, ttl, guards); err != nil {
		return errors.Tag(err)
	}
	// 先修复并剪枝历史分片，再加入权威快照成员；成员写入后的分片淘汰会被最终可信校验识别。
	if err := s.restorePrefixIndexSentinels(ctx, indexKey, ttl, guards); err != nil {
		return errors.Tag(err)
	}
	if err := s.addPrefixIndexKeysGuardedInternal(ctx, indexKey, ttl, keepKeys, guards); err != nil {
		return errors.Tag(err)
	}
	if err := s.commitPrefixIndex(ctx, indexKey, ttl, guards); err != nil {
		return errors.Tag(err)
	}
	_, err := s.trustedPrefixIndexShards(ctx, indexKey)
	return errors.Tag(err)
}

// preparePrefixIndexRebuild 在 guard 有效时初始化活跃位图；位图缺失或损坏时同步撤销旧 manifest。
func (s *RedisStore) preparePrefixIndexRebuild(ctx context.Context, indexKey string, ttl time.Duration, guards []LockGuard) error {
	guards, err := cleanLockGuards(guards)
	if err != nil {
		return errors.Tag(err)
	}
	if len(guards) == 0 {
		return errors.Errorf("前缀索引重建缺少锁guard")
	}
	keys := make([]string, 0, len(guards)+2)
	args := make([]any, 0, len(guards)+3)
	args = append(args, len(guards))
	for _, guard := range guards {
		keys = append(keys, guard.Key)
		args = append(args, guard.Owner)
	}
	keys = append(keys, prefixIndexActiveKey(indexKey), indexKey)
	if err := rejectGuardKeyOverlap(guards, prefixIndexActiveKey(indexKey), indexKey); err != nil {
		return errors.Tag(err)
	}
	args = append(args, ttlMilliseconds(scaleDuration(ttl, 3)), encodePrefixIndexBitmap(0))
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot(keys); err != nil {
			return errors.Tag(err)
		}
	}
	if _, err := prefixIndexPrepareScript.Run(ctx, s.client, keys, args...).Int64(); err != nil {
		return errors.Tag(normalizeLockGuardError(err))
	}
	return nil
}

// prefixIndexActiveKey 返回活跃分片位图 key。
func prefixIndexActiveKey(indexKey string) string {
	if hasRedisClusterHashTag(indexKey) {
		return prefixIndexChildKey(indexKey, "active")
	}
	return tablecacheMetaKey("pidx:active", indexKey)
}

// encodePrefixIndexBitmap 把活跃分片位图编码为固定八字节字符串。
func encodePrefixIndexBitmap(bitmap uint64) string {
	value := make([]byte, prefixIndexShardCount/8)
	for shard := 0; shard < prefixIndexShardCount; shard++ {
		if bitmap&(uint64(1)<<shard) != 0 {
			value[shard/8] |= byte(1 << (7 - shard%8))
		}
	}
	return string(value)
}

// decodePrefixIndexBitmap 解码并校验活跃分片位图。
func decodePrefixIndexBitmap(value string) (uint64, error) {
	if len(value) == 0 || len(value) > prefixIndexShardCount/8 {
		return 0, errors.Wrapf(ErrPrefixIndexUntrusted, "前缀索引活跃分片位图长度[%d]异常", len(value))
	}
	value += strings.Repeat("\x00", prefixIndexShardCount/8-len(value))
	var bitmap uint64
	for shard := 0; shard < prefixIndexShardCount; shard++ {
		if value[shard/8]&byte(1<<(7-shard%8)) != 0 {
			bitmap |= uint64(1) << shard
		}
	}
	return bitmap, nil
}

// prefixIndexActiveShards 读取位图中实际使用的分片编号。
func (s *RedisStore) prefixIndexActiveShards(ctx context.Context, indexKey string) ([]int, error) {
	value, err := s.client.Get(ctx, prefixIndexActiveKey(indexKey)).Result()
	if err == redis.Nil {
		return nil, errors.Wrapf(ErrPrefixIndexUntrusted, "前缀索引活跃分片位图不存在")
	}
	if err != nil {
		return nil, errors.Tag(err)
	}
	bitmap, err := decodePrefixIndexBitmap(value)
	if err != nil {
		return nil, errors.Tag(err)
	}
	shards := make([]int, 0, prefixIndexShardCount)
	for shard := 0; shard < prefixIndexShardCount; shard++ {
		if bitmap&(uint64(1)<<shard) != 0 {
			shards = append(shards, shard)
		}
	}
	return shards, nil
}

// trustedPrefixIndexShards 校验 manifest、位图和全部活跃分片哨兵。
func (s *RedisStore) trustedPrefixIndexShards(ctx context.Context, indexKey string) ([]int, error) {
	version, err := s.client.Get(ctx, indexKey).Result()
	if err != nil || version != prefixIndexManifestValue {
		if err != nil && err != redis.Nil {
			return nil, errors.Tag(err)
		}
		return nil, errors.Wrapf(ErrPrefixIndexUntrusted, "前缀索引manifest版本不可信")
	}
	shards, err := s.prefixIndexActiveShards(ctx, indexKey)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(shards) == 0 {
		return shards, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.FloatCmd, 0, len(shards))
	for _, shard := range shards {
		cmds = append(cmds, pipe.ZScore(ctx, prefixIndexShardKey(indexKey, shard), prefixIndexSentinel))
	}
	_, execErr := pipe.Exec(ctx)
	for index, cmd := range cmds {
		if cmd.Err() == redis.Nil {
			return nil, errors.Wrapf(ErrPrefixIndexUntrusted, "前缀索引活跃分片[%d]缺失", shards[index])
		}
		if cmd.Err() != nil {
			return nil, errors.Tag(cmd.Err())
		}
	}
	if execErr != nil {
		return nil, errors.Tag(execErr)
	}
	return shards, nil
}

// filterPrefixIndexSentinel 从业务成员结果中移除内部哨兵。
func filterPrefixIndexSentinel(keys []string) []string {
	result := keys[:0]
	for _, key := range keys {
		if key != prefixIndexSentinel {
			result = append(result, key)
		}
	}
	return result
}

// deletePrefixIndexKeysGuarded 按可信索引枚举成员，并在每批真实删除时原子校验锁 owner。
// 索引成员保持安全超集，由写入时间和TTL自然收敛，避免旧 owner 延迟 ZREM 新成员。
func (s *RedisStore) deletePrefixIndexKeysGuarded(ctx context.Context, indexKey string, count int64, shards []int, guards []LockGuard) (int64, error) {
	var deleted atomic.Int64
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(prefixIndexShardConcurrency)
	for _, shard := range shards {
		shardKey := prefixIndexShardKey(indexKey, shard)
		group.Go(func() error {
			actual, err := s.deletePrefixIndexShardGuarded(groupCtx, shardKey, count, nil, guards)
			deleted.Add(actual)
			return errors.Tag(err)
		})
	}
	if err := group.Wait(); err != nil {
		return deleted.Load(), errors.Tag(err)
	}
	return deleted.Load(), nil
}

// deletePrefixIndexStaleGuarded 删除不在当前快照中的索引成员对应数据，但不破坏性移除索引成员。
func (s *RedisStore) deletePrefixIndexStaleGuarded(ctx context.Context, indexKey string, count int64, keep map[string]struct{}, guards []LockGuard) (int64, error) {
	shards, err := s.trustedPrefixIndexShards(ctx, indexKey)
	if err != nil {
		return 0, errors.Tag(err)
	}
	var deleted atomic.Int64
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(prefixIndexShardConcurrency)
	for _, shard := range shards {
		shardKey := prefixIndexShardKey(indexKey, shard)
		group.Go(func() error {
			actual, err := s.deletePrefixIndexShardGuarded(groupCtx, shardKey, count, keep, guards)
			deleted.Add(actual)
			return errors.Tag(err)
		})
	}
	if err := group.Wait(); err != nil {
		return deleted.Load(), errors.Tag(err)
	}
	return deleted.Load(), nil
}

// deletePrefixIndexShardGuarded 单次遍历索引分片并按 guard 删除真实 key。
func (s *RedisStore) deletePrefixIndexShardGuarded(ctx context.Context, shardKey string, count int64, keep map[string]struct{}, guards []LockGuard) (int64, error) {
	if err := s.requirePrefixIndexSentinel(ctx, shardKey); err != nil {
		return 0, errors.Tag(err)
	}
	var cursor uint64
	var deleted int64
	for {
		values, next, err := s.client.ZScan(ctx, shardKey, cursor, "*", count).Result()
		if err != nil {
			return deleted, errors.Tag(err)
		}
		keys := make([]string, 0, len(values)/2)
		for index := 0; index+1 < len(values); index += 2 {
			keys = append(keys, values[index])
		}
		keys = filterPrefixIndexSentinel(keys)
		if keep != nil {
			keys = filterStaleKeys(keys, keep)
		}
		for start := 0; start < len(keys); start += s.unlinkChunkSize {
			end := min(start+s.unlinkChunkSize, len(keys))
			actual, err := s.runGuardedMutation(ctx, s.client, guards, nil, keys[start:end])
			deleted += actual
			if err != nil {
				return deleted, errors.Tag(err)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if err := s.requirePrefixIndexSentinel(ctx, shardKey); err != nil {
		return deleted, errors.Tag(err)
	}
	return deleted, nil
}

// requirePrefixIndexSentinel 验证分片在当前读取时刻仍具备完整性证据。
func (s *RedisStore) requirePrefixIndexSentinel(ctx context.Context, shardKey string) error {
	if err := s.client.ZScore(ctx, shardKey, prefixIndexSentinel).Err(); err != nil {
		if err == redis.Nil {
			return errors.Wrapf(ErrPrefixIndexUntrusted, "前缀索引分片[%s]缺少哨兵", shardKey)
		}
		return errors.Tag(err)
	}
	return nil
}

// deletePatternsExceptGuarded 重复全量扫描，直到一整轮没有候选，并在每批删除内校验 owner。
func (s *RedisStore) deletePatternsExceptGuarded(ctx context.Context, patterns []string, count int64, keep map[string]struct{}, guards []LockGuard) (int64, error) {
	var deleted int64
	for _, pattern := range uniqueSortedKeys(cleanRedisKeys(patterns)) {
		for {
			var passDeleted atomic.Int64
			var candidates atomic.Int64
			err := s.forEachScanNode(ctx, func(nodeCtx context.Context, client redis.UniversalClient) error {
				var cursor uint64
				for {
					keys, next, err := client.Scan(nodeCtx, cursor, pattern, count).Result()
					if err != nil {
						return errors.Tag(err)
					}
					keys = filterStaleKeys(keys, keep)
					candidates.Add(int64(len(keys)))
					for start := 0; start < len(keys); start += s.unlinkChunkSize {
						end := min(start+s.unlinkChunkSize, len(keys))
						actual, err := s.runGuardedMutation(nodeCtx, client, guards, nil, keys[start:end])
						passDeleted.Add(actual)
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
			deleted += passDeleted.Load()
			if err != nil {
				return deleted, errors.Tag(err)
			}
			if candidates.Load() == 0 {
				break
			}
		}
	}
	return deleted, nil
}

// forEachScanNode 按客户端拓扑遍历所有需要执行 SCAN 的主节点或分片。
func (s *RedisStore) forEachScanNode(ctx context.Context, visit func(context.Context, redis.UniversalClient) error) error {
	if scanner, ok := s.client.(redisMasterScanner); ok {
		return errors.Tag(scanner.ForEachMaster(ctx, func(nodeCtx context.Context, client *redis.Client) error {
			return visit(nodeCtx, client)
		}))
	}
	cluster, err := s.detectClusterTopology(ctx)
	if err != nil {
		return errors.Tag(err)
	}
	if cluster {
		return errors.Wrapf(ErrRedisTopologyUnsupported, "Redis Cluster客户端未暴露ForEachMaster拓扑能力")
	}
	return visit(ctx, s.client)
}

// isClusterDisabledError 判断当前客户端明确运行在非 Cluster 模式。
func isClusterDisabledError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "cluster support disabled")
}

// filterStaleKeys 原地过滤哨兵和当前快照仍需保留的 key。
func filterStaleKeys(keys []string, keep map[string]struct{}) []string {
	result := keys[:0]
	for _, key := range keys {
		if key == prefixIndexSentinel {
			continue
		}
		if _, ok := keep[key]; !ok {
			result = append(result, key)
		}
	}
	return result
}

// commitPrefixIndex 仅在 guard 有效且活跃位图可信时原子续期并写入 manifest。
func (s *RedisStore) commitPrefixIndex(ctx context.Context, indexKey string, ttl time.Duration, guards []LockGuard) error {
	guards, err := cleanLockGuards(guards)
	if err != nil {
		return errors.Tag(err)
	}
	if len(guards) == 0 {
		return errors.Errorf("前缀索引提交缺少锁guard")
	}
	activeKey := prefixIndexActiveKey(indexKey)
	keys := make([]string, 0, len(guards)+2)
	args := make([]any, 0, len(guards)+4)
	args = append(args, len(guards))
	for _, guard := range guards {
		keys = append(keys, guard.Key)
		args = append(args, guard.Owner)
	}
	keys = append(keys, activeKey, indexKey)
	if err := rejectGuardKeyOverlap(guards, activeKey, indexKey); err != nil {
		return errors.Tag(err)
	}
	args = append(args,
		ttlMilliseconds(scaleDuration(ttl, 3)),
		ttlMilliseconds(scaleDuration(ttl, 2)),
		prefixIndexManifestValue,
	)
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot(keys); err != nil {
			return errors.Tag(err)
		}
	}
	committed, err := prefixIndexCommitScript.Run(ctx, s.client, keys, args...).Int()
	if err != nil {
		return errors.Tag(normalizeLockGuardError(err))
	}
	if committed != 1 {
		return errors.Wrapf(ErrPrefixIndexUntrusted, "前缀索引活跃分片位图不存在或类型无效")
	}
	return nil
}

// prefixIndexMemberShard 使用稳定哈希把成员映射到固定分片。
func prefixIndexMemberShard(key string) int {
	const offset32 = uint32(2166136261)
	const prime32 = uint32(16777619)
	hash := offset32
	for index := 0; index < len(key); index++ {
		hash ^= uint32(key[index])
		hash *= prime32
	}
	return int(hash & (prefixIndexShardCount - 1))
}

// prefixIndexShardKey 生成前缀索引分片 key。
func prefixIndexShardKey(indexKey string, shard int) string {
	if hasRedisClusterHashTag(indexKey) {
		return prefixIndexChildKey(indexKey, fmt.Sprintf("s:%02x", shard))
	}
	sum := sha256.Sum256([]byte(indexKey))
	digest := hex.EncodeToString(sum[:8])
	return fmt.Sprintf("tcm:pidx:{%s:%02x}:%s", digest, shard, digest)
}

// prefixIndexChildKey 为同槽索引 key 生成短子 key。
func prefixIndexChildKey(indexKey string, suffix string) string {
	if strings.HasSuffix(indexKey, ":") {
		return indexKey + suffix
	}
	return indexKey + ":" + suffix
}

// normalizeScanCount 收敛无效或过大的 SCAN count，避免单轮 Redis 工作量和响应内存失控。
func normalizeScanCount(count int64) int64 {
	if count <= 0 {
		return defaultScanCount
	}
	return min(count, maxScanCount)
}

// boolInt 把布尔值转换为 Lua 参数使用的整数。
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// scaleDuration 安全放大时长并在溢出时饱和到最大值。
func scaleDuration(value time.Duration, multiplier int64) time.Duration {
	if value <= 0 || multiplier <= 0 {
		return 0
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if value > maxDuration/time.Duration(multiplier) {
		return maxDuration
	}
	return value * time.Duration(multiplier)
}
