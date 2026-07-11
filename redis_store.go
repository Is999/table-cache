package tablecache

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	randv2 "math/rand/v2"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
)

//go:embed redis_scripts/release_lock.lua
var releaseLockLuaAsset string // releaseLockLuaAsset 是释放锁脚本资产

var releaseLockLua = stripLeadingLuaComments(releaseLockLuaAsset) // releaseLockLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/refresh_lock.lua
var refreshLockLuaAsset string // refreshLockLuaAsset 是续期锁脚本资产

var refreshLockLua = stripLeadingLuaComments(refreshLockLuaAsset) // refreshLockLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/replace_collection.lua
var replaceCollectionAsset string // replaceCollectionAsset 是覆盖集合脚本资产

var replaceCollectionScript = stripLeadingLuaComments(replaceCollectionAsset) // replaceCollectionScript 是剥离文件头后的可执行脚本

//go:embed redis_scripts/mutate_collection.lua
var mutateCollectionAsset string // mutateCollectionAsset 是增量集合写入与TTL脚本资产

var mutateCollectionScript = stripLeadingLuaComments(mutateCollectionAsset) // mutateCollectionScript 是剥离文件头后的可执行脚本

//go:embed redis_scripts/prefix_index_add.lua
var prefixIndexAddAsset string // prefixIndexAddAsset 是前缀索引分片登记与过期剪枝脚本资产

var prefixIndexAddScript = stripLeadingLuaComments(prefixIndexAddAsset) // prefixIndexAddScript 是剥离文件头后的可执行脚本

//go:embed redis_scripts/prefix_index_activate.lua
var prefixIndexActivateAsset string // prefixIndexActivateAsset 是前缀索引活跃分片登记脚本资产

var prefixIndexActivateLua = stripLeadingLuaComments(prefixIndexActivateAsset) // prefixIndexActivateLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/prefix_index_commit.lua
var prefixIndexCommitAsset string // prefixIndexCommitAsset 是前缀索引manifest原子提交脚本资产

var prefixIndexCommitLua = stripLeadingLuaComments(prefixIndexCommitAsset) // prefixIndexCommitLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/prefix_index_prepare.lua
var prefixIndexPrepareAsset string // prefixIndexPrepareAsset 是前缀索引重建准备脚本资产

var prefixIndexPrepareLua = stripLeadingLuaComments(prefixIndexPrepareAsset) // prefixIndexPrepareLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/fields_empty_registry.lua
var fieldsEmptyRegistryAsset string // fieldsEmptyRegistryAsset 是字段组合空值registry脚本资产

var fieldsEmptyRegistryLua = stripLeadingLuaComments(fieldsEmptyRegistryAsset) // fieldsEmptyRegistryLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/replace_fields_with_empty.lua
var replaceFieldsWithEmptyAsset string // replaceFieldsWithEmptyAsset 是字段空值与旧Hash字段一致切换脚本资产

var replaceFieldsWithEmptyLua = stripLeadingLuaComments(replaceFieldsWithEmptyAsset) // replaceFieldsWithEmptyLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/delete_hash_fields.lua
var deleteHashFieldsAsset string // deleteHashFieldsAsset 是带锁删除Hash字段脚本资产

var deleteHashFieldsLua = stripLeadingLuaComments(deleteHashFieldsAsset) // deleteHashFieldsLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/acquire_refresh_lock.lua
var acquireRefreshLockAsset string // acquireRefreshLockAsset 是刷新锁获取与owner读取脚本资产

var acquireRefreshLockLua = stripLeadingLuaComments(acquireRefreshLockAsset) // acquireRefreshLockLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/replace_with_marker.lua
var replaceWithMarkerAsset string // replaceWithMarkerAsset 是元信息marker与旧缓存一致切换脚本资产

var replaceWithMarkerLua = stripLeadingLuaComments(replaceWithMarkerAsset) // replaceWithMarkerLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/guarded_mutation.lua
var guardedMutationAsset string // guardedMutationAsset 是锁owner校验与业务写删原子提交脚本资产

var guardedMutationLua = stripLeadingLuaComments(guardedMutationAsset) // guardedMutationLua 是剥离文件头后的可执行脚本

//go:embed redis_scripts/read_collection_limited.lua
var readCollectionLimitedAsset string // readCollectionLimitedAsset 是集合规模检查与全量读取原子脚本资产

var readCollectionLimitedLua = stripLeadingLuaComments(readCollectionLimitedAsset) // readCollectionLimitedLua 是剥离文件头后的可执行脚本

// releaseLockScript 仅在锁 owner 一致时释放重建锁。
var releaseLockScript = redis.NewScript(releaseLockLua)

// refreshLockScript 仅在锁 owner 一致时续期重建锁。
var refreshLockScript = redis.NewScript(refreshLockLua)

// replaceWithMarkerScript 先写元信息 marker，再删除同槽旧缓存。
var replaceWithMarkerScript = redis.NewScript(replaceWithMarkerLua)

// acquireRefreshLockScript 原子获取刷新锁，竞争失败时返回当前owner。
var acquireRefreshLockScript = redis.NewScript(acquireRefreshLockLua)

// readCollectionLimitedScript 原子检查集合规模并读取完整值。
var readCollectionLimitedScript = redis.NewScript(readCollectionLimitedLua)

// collectionReplaceSeq 为随机源异常时的集合临时 key 提供进程内去重序列。
var collectionReplaceSeq atomic.Uint64

const (
	// defaultUnlinkChunkSize 表示一次 Pipeline 中聚合的 UNLINK 命令数量，避免超大 pipeline 带来内存尖刺。
	defaultUnlinkChunkSize = 512
	// defaultWriteBatchChunkSize 表示一次写入 Pipeline 中最多聚合的 Entry 数量，避免全量刷新生成超大请求。
	defaultWriteBatchChunkSize = 256
	// defaultCollectionPageCount 表示分页读取默认及无配置时的单页硬上限。
	defaultCollectionPageCount = int64(1000)
	// defaultMaxCollectionCount 表示集合全量读写默认成员上限，0 仍可由调用方显式关闭。
	defaultMaxCollectionCount = 5000
	// maxUnlinkChunkSize 表示单个 UNLINK Pipeline 允许的命令数硬上限。
	maxUnlinkChunkSize = 10_000
	// maxPipelineRetries 表示幂等 Pipeline 允许的瞬时失败重试硬上限。
	maxPipelineRetries = 10
	// maxDefaultJitterRatio 表示默认 TTL 抖动最多等于基础 TTL。
	maxDefaultJitterRatio = 1.0
)

const (
	// collectionReadStateHit 表示受限集合读取成功。
	collectionReadStateHit = "tablecache_collection_hit"
	// collectionReadStateMiss 表示集合 key 不存在。
	collectionReadStateMiss = "tablecache_collection_miss"
	// collectionReadStateTooLarge 表示集合成员数超过读取上限。
	collectionReadStateTooLarge = "tablecache_collection_too_large"
)

// RedisStoreOption 表示 RedisStore 可选配置。
type RedisStoreOption func(*RedisStore)

// RedisStore 是基于 go-redis 的 Store 适配器，可直接服务 go-zero、Gin、Kratos 等 Go 框架。
type RedisStore struct {
	client                  redis.UniversalClient // client 是 Redis 通用客户端，支持单机、哨兵主节点和Cluster master；Ring会在校验期拒绝
	encoder                 Encoder               // encoder 是复杂值序列化函数
	pipelineRetries         int                   // pipelineRetries 表示批量 UNLINK/覆盖写在瞬时失败时的最大重试次数
	unlinkChunkSize         int                   // unlinkChunkSize 表示每个 Pipeline 批次最多聚合的 UNLINK 命令数，用于控制网络往返与内存占用平衡
	maxCollectionReadCount  int64                 // maxCollectionReadCount 表示全量集合读取前允许的最大成员数，0 表示不限制
	maxCollectionWriteCount int                   // maxCollectionWriteCount 表示单 Entry 集合写入允许的最大成员数，0 表示不限制
	defaultJitterRatio      float64               // defaultJitterRatio 表示未显式配置 Jitter 时按 TTL 比例增加默认抖动
	topologyState           atomic.Int32          // topologyState 缓存已确认的单节点或Cluster状态，避免前缀热路径重复探测
	configErr               error                 // configErr 保存 option 构造期错误，由 Validate 在启动期统一返回
}

// PipelineExecError 表示 Pipeline 执行失败的增强错误信息。
// 在部分命令失败（例如 cluster 路由抖动、网络短暂错误）时可携带失败 key 列表，便于线上快速定位与重试。
type PipelineExecError struct {
	Operation  string   // Operation 表示本次 Pipeline 执行的操作名称（用于日志与排障归类）
	FailedKeys []string // FailedKeys 表示本次 Pipeline 中检测到失败的 key 列表（已排序去重）
	Cause      error    // Cause 是底层错误原因
}

// Error 返回稳定的 Pipeline 失败摘要，避免日志输出过长 key 列表。
func (e *PipelineExecError) Error() string {
	if e == nil {
		return ""
	}
	cause := ""
	if e.Cause != nil {
		cause = fmt.Sprintf(" cause=%v", e.Cause)
	}
	if len(e.FailedKeys) == 0 {
		return fmt.Sprintf("tablecache pipeline exec failed op=%s%s", e.Operation, cause)
	}
	return fmt.Sprintf("tablecache pipeline exec failed op=%s failed_keys=%d%s", e.Operation, len(e.FailedKeys), cause)
}

// Unwrap 返回底层错误，便于 errors.Is 和 errors.As 继续匹配。
func (e *PipelineExecError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// pipelineCmd 记录 Pipeline 命令与业务 key 的对应关系，便于失败时聚合 key。
type pipelineCmd struct {
	key string      // key 表示当前命令关联的业务 key，用于错误聚合时定位失败项
	cmd redis.Cmder // cmd 表示当前 pipeline 中的具体命令实例
}

// preparedEntry 保存一次完成编码、边界校验和 TTL 计算后的写入计划，分批提交时不再重复处理业务值。
type preparedEntry struct {
	key         string         // key 是目标 Redis key
	typ         CacheType      // typ 是缓存数据类型
	ttl         time.Duration  // ttl 是本次计算后的实际 TTL
	overwrite   bool           // overwrite 表示是否覆盖整个 key
	stringValue string         // stringValue 是已编码的 String 值
	mapValues   map[string]any // mapValues 是已编码的 Hash 字段
	values      []any          // values 是已编码的 List 或 Set 成员
	zValues     []redis.Z      // zValues 是已编码的 ZSet 成员
}

// uniqueSortedKeys 对失败 key 做排序去重，便于日志与监控稳定展示。
func uniqueSortedKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	write := 0
	for read := 0; read < len(keys); read++ {
		if read == 0 || keys[read] != keys[read-1] {
			keys[write] = keys[read]
			write++
		}
	}
	return keys[:write]
}

// cleanRedisKeys 清洗调用方传入的 Redis key 列表，去掉空白 key 以避免无效命令进入 Pipeline。
func cleanRedisKeys(keys []string) []string {
	cleanKeys := make([]string, 0, len(keys)) // cleanKeys 表示去空白后的有效 Redis key 列表
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key != "" {
			cleanKeys = append(cleanKeys, key)
		}
	}
	return cleanKeys
}

// NewRedisStore 创建 go-redis 存储适配器。
func NewRedisStore(client redis.UniversalClient, opts ...RedisStoreOption) *RedisStore {
	store := &RedisStore{
		client:                  client,
		encoder:                 defaultEncoder,
		pipelineRetries:         1,
		unlinkChunkSize:         defaultUnlinkChunkSize,
		defaultJitterRatio:      0.1,
		maxCollectionReadCount:  defaultMaxCollectionCount,
		maxCollectionWriteCount: defaultMaxCollectionCount,
	}
	for _, opt := range opts {
		if isNilInterface(opt) {
			store.setConfigError(errors.Errorf("RedisStore Option不能为空"))
			continue
		}
		opt(store)
	}
	return store
}

// validate 校验 RedisStore 是否具备可用客户端和合法配置。
func (s *RedisStore) validate() error {
	if s == nil || isNilInterface(s.client) {
		return errors.Errorf("Redis客户端未初始化")
	}
	if s.isRingTopology() {
		return errors.Wrapf(ErrRedisTopologyUnsupported, "Redis Ring故障重映射可能复活旧值，table-cache拒绝使用")
	}
	if cluster, ok := s.client.(*redis.ClusterClient); ok {
		options := cluster.Options()
		if options != nil && (options.ReadOnly || options.RouteByLatency || options.RouteRandomly) {
			return errors.Errorf("Redis Cluster必须关闭副本读路由，确保锁、marker与业务值读取主节点")
		}
	}
	if s.configErr != nil {
		return errors.Tag(s.configErr)
	}
	return nil
}

// ValidateTablecacheStore 执行 Manager 构造期专用的 RedisStore 校验。
func (s *RedisStore) ValidateTablecacheStore() error {
	return s.validate()
}

// setConfigError 保留第一个构造期配置错误，供启动校验统一返回。
func (s *RedisStore) setConfigError(err error) {
	if s != nil && s.configErr == nil && err != nil {
		s.configErr = err
	}
}

// WithUnlinkChunkSize 设置批量删除时单个 Pipeline 的 UNLINK 命令数量。
// 较大的 chunk 可减少网络往返，较小的 chunk 可降低单次请求体积和 Redis 侧瞬时压力。
func WithUnlinkChunkSize(size int) RedisStoreOption {
	return func(store *RedisStore) {
		if size > 0 && size <= maxUnlinkChunkSize {
			store.unlinkChunkSize = size
			return
		}
		store.setConfigError(errors.Errorf("UNLINK批次大小必须在1到%d之间", maxUnlinkChunkSize))
	}
}

// WithRedisEncoder 设置 Redis 复杂值序列化函数。
func WithRedisEncoder(encoder Encoder) RedisStoreOption {
	return func(store *RedisStore) {
		if encoder != nil {
			store.encoder = encoder
			return
		}
		store.setConfigError(errors.Errorf("Redis编码器不能为空"))
	}
}

// WithPipelineRetries 设置批量删除与覆盖写的瞬时失败重试次数。
func WithPipelineRetries(retries int) RedisStoreOption {
	return func(store *RedisStore) {
		if retries >= 0 && retries <= maxPipelineRetries {
			store.pipelineRetries = retries
			return
		}
		store.setConfigError(errors.Errorf("Pipeline重试次数必须在0到%d之间", maxPipelineRetries))
	}
}

// WithMaxCollectionReadCount 设置全量读取 Hash/List/Set/ZSet 前允许的最大成员数；默认5000，0表示显式不限制。
func WithMaxCollectionReadCount(limit int64) RedisStoreOption {
	return func(store *RedisStore) {
		if limit >= 0 {
			store.maxCollectionReadCount = limit
			return
		}
		store.setConfigError(errors.Errorf("集合读取上限不能为负数"))
	}
}

// WithMaxCollectionWriteCount 设置单个 Entry 写入集合成员数上限；默认5000，0表示显式不限制。
func WithMaxCollectionWriteCount(limit int) RedisStoreOption {
	return func(store *RedisStore) {
		if limit >= 0 {
			store.maxCollectionWriteCount = limit
			return
		}
		store.setConfigError(errors.Errorf("集合写入上限不能为负数"))
	}
}

// WithDefaultJitterRatio 设置未显式配置 Jitter 时的默认 TTL 抖动比例；0 表示关闭默认抖动。
func WithDefaultJitterRatio(ratio float64) RedisStoreOption {
	return func(store *RedisStore) {
		if ratio >= 0 && ratio <= maxDefaultJitterRatio && !math.IsNaN(ratio) && !math.IsInf(ratio, 0) {
			store.defaultJitterRatio = ratio
			return
		}
		store.setConfigError(errors.Errorf("默认TTL抖动比例必须是0到%.0f之间的有限数", maxDefaultJitterRatio))
	}
}

// ApplyMutation 按“索引先加、业务写删原子提交”的故障安全顺序提交变更。
func (s *RedisStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	if err := s.validate(); err != nil {
		return errors.Tag(err)
	}
	deleteKeys := cleanRedisKeys(mutation.DeleteKeys) // deleteKeys 表示本次需要 UNLINK 的真实 Redis key
	prepared, err := s.prepareWriteEntries(mutation.WriteEntries)
	if err != nil {
		return errors.Tag(err)
	}
	if err := validateMutation(deleteKeys, prepared); err != nil {
		return errors.Tag(err)
	}
	guards, err := cleanLockGuards(mutation.Guards)
	if err != nil {
		return errors.Tag(err)
	}
	if len(guards) > 0 && len(prepared) > 1 {
		return errors.Wrapf(ErrInvalidConfig, "带锁缓存变更最多写入一条Entry")
	}
	for _, indexMutation := range mutation.AddIndex {
		if err := s.addPrefixIndexKeysGuarded(ctx, indexMutation.IndexKey, indexMutation.TTL, guards, indexMutation.Keys...); err != nil {
			return errors.Tag(err)
		}
	}
	if len(guards) > 0 && len(prepared) == 0 && len(deleteKeys) == 0 {
		return nil
	}
	if len(guards) > 0 {
		var entry *preparedEntry
		if len(prepared) == 1 {
			entry = &prepared[0]
		}
		_, err := s.runGuardedMutation(ctx, s.client, guards, entry, deleteKeys)
		return errors.Tag(err)
	}
	if err := s.writePreparedEntries(ctx, prepared); err != nil {
		return errors.Tag(err)
	}
	if _, err := s.unlinkKeys(ctx, s.client, deleteKeys); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// validateMutation 拒绝会让故障安全顺序自相删除的数据重叠。
func validateMutation(deleteKeys []string, entries []preparedEntry) error {
	writeKeys := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		writeKeys[entry.key] = struct{}{}
	}
	for _, key := range deleteKeys {
		if _, exists := writeKeys[key]; exists {
			return errors.Wrapf(ErrInvalidConfig, "故障安全变更不能同时删除和写入key[%s]", key)
		}
	}
	return nil
}

// ReplaceWithMarker 先写入 String marker，成功后再删除同槽旧缓存。
func (s *RedisStore) ReplaceWithMarker(ctx context.Context, guards []LockGuard, marker Entry, deleteKeys ...string) error {
	if err := s.validate(); err != nil {
		return errors.Tag(err)
	}
	if marker.Type != TypeString {
		return errors.Errorf("缓存marker必须是String类型")
	}
	prepared, err := s.prepareWriteEntry(marker)
	if err != nil {
		return errors.Tag(err)
	}
	if prepared.ttl < 0 {
		return errors.Errorf("缓存marker TTL不能小于0")
	}
	deleteKeys = uniqueSortedKeys(cleanRedisKeys(deleteKeys))
	guards, err = cleanLockGuards(guards)
	if err != nil {
		return errors.Tag(err)
	}
	if len(guards) == 0 {
		return errors.Errorf("缓存marker提交缺少锁guard")
	}
	keys := make([]string, 0, len(guards)+len(deleteKeys)+1)
	args := make([]any, 0, len(guards)+3)
	args = append(args, len(guards))
	for _, guard := range guards {
		keys = append(keys, guard.Key)
		args = append(args, guard.Owner)
	}
	keys = append(keys, prepared.key)
	keys = append(keys, deleteKeys...)
	if err := rejectGuardKeyOverlap(guards, keys[len(guards):]...); err != nil {
		return errors.Tag(err)
	}
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot(keys); err != nil {
			return errors.Tag(err)
		}
	}
	for _, key := range deleteKeys {
		if key == prepared.key {
			return errors.Errorf("缓存marker不能同时被删除")
		}
	}
	args = append(args, prepared.stringValue, ttlMilliseconds(prepared.ttl))
	if _, err := replaceWithMarkerScript.Run(ctx, s.client, keys, args...).Int64(); err != nil {
		return errors.Tag(normalizeLockGuardError(err))
	}
	return nil
}

// validateSameRedisSlot 校验原子脚本涉及的全部 key 位于同一 Redis Cluster slot。
func validateSameRedisSlot(keys []string) error {
	var slotTag string
	for _, key := range cleanRedisKeys(keys) {
		tag := redisClusterHashTag(key)
		if slotTag == "" {
			slotTag = tag
			continue
		}
		if tag != slotTag {
			return errors.Errorf("原子缓存变更包含跨slot key")
		}
	}
	return nil
}

// unlinkKeys 使用 Pipeline 单 key UNLINK，返回真实删除数量并只重试未确认成功的命令。
func (s *RedisStore) unlinkKeys(ctx context.Context, client redis.UniversalClient, keys []string) (int64, error) {
	if client == nil {
		return 0, errors.Errorf("Redis客户端未初始化")
	}
	// chunkSize 控制单次 Pipeline 聚合的 UNLINK 命令数量，防止一次性提交过多 key 造成网络包和内存尖刺。
	chunkSize := s.unlinkChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultUnlinkChunkSize
	}
	var deleted int64
	for start := 0; start < len(keys); start += chunkSize {
		end := start + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		pending := append([]string(nil), keys[start:end]...) // pending 只保留尚未确认执行成功的 key，避免重试后重复计数
		var lastErr error
		retries := min(max(s.pipelineRetries, 0), maxPipelineRetries)
		for attempt := 0; len(pending) > 0; attempt++ {
			if attempt > 0 {
				if err := waitWithContext(ctx, pipelineRetryDelay(attempt)); err != nil {
					return deleted, errors.Tag(err)
				}
			}
			pipe := client.Pipeline()
			cmds := make([]*redis.IntCmd, 0, len(pending))
			for _, key := range pending {
				cmds = append(cmds, pipe.Unlink(ctx, key))
			}
			_, execErr := pipe.Exec(ctx)
			if execErr != nil {
				lastErr = execErr
			}
			next := make([]string, 0, len(pending))
			for index, cmd := range cmds {
				count, cmdErr := cmd.Result()
				if cmdErr != nil || (execErr != nil && count == 0) {
					lastErr = cmdErr
					if lastErr == nil {
						lastErr = execErr
					}
					next = append(next, pending[index])
					continue
				}
				deleted += count
			}
			pending = next
			if attempt >= retries {
				break
			}
		}
		if len(pending) > 0 {
			if lastErr == nil {
				lastErr = errors.Errorf("UNLINK命令执行状态未知")
			}
			return deleted, errors.Tag(&PipelineExecError{
				Operation:  "unlink_keys",
				FailedKeys: uniqueSortedKeys(pending),
				Cause:      lastErr,
			})
		}
	}
	return deleted, nil
}

// Exists 判断 Redis key 是否存在。
func (s *RedisStore) Exists(ctx context.Context, key string) (bool, error) {
	if err := s.validate(); err != nil {
		return false, errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return false, nil
	}
	count, err := s.client.Exists(ctx, key).Result()
	return count > 0, errors.Tag(err)
}

// ExistsMulti 批量判断 Redis key 是否存在，返回 map 只包含传入的有效 key。
func (s *RedisStore) ExistsMulti(ctx context.Context, keys ...string) (map[string]bool, error) {
	if err := s.validate(); err != nil {
		return nil, errors.Tag(err)
	}
	cleanKeys := uniqueSortedKeys(cleanRedisKeys(keys))
	if len(cleanKeys) == 0 {
		return map[string]bool{}, nil
	}
	result := make(map[string]bool, len(cleanKeys))
	// 每批固定上限，既合并高频等待轮询，也避免外部调用一次构造无界 Pipeline。
	for start := 0; start < len(cleanKeys); start += defaultWriteBatchChunkSize {
		end := min(start+defaultWriteBatchChunkSize, len(cleanKeys))
		pipe := s.client.Pipeline()
		cmds := make([]*redis.IntCmd, 0, end-start)
		for _, key := range cleanKeys[start:end] {
			cmds = append(cmds, pipe.Exists(ctx, key))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, errors.Tag(err)
		}
		for index, cmd := range cmds {
			count, err := cmd.Result()
			if err != nil {
				return nil, errors.Tag(err)
			}
			result[cleanKeys[start+index]] = count > 0
		}
	}
	return result, nil
}

// Read 按缓存类型读取 Redis 原始值。
func (s *RedisStore) Read(ctx context.Context, key string, typ CacheType) (any, error) {
	if err := s.validate(); err != nil {
		return nil, errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, errors.Tag(ErrCacheMiss)
	}
	switch typ {
	case TypeString:
		return s.readString(ctx, key)
	case TypeHash:
		return s.readHash(ctx, key)
	case TypeList:
		return s.readList(ctx, key)
	case TypeSet:
		return s.readSet(ctx, key)
	case TypeZSet:
		return s.readZSet(ctx, key)
	default:
		return nil, errors.Errorf("不支持的Redis缓存类型: %s", typ)
	}
}

// readRefreshSnapshot 按 generation→业务值的固定顺序合并读穿热路径；不安全或未受限的集合回退通用 Store 路径。
func (s *RedisStore) readRefreshSnapshot(ctx context.Context, key string, typ CacheType, fields []string, resultKey string) (refreshSnapshot, bool, error) {
	if err := s.validate(); err != nil {
		return refreshSnapshot{}, true, errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	resultKey = strings.TrimSpace(resultKey)
	fields = uniqueSortedKeys(cleanRedisKeys(fields))
	if key == "" || resultKey == "" {
		return refreshSnapshot{}, true, errors.Errorf("读穿快照业务key和结果key不能为空")
	}
	if len(fields) > 0 && typ != TypeHash {
		return refreshSnapshot{}, true, errors.Errorf("只有Hash读穿快照支持fields")
	}
	if len(fields) > 0 {
		if err := s.validateReadPageOptions(ReadPageOptions{Fields: fields}); err != nil {
			return refreshSnapshot{}, true, errors.Tag(err)
		}
	}
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot([]string{resultKey, key}); err != nil {
			return refreshSnapshot{}, true, errors.Tag(err)
		}
	}
	switch {
	case typ == TypeString:
		pipe := s.client.Pipeline()
		resultCmd := pipe.Get(ctx, resultKey)
		valueCmd := pipe.Get(ctx, key)
		_, execErr := pipe.Exec(ctx)
		result, resultErr := resultCmd.Result()
		if resultErr != nil && resultErr != redis.Nil {
			return refreshSnapshot{}, true, errors.Tag(resultErr)
		}
		value, valueErr := valueCmd.Result()
		if valueErr != nil && valueErr != redis.Nil {
			return refreshSnapshot{}, true, errors.Tag(valueErr)
		}
		if execErr != nil && execErr != redis.Nil {
			return refreshSnapshot{}, true, errors.Tag(execErr)
		}
		return refreshSnapshot{
			Value:       value,
			ValueReady:  valueErr == nil,
			Result:      result,
			ResultReady: resultErr == nil,
		}, true, nil
	case typ == TypeHash && len(fields) > 0:
		pipe := s.client.Pipeline()
		resultCmd := pipe.Get(ctx, resultKey)
		valueCmd := pipe.HMGet(ctx, key, fields...)
		_, execErr := pipe.Exec(ctx)
		result, resultErr := resultCmd.Result()
		if resultErr != nil && resultErr != redis.Nil {
			return refreshSnapshot{}, true, errors.Tag(resultErr)
		}
		values, valueErr := valueCmd.Result()
		if valueErr != nil {
			return refreshSnapshot{}, true, errors.Tag(valueErr)
		}
		if execErr != nil && execErr != redis.Nil {
			return refreshSnapshot{}, true, errors.Tag(execErr)
		}
		value := make(map[string]string, len(fields))
		for index, item := range values {
			if item != nil {
				value[fields[index]] = redisValueToString(item)
			}
		}
		return refreshSnapshot{
			Value:       value,
			ValueReady:  len(value) == len(fields),
			Result:      result,
			ResultReady: resultErr == nil,
		}, true, nil
	case isCollectionType(typ):
		if s.maxCollectionReadCount <= 0 {
			return refreshSnapshot{}, false, nil
		}
		values, valueReady, result, resultReady, err := s.readCollectionLimitedSnapshot(ctx, key, typ, resultKey)
		if err != nil {
			return refreshSnapshot{}, true, errors.Tag(err)
		}
		if !valueReady {
			return refreshSnapshot{Result: result, ResultReady: resultReady}, true, nil
		}
		value, err := decodeCollectionValues(key, typ, values)
		if err != nil {
			return refreshSnapshot{}, true, errors.Tag(err)
		}
		return refreshSnapshot{Value: value, ValueReady: true, Result: result, ResultReady: resultReady}, true, nil
	default:
		return refreshSnapshot{}, true, errors.Errorf("不支持的Redis缓存类型: %s", typ)
	}
}

// ReadPage 按缓存类型分页读取 Redis 原始值，避免大集合必须一次性全量读取。
func (s *RedisStore) ReadPage(ctx context.Context, key string, typ CacheType, options ReadPageOptions) (ReadPageResult, error) {
	if err := s.validate(); err != nil {
		return ReadPageResult{}, errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return ReadPageResult{}, errors.Tag(ErrCacheMiss)
	}
	if isCollectionType(typ) {
		options.Fields = uniqueSortedKeys(cleanRedisKeys(options.Fields))
		if err := s.validateReadPageOptions(options); err != nil {
			return ReadPageResult{}, errors.Tag(err)
		}
	}
	switch typ {
	case TypeString:
		value, err := s.readString(ctx, key)
		if err != nil {
			return ReadPageResult{}, errors.Tag(err)
		}
		return ReadPageResult{Value: value}, nil
	case TypeHash:
		return s.readHashPage(ctx, key, options)
	case TypeList:
		return s.readListPage(ctx, key, options)
	case TypeSet:
		return s.readSetPage(ctx, key, options)
	case TypeZSet:
		return s.readZSetPage(ctx, key, options)
	default:
		return ReadPageResult{}, errors.Errorf("不支持的Redis缓存类型: %s", typ)
	}
}

// AcquireRefreshLock 原子获取刷新锁，失败时在同一脚本内返回当前owner。
func (s *RedisStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	if err := s.validate(); err != nil {
		return false, "", errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" || ttl <= 0 {
		return false, "", errors.Errorf("Redis刷新锁key、owner和TTL必须有效")
	}
	result, err := acquireRefreshLockScript.Run(ctx, s.client, []string{key}, value, ttlMilliseconds(ttl)).Slice()
	if err != nil {
		return false, "", errors.Tag(err)
	}
	if len(result) != 2 {
		return false, "", errors.Errorf("Redis刷新锁脚本返回值无效")
	}
	locked := fmt.Sprint(result[0]) == "1"
	return locked, fmt.Sprint(result[1]), nil
}

// RefreshLock 仅当锁值与持有者标识一致时续期锁。
func (s *RedisStore) RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	if err := s.validate(); err != nil {
		return false, errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" || ttl <= 0 {
		return false, nil
	}
	result, err := refreshLockScript.Run(ctx, s.client, []string{key}, value, ttlMilliseconds(ttl)).Int64()
	if err != nil {
		return false, errors.Tag(err)
	}
	return result > 0, nil
}

// ReleaseLock 仅当锁值与持有者标识一致时释放锁，避免误删其它实例刚抢到的锁。
func (s *RedisStore) ReleaseLock(ctx context.Context, key string, value string) (bool, error) {
	if err := s.validate(); err != nil {
		return false, errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return false, nil
	}
	result, err := releaseLockScript.Run(ctx, s.client, []string{key}, value).Int64()
	if err != nil {
		return false, errors.Tag(err)
	}
	return result > 0, nil
}

// writePreparedEntries 分批提交已完成编码和校验的写入计划。
func (s *RedisStore) writePreparedEntries(ctx context.Context, entries []preparedEntry) error {
	if len(entries) == 0 {
		return nil
	}
	retryable := preparedEntriesRetryable(entries)
	// 分批提交避免单个 pipeline 过大导致的网络包过大、内存尖刺与 Redis 侧处理压力。
	const chunkSize = defaultWriteBatchChunkSize
	for start := 0; start < len(entries); start += chunkSize {
		end := start + chunkSize
		if end > len(entries) {
			end = len(entries)
		}
		var finalCmds []pipelineCmd
		// 即使 Exec 返回 nil，也要检查各命令的 Err，避免漏掉部分失败。
		retries := 0
		if retryable {
			retries = s.pipelineRetries
		}
		err := execPipelineWithRetry(ctx, retries, func() error {
			pipe := s.client.Pipeline()
			cmds := make([]pipelineCmd, 0, end-start)
			for _, entry := range entries[start:end] {
				s.enqueuePreparedWrite(ctx, pipe, entry, &cmds)
			}
			finalCmds = cmds
			return execAndCheckPipeline(ctx, pipe, cmds)
		})
		if err != nil {
			return errors.Tag(&PipelineExecError{
				Operation:  "write_batch",
				FailedKeys: failedPipelineKeys(finalCmds),
				Cause:      err,
			})
		}
	}
	return nil
}

// preparedEntriesRetryable 判断当前批量写是否允许自动重试。
func preparedEntriesRetryable(entries []preparedEntry) bool {
	for _, entry := range entries {
		if !entry.overwrite {
			return false
		}
	}
	return true
}

// prepareWriteEntries 在首条 Redis 命令前一次性完成所有值编码、边界校验和 TTL 计算。
func (s *RedisStore) prepareWriteEntries(entries []Entry) ([]preparedEntry, error) {
	prepared := make([]preparedEntry, 0, len(entries))
	for _, entry := range entries {
		item, err := s.prepareWriteEntry(entry)
		if err != nil {
			return nil, errors.Tag(err)
		}
		prepared = append(prepared, item)
	}
	return prepared, nil
}

// prepareWriteEntry 把单条 Entry 转换为可直接提交的写入计划。
func (s *RedisStore) prepareWriteEntry(entry Entry) (preparedEntry, error) {
	entry.Key = strings.TrimSpace(entry.Key)
	if entry.Key == "" {
		return preparedEntry{}, errors.Errorf("Redis缓存key不能为空")
	}
	if err := validateRedisClusterHashTagKey(entry.Key); err != nil {
		return preparedEntry{}, errors.Tag(err)
	}
	if err := validateEntryTTL(entry, s.defaultJitterRatio); err != nil {
		return preparedEntry{}, errors.Tag(err)
	}
	prepared := preparedEntry{
		key:       entry.Key,
		typ:       entry.Type,
		ttl:       jitterDurationWithDefault(entry.TTL, entry.Jitter, s.defaultJitterRatio),
		overwrite: entryShouldOverwrite(entry),
	}
	if (prepared.typ == TypeString || prepared.typ == TypeList) && !prepared.overwrite {
		return preparedEntry{}, errors.Wrapf(ErrInvalidConfig, "缓存key[%s]的类型[%s]不支持增量写入", entry.Key, entry.Type)
	}
	if count, ok := rawCollectionInputCount(entry.Type, entry.Value); ok {
		if err := s.ensureCollectionWriteCount(entry.Key, entry.Type, count); err != nil {
			return preparedEntry{}, errors.Tag(err)
		}
	}
	switch entry.Type {
	case TypeString:
		value, err := s.encodeString(entry.Value)
		prepared.stringValue = value
		return prepared, errors.Tag(err)
	case TypeHash:
		values, err := s.normalizeMap(entry.Value)
		if err != nil {
			return preparedEntry{}, errors.Tag(err)
		}
		prepared.mapValues = values
		return prepared, nil
	case TypeList, TypeSet:
		values, err := s.normalizeSlice(entry.Value)
		if err != nil {
			return preparedEntry{}, errors.Tag(err)
		}
		prepared.values = values
		return prepared, nil
	case TypeZSet:
		values, err := s.normalizeZSet(entry.Value)
		if err != nil {
			return preparedEntry{}, errors.Tag(err)
		}
		prepared.zValues = values
		return prepared, nil
	default:
		return preparedEntry{}, errors.Errorf("不支持的Redis缓存类型: %s", entry.Type)
	}
}

// rawCollectionInputCount 在编码和复制前 O(1) 读取合法集合容器规模。
func rawCollectionInputCount(typ CacheType, value any) (int, bool) {
	if value == nil {
		return 0, true
	}
	switch typ {
	case TypeHash:
		input := reflect.ValueOf(value)
		if input.Kind() == reflect.Map {
			return input.Len(), true
		}
	case TypeList, TypeSet:
		input := reflect.ValueOf(value)
		if input.Kind() == reflect.Slice || input.Kind() == reflect.Array {
			return input.Len(), true
		}
	case TypeZSet:
		switch input := value.(type) {
		case []ZMember:
			return len(input), true
		case map[string]float64:
			return len(input), true
		}
	}
	return 0, false
}

// enqueuePreparedWrite 把已准备的单条写入加入 Pipeline。
func (s *RedisStore) enqueuePreparedWrite(ctx context.Context, pipe redis.Pipeliner, entry preparedEntry, cmds *[]pipelineCmd) {
	if entry.typ == TypeString {
		cmd := pipe.Set(ctx, entry.key, entry.stringValue, entry.ttl)
		*cmds = append(*cmds, pipelineCmd{key: entry.key, cmd: cmd})
		return
	}
	values := entry.values
	if entry.typ == TypeHash {
		values = make([]any, 0, len(entry.mapValues)*2)
		for field, value := range entry.mapValues {
			values = append(values, field, value)
		}
	} else if entry.typ == TypeZSet {
		values = make([]any, 0, len(entry.zValues)*2)
		for _, value := range entry.zValues {
			values = append(values, value.Score, value.Member)
		}
	}
	if entry.overwrite {
		s.enqueueCollectionReplace(ctx, pipe, entry.key, entry.typ, entry.ttl, values, cmds)
		return
	}
	s.enqueueCollectionMutation(ctx, pipe, entry.key, entry.typ, entry.ttl, values, cmds)
}

// validateEntryTTL 校验基础 TTL、显式抖动和默认抖动不会产生非法时长。
func validateEntryTTL(entry Entry, defaultRatio float64) error {
	if entry.TTL < 0 || entry.Jitter < 0 {
		return errors.Errorf("缓存TTL和Jitter不能为负数")
	}
	if entry.TTL == 0 && entry.Jitter > 0 {
		return errors.Errorf("永久缓存不能配置Jitter")
	}
	if entry.TTL == 0 {
		return nil
	}
	const maxDuration = time.Duration(1<<63 - 1)
	jitter := entry.Jitter
	if jitter == 0 && defaultRatio > 0 {
		if math.IsNaN(defaultRatio) || math.IsInf(defaultRatio, 0) {
			return errors.Errorf("默认TTL抖动比例必须是有限数")
		}
		jitterValue := float64(entry.TTL) * defaultRatio
		if jitterValue > float64(maxDuration-entry.TTL) {
			return errors.Errorf("缓存TTL与默认Jitter相加溢出")
		}
		jitter = time.Duration(jitterValue)
	}
	if jitter > maxDuration-entry.TTL {
		return errors.Errorf("缓存TTL与Jitter相加溢出")
	}
	return nil
}

// execAndCheckPipeline 执行 Pipeline 并检查每条命令错误，避免 Exec 返回 nil 但单命令失败被遗漏。
func execAndCheckPipeline(ctx context.Context, pipe redis.Pipeliner, cmds []pipelineCmd) error {
	_, execErr := pipe.Exec(ctx)
	if execErr == nil {
		for _, cmd := range cmds {
			if cmd.cmd.Err() != nil {
				execErr = cmd.cmd.Err()
				break
			}
		}
	}
	if execErr != nil {
		return errors.Tag(execErr)
	}
	return nil
}

// failedPipelineKeys 提取 Pipeline 中失败命令关联的 key，并做排序去重，便于错误日志定位。
func failedPipelineKeys(cmds []pipelineCmd) []string {
	failed := make([]string, 0, 8) // failed 表示 Pipeline 中明确返回错误的 key 列表
	for _, cmd := range cmds {
		if cmd.cmd.Err() != nil {
			failed = append(failed, cmd.key)
		}
	}
	return uniqueSortedKeys(failed)
}

// execPipelineWithRetry 对可安全重试的 pipeline 做轻量重试，吸收瞬时网络/路由抖动。
func execPipelineWithRetry(ctx context.Context, retries int, exec func() error) error {
	retries = min(max(retries, 0), maxPipelineRetries)
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if err := waitWithContext(ctx, pipelineRetryDelay(attempt)); err != nil {
				return errors.Tag(err)
			}
		}
		if err := exec(); err != nil {
			lastErr = err
			if attempt >= retries {
				break
			}
			continue
		}
		return nil
	}
	return errors.Tag(lastErr)
}

// pipelineRetryDelay 返回 pipeline 第 N 次重试前的退避时长。
func pipelineRetryDelay(attempt int) time.Duration {
	delay := 10 * time.Millisecond
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= 80*time.Millisecond {
			return 80 * time.Millisecond
		}
	}
	return delay
}

// enqueueCollectionMutation 使用单 key Lua 原子提交增量集合写入与TTL语义。
func (s *RedisStore) enqueueCollectionMutation(ctx context.Context, pipe redis.Pipeliner, key string, typ CacheType, ttl time.Duration, values []any, cmds *[]pipelineCmd) {
	args := make([]any, 0, len(values)+2)
	args = append(args, string(typ), ttlMilliseconds(ttl))
	args = append(args, values...)
	cmd := pipe.Eval(ctx, mutateCollectionScript, []string{key}, args...)
	*cmds = append(*cmds, pipelineCmd{key: key, cmd: cmd})
}

// enqueueCollectionReplace 先在同槽临时 key 构建集合，再由 Lua 原子替换目标 key。
func (s *RedisStore) enqueueCollectionReplace(ctx context.Context, pipe redis.Pipeliner, key string, typ CacheType, ttl time.Duration, values []any, cmds *[]pipelineCmd) {
	args := make([]any, 0, len(values)+2)
	args = append(args, string(typ), ttlMilliseconds(ttl))
	args = append(args, values...)
	temporaryKey := collectionReplaceTempKey(key)
	cmd := pipe.Eval(ctx, replaceCollectionScript, []string{key, temporaryKey}, args...)
	*cmds = append(*cmds, pipelineCmd{key: key, cmd: cmd})
}

// collectionReplaceTempKey 生成与目标同槽且低碰撞的集合替换临时 key。
func collectionReplaceTempKey(key string) string {
	var token uint64
	if err := binary.Read(rand.Reader, binary.LittleEndian, &token); err != nil {
		token = uint64(time.Now().UnixNano()) ^ collectionReplaceSeq.Add(1)
	}
	return tablecacheMetaKey(fmt.Sprintf("replace:%016x", token), key)
}

// readString 读取 String 缓存。
func (s *RedisStore) readString(ctx context.Context, key string) (string, error) {
	value, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", errors.Tag(ErrCacheMiss)
	}
	return value, errors.Tag(err)
}

// readCollectionLimited 在一个 Lua 执行边界内检查成员数并读取集合，避免检查后并发增长绕过上限。
func (s *RedisStore) readCollectionLimited(ctx context.Context, key string, typ CacheType) ([]any, error) {
	values, ready, _, _, err := s.readCollectionLimitedSnapshot(ctx, key, typ, "")
	if err != nil {
		return nil, errors.Tag(err)
	}
	if !ready {
		return nil, errors.Tag(ErrCacheMiss)
	}
	return values, nil
}

// readCollectionLimitedSnapshot 原子读取可选完成代际，并在同一 Lua 边界内限制集合规模。
func (s *RedisStore) readCollectionLimitedSnapshot(ctx context.Context, key string, typ CacheType, resultKey string) ([]any, bool, string, bool, error) {
	keys := []string{key}
	if resultKey != "" {
		keys = append(keys, resultKey)
	}
	result, err := readCollectionLimitedScript.Run(ctx, s.client, keys, string(typ), s.maxCollectionReadCount).Slice()
	if err != nil {
		return nil, false, "", false, errors.Tag(err)
	}
	if len(result) < 2 {
		return nil, false, "", false, errors.Errorf("缓存key[%s]集合受限读取返回值无效", key)
	}
	generation, generationReady := redisOptionalString(result[1])
	switch redisValueToString(result[0]) {
	case collectionReadStateMiss:
		if len(result) != 2 {
			return nil, false, "", false, errors.Errorf("缓存key[%s]集合未命中返回值无效", key)
		}
		return nil, false, generation, generationReady, nil
	case collectionReadStateTooLarge:
		if len(result) != 3 {
			return nil, false, "", false, errors.Errorf("缓存key[%s]集合超限返回值无效", key)
		}
		size, parseErr := strconv.ParseInt(redisValueToString(result[2]), 10, 64)
		if parseErr != nil {
			return nil, false, "", false, errors.Wrapf(parseErr, "缓存key[%s]集合成员数返回值无效", key)
		}
		return nil, false, "", false, errors.Wrapf(ErrCollectionTooLarge, "缓存key[%s]集合成员数[%d]超过读取上限[%d]", key, size, s.maxCollectionReadCount)
	case collectionReadStateHit:
		if len(result) != 3 {
			return nil, false, "", false, errors.Errorf("缓存key[%s]集合读取返回值无效", key)
		}
		values, ok := result[2].([]any)
		if !ok {
			return nil, false, "", false, errors.Errorf("缓存key[%s]集合读取结果类型错误", key)
		}
		return values, true, generation, generationReady, nil
	default:
		return nil, false, "", false, errors.Errorf("缓存key[%s]集合读取状态无效", key)
	}
}

// readHash 读取 Hash 缓存。
func (s *RedisStore) readHash(ctx context.Context, key string) (map[string]string, error) {
	if s.maxCollectionReadCount > 0 {
		values, err := s.readCollectionLimited(ctx, key, TypeHash)
		if err != nil {
			return nil, errors.Tag(err)
		}
		if len(values)%2 != 0 {
			return nil, errors.Errorf("缓存key[%s]Hash读取结果字段不完整", key)
		}
		result := make(map[string]string, len(values)/2)
		for index := 0; index < len(values); index += 2 {
			result[redisValueToString(values[index])] = redisValueToString(values[index+1])
		}
		return result, nil
	}
	value, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, errors.Tag(ErrCacheMiss)
	}
	return value, nil
}

// readList 读取 List 缓存。
func (s *RedisStore) readList(ctx context.Context, key string) ([]string, error) {
	if s.maxCollectionReadCount > 0 {
		values, err := s.readCollectionLimited(ctx, key, TypeList)
		if err != nil {
			return nil, errors.Tag(err)
		}
		result := make([]string, 0, len(values))
		for _, value := range values {
			result = append(result, redisValueToString(value))
		}
		return result, nil
	}
	value, err := s.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, errors.Tag(ErrCacheMiss)
	}
	return value, nil
}

// readSet 读取 Set 缓存。
func (s *RedisStore) readSet(ctx context.Context, key string) ([]string, error) {
	if s.maxCollectionReadCount > 0 {
		values, err := s.readCollectionLimited(ctx, key, TypeSet)
		if err != nil {
			return nil, errors.Tag(err)
		}
		result := make([]string, 0, len(values))
		for _, value := range values {
			result = append(result, redisValueToString(value))
		}
		return result, nil
	}
	value, err := s.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, errors.Tag(ErrCacheMiss)
	}
	return value, nil
}

// readZSet 读取 ZSet 缓存。
func (s *RedisStore) readZSet(ctx context.Context, key string) ([]ZMember, error) {
	if s.maxCollectionReadCount > 0 {
		values, err := s.readCollectionLimited(ctx, key, TypeZSet)
		if err != nil {
			return nil, errors.Tag(err)
		}
		if len(values)%2 != 0 {
			return nil, errors.Errorf("缓存key[%s]ZSet读取结果成员不完整", key)
		}
		result := make([]ZMember, 0, len(values)/2)
		for index := 0; index < len(values); index += 2 {
			score, parseErr := strconv.ParseFloat(redisValueToString(values[index+1]), 64)
			if parseErr != nil {
				return nil, errors.Wrapf(parseErr, "缓存key[%s]ZSet分数返回值无效", key)
			}
			result = append(result, ZMember{Member: redisValueToString(values[index]), Score: score})
		}
		return result, nil
	}
	value, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, errors.Tag(ErrCacheMiss)
	}
	result := make([]ZMember, 0, len(value))
	for _, item := range value {
		result = append(result, ZMember{Member: item.Member, Score: item.Score})
	}
	return result, nil
}

// readHashPage 分页读取 Hash；指定 Fields 时优先读取固定字段。
func (s *RedisStore) readHashPage(ctx context.Context, key string, options ReadPageOptions) (ReadPageResult, error) {
	fields := options.Fields
	if len(fields) > 0 {
		values, err := s.client.HMGet(ctx, key, fields...).Result()
		if err != nil {
			return ReadPageResult{}, errors.Tag(err)
		}
		result := make(map[string]string, len(fields))
		for index, value := range values {
			if value == nil {
				continue
			}
			result[fields[index]] = redisValueToString(value)
		}
		if len(result) == 0 {
			return ReadPageResult{}, errors.Tag(ErrCacheMiss)
		}
		return ReadPageResult{Value: result}, nil
	}
	count := s.readPageCount(options.Count)
	values, cursor, err := s.client.HScan(ctx, key, options.Cursor, options.Match, count).Result()
	if err != nil {
		return ReadPageResult{}, errors.Tag(err)
	}
	result := make(map[string]string, len(values)/2)
	if int64(len(values)/2) > s.collectionPageLimit() {
		return ReadPageResult{}, errors.Wrapf(ErrCollectionTooLarge, "缓存key[%s]Hash分页结果超过上限[%d]", key, s.collectionPageLimit())
	}
	for index := 0; index+1 < len(values); index += 2 {
		result[values[index]] = values[index+1]
	}
	if len(result) == 0 && cursor == 0 {
		return ReadPageResult{}, errors.Tag(ErrCacheMiss)
	}
	return ReadPageResult{Value: result, Cursor: cursor}, nil
}

// readListPage 分页读取 List。
func (s *RedisStore) readListPage(ctx context.Context, key string, options ReadPageOptions) (ReadPageResult, error) {
	start, stop, err := s.normalizeRange(options)
	if err != nil {
		return ReadPageResult{}, errors.Tag(err)
	}
	values, err := s.client.LRange(ctx, key, start, stop).Result()
	if err != nil {
		return ReadPageResult{}, errors.Tag(err)
	}
	if len(values) == 0 {
		return ReadPageResult{}, errors.Tag(ErrCacheMiss)
	}
	return ReadPageResult{Value: values}, nil
}

// readSetPage 使用 SSCAN 分页读取 Set。
func (s *RedisStore) readSetPage(ctx context.Context, key string, options ReadPageOptions) (ReadPageResult, error) {
	count := s.readPageCount(options.Count)
	values, cursor, err := s.client.SScan(ctx, key, options.Cursor, options.Match, count).Result()
	if err != nil {
		return ReadPageResult{}, errors.Tag(err)
	}
	if len(values) == 0 && cursor == 0 {
		return ReadPageResult{}, errors.Tag(ErrCacheMiss)
	}
	if int64(len(values)) > s.collectionPageLimit() {
		return ReadPageResult{}, errors.Wrapf(ErrCollectionTooLarge, "缓存key[%s]Set分页结果超过上限[%d]", key, s.collectionPageLimit())
	}
	return ReadPageResult{Value: values, Cursor: cursor}, nil
}

// readZSetPage 分页读取 ZSet。
func (s *RedisStore) readZSetPage(ctx context.Context, key string, options ReadPageOptions) (ReadPageResult, error) {
	start, stop, err := s.normalizeRange(options)
	if err != nil {
		return ReadPageResult{}, errors.Tag(err)
	}
	values, err := s.client.ZRangeWithScores(ctx, key, start, stop).Result()
	if err != nil {
		return ReadPageResult{}, errors.Tag(err)
	}
	if len(values) == 0 {
		return ReadPageResult{}, errors.Tag(ErrCacheMiss)
	}
	result := make([]ZMember, 0, len(values))
	for _, item := range values {
		result = append(result, ZMember{Member: item.Member, Score: item.Score})
	}
	return ReadPageResult{Value: result}, nil
}

// normalizeRange 收敛范围读取参数，并拒绝负数或超过单页上限的范围。
func (s *RedisStore) normalizeRange(options ReadPageOptions) (int64, int64, error) {
	start := options.Start
	if start < 0 || options.Stop < 0 {
		return 0, 0, errors.Errorf("集合分页范围不能为负数")
	}
	stop := options.Stop
	if stop == 0 {
		count := s.readPageCount(options.Count)
		const maxInt64 = int64(1<<63 - 1)
		if count-1 > maxInt64-start {
			return 0, 0, errors.Wrapf(ErrCollectionTooLarge, "集合分页范围计算溢出")
		}
		stop = start + count - 1
	}
	if stop < start {
		return 0, 0, errors.Errorf("集合分页Stop不能小于Start")
	}
	limit := s.collectionPageLimit()
	if stop-start >= limit {
		return 0, 0, errors.Wrapf(ErrCollectionTooLarge, "集合分页范围超过单页上限[%d]", limit)
	}
	return start, stop, nil
}

// validateReadPageOptions 校验集合分页参数不超过硬上限。
func (s *RedisStore) validateReadPageOptions(options ReadPageOptions) error {
	limit := s.collectionPageLimit()
	if options.Count > limit {
		return errors.Wrapf(ErrCollectionTooLarge, "集合分页Count[%d]超过单页上限[%d]", options.Count, limit)
	}
	if int64(len(options.Fields)) > limit {
		return errors.Wrapf(ErrCollectionTooLarge, "Hash分页Fields数量超过单页上限[%d]", limit)
	}
	if options.Start < 0 || options.Stop < 0 {
		return errors.Errorf("集合分页范围不能为负数")
	}
	return nil
}

// collectionPageLimit 返回集合分页读取的单页硬上限。
func (s *RedisStore) collectionPageLimit() int64 {
	if s.maxCollectionReadCount > 0 {
		return s.maxCollectionReadCount
	}
	return defaultCollectionPageCount
}

// readPageCount 返回调用方请求或默认收敛后的分页数量。
func (s *RedisStore) readPageCount(count int64) int64 {
	if count > 0 {
		return count
	}
	return min(defaultScanCount, s.collectionPageLimit())
}

// redisValueToString 把 HMGET 返回值转换成字符串。
func redisValueToString(value any) string {
	switch data := value.(type) {
	case string:
		return data
	case []byte:
		return string(data)
	default:
		return fmt.Sprint(data)
	}
}

// redisOptionalString 解析 Redis 可空字符串结果。
func redisOptionalString(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	if flag, ok := value.(bool); ok && !flag {
		return "", false
	}
	return redisValueToString(value), true
}

// decodeCollectionValues 把受限集合脚本的扁平结果转换成公开读取值。
func decodeCollectionValues(key string, typ CacheType, values []any) (any, error) {
	switch typ {
	case TypeHash:
		if len(values)%2 != 0 {
			return nil, errors.Errorf("缓存key[%s]Hash读取结果字段不完整", key)
		}
		result := make(map[string]string, len(values)/2)
		for index := 0; index < len(values); index += 2 {
			result[redisValueToString(values[index])] = redisValueToString(values[index+1])
		}
		return result, nil
	case TypeList, TypeSet:
		result := make([]string, 0, len(values))
		for _, value := range values {
			result = append(result, redisValueToString(value))
		}
		return result, nil
	case TypeZSet:
		if len(values)%2 != 0 {
			return nil, errors.Errorf("缓存key[%s]ZSet读取结果成员不完整", key)
		}
		result := make([]ZMember, 0, len(values)/2)
		for index := 0; index < len(values); index += 2 {
			score, err := strconv.ParseFloat(redisValueToString(values[index+1]), 64)
			if err != nil {
				return nil, errors.Wrapf(err, "缓存key[%s]ZSet分数返回值无效", key)
			}
			result = append(result, ZMember{Member: redisValueToString(values[index]), Score: score})
		}
		return result, nil
	default:
		return nil, errors.Errorf("不支持的Redis集合类型: %s", typ)
	}
}

// ensureCollectionWriteCount 检查单 Entry 集合写入规模。
func (s *RedisStore) ensureCollectionWriteCount(key string, typ CacheType, count int) error {
	if s.maxCollectionWriteCount <= 0 || !isCollectionType(typ) {
		return nil
	}
	if count > s.maxCollectionWriteCount {
		return errors.Wrapf(ErrCollectionTooLarge, "缓存key[%s]集合成员数[%d]超过写入上限[%d]", key, count, s.maxCollectionWriteCount)
	}
	return nil
}

// encodeString 把 String 缓存值转换成字符串，复杂结构统一 JSON 化。
func (s *RedisStore) encodeString(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return s.encoder(typed)
	}
}

// normalizeMap 把任意 map 结构转换成 Redis HSet 可接受的 map[string]any。
func (s *RedisStore) normalizeMap(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	switch typed := value.(type) {
	case map[string]any:
		return s.encodeMapValues(typed)
	case map[string]string:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = item
		}
		return result, nil
	}
	refValue := reflect.ValueOf(value)
	if refValue.Kind() != reflect.Map {
		return nil, errors.Errorf("Hash缓存值必须是map结构")
	}
	if refValue.Type().Key().Kind() != reflect.String {
		return nil, errors.Errorf("Hash缓存值必须使用字符串类型的map key")
	}
	result := make(map[string]any, refValue.Len())
	for _, mapKey := range refValue.MapKeys() {
		item, err := s.encodeRedisValue(refValue.MapIndex(mapKey).Interface())
		if err != nil {
			return nil, errors.Tag(err)
		}
		result[mapKey.String()] = item
	}
	return result, nil
}

// encodeMapValues 对 map 值做复杂类型序列化。
func (s *RedisStore) encodeMapValues(value map[string]any) (map[string]any, error) {
	result := make(map[string]any, len(value))
	for field, item := range value {
		encoded, err := s.encodeRedisValue(item)
		if err != nil {
			return nil, errors.Tag(err)
		}
		result[field] = encoded
	}
	return result, nil
}

// normalizeSlice 把数组或切片转换成 Redis List/Set 可接受的参数数组。
func (s *RedisStore) normalizeSlice(value any) ([]any, error) {
	if value == nil {
		return []any{}, nil
	}
	switch typed := value.(type) {
	case []any:
		return s.encodeSliceValues(typed)
	case []string:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, item)
		}
		return result, nil
	case []int:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, item)
		}
		return result, nil
	}
	refValue := reflect.ValueOf(value)
	if refValue.Kind() != reflect.Slice && refValue.Kind() != reflect.Array {
		return nil, errors.Errorf("List/Set缓存值必须是数组或切片")
	}
	result := make([]any, 0, refValue.Len())
	for i := 0; i < refValue.Len(); i++ {
		item, err := s.encodeRedisValue(refValue.Index(i).Interface())
		if err != nil {
			return nil, errors.Tag(err)
		}
		result = append(result, item)
	}
	return result, nil
}

// encodeSliceValues 对切片值做复杂类型序列化。
func (s *RedisStore) encodeSliceValues(value []any) ([]any, error) {
	result := make([]any, 0, len(value))
	for _, item := range value {
		encoded, err := s.encodeRedisValue(item)
		if err != nil {
			return nil, errors.Tag(err)
		}
		result = append(result, encoded)
	}
	return result, nil
}

// normalizeZSet 把 ZSet 输入转换成 go-redis Z 结构。
func (s *RedisStore) normalizeZSet(value any) ([]redis.Z, error) {
	if value == nil {
		return []redis.Z{}, nil
	}
	switch typed := value.(type) {
	case []ZMember:
		result := make([]redis.Z, 0, len(typed))
		for _, item := range typed {
			if math.IsNaN(item.Score) || math.IsInf(item.Score, 0) {
				return nil, errors.Errorf("ZSet缓存score必须是有限数")
			}
			member, err := s.encodeRedisValue(item.Member)
			if err != nil {
				return nil, errors.Tag(err)
			}
			result = append(result, redis.Z{Score: item.Score, Member: member})
		}
		return result, nil
	case map[string]float64:
		result := make([]redis.Z, 0, len(typed))
		for member, score := range typed {
			if math.IsNaN(score) || math.IsInf(score, 0) {
				return nil, errors.Errorf("ZSet缓存score必须是有限数")
			}
			result = append(result, redis.Z{Score: score, Member: member})
		}
		return result, nil
	}
	return nil, errors.Errorf("ZSet缓存值必须是[]ZMember或map[string]float64")
}

// encodeRedisValue 把 Redis 成员值转换成可安全写入的标量或 JSON 字符串。
func (s *RedisStore) encodeRedisValue(value any) (any, error) {
	if value == nil {
		return "", nil
	}
	switch value.(type) {
	case string, []byte, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return value, nil
	default:
		return s.encoder(value)
	}
}

// defaultEncoder 使用标准 JSON 序列化复杂缓存值。
func defaultEncoder(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", errors.Tag(err)
	}
	return string(body), nil
}

// jitterDurationWithDefault 给基础 TTL 增加抖动；未传显式 jitter 时按默认比例计算。
func jitterDurationWithDefault(base time.Duration, jitter time.Duration, defaultRatio float64) time.Duration {
	if base <= 0 {
		return 0
	}
	if jitter <= 0 {
		if defaultRatio <= 0 {
			return base
		}
		jitter = time.Duration(float64(base) * defaultRatio)
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if jitter > maxDuration-base {
		jitter = maxDuration - base
	}
	if jitter <= 0 {
		return base
	}
	// #nosec G404 -- TTL 抖动只用于分散过期时间，不生成锁 owner 或其它安全凭据。
	return base + time.Duration(randv2.Int64N(int64(jitter)))
}

// ttlMilliseconds 把 Go TTL 转换成 Redis PEXPIRE 使用的毫秒值，并向上保留亚毫秒 TTL。
func ttlMilliseconds(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	milliseconds := ttl.Milliseconds()
	if milliseconds <= 0 {
		return 1
	}
	return milliseconds
}

// entryShouldOverwrite 判断当前写入是否需要先异步删除旧 key。
func entryShouldOverwrite(entry Entry) bool {
	if entry.Overwrite == nil {
		return true
	}
	return *entry.Overwrite
}
