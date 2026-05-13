package tablecache

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var releaseLockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

var refreshLockScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("pexpire", KEYS[1], ARGV[2])
	end
	return 0
	`)

const replaceCollectionScript = `
local key = KEYS[1]
local kind = ARGV[1]
local ttl = tonumber(ARGV[2])
if ttl == nil then
	return redis.error_reply("invalid ttl")
end

redis.call("unlink", key)

local function flush(command, values)
	if #values > 0 then
		redis.call(command, key, unpack(values))
	end
end

if kind == "hash" then
	if ((#ARGV - 2) % 2) ~= 0 then
		return redis.error_reply("invalid hash arguments")
	end
	local batch = {}
	for i = 3, #ARGV, 2 do
		batch[#batch + 1] = ARGV[i]
		batch[#batch + 1] = ARGV[i + 1]
		if #batch >= 256 then
			flush("hset", batch)
			batch = {}
		end
	end
	flush("hset", batch)
elseif kind == "list" then
	local batch = {}
	for i = 3, #ARGV do
		batch[#batch + 1] = ARGV[i]
		if #batch >= 256 then
			flush("rpush", batch)
			batch = {}
		end
	end
	flush("rpush", batch)
elseif kind == "set" then
	local batch = {}
	for i = 3, #ARGV do
		batch[#batch + 1] = ARGV[i]
		if #batch >= 256 then
			flush("sadd", batch)
			batch = {}
		end
	end
	flush("sadd", batch)
elseif kind == "zset" then
	if ((#ARGV - 2) % 2) ~= 0 then
		return redis.error_reply("invalid zset arguments")
	end
	local batch = {}
	for i = 3, #ARGV, 2 do
		batch[#batch + 1] = ARGV[i]
		batch[#batch + 1] = ARGV[i + 1]
		if #batch >= 256 then
			flush("zadd", batch)
			batch = {}
		end
	end
	flush("zadd", batch)
else
	return redis.error_reply("unsupported collection type: " .. kind)
end

if ttl > 0 then
	redis.call("pexpire", key, ttl)
end
return 1
	`

const (
	// defaultUnlinkChunkSize 表示一次 Pipeline 中聚合的 UNLINK 命令数量，避免超大 pipeline 带来内存尖刺。
	defaultUnlinkChunkSize = 512
	// defaultScanUnlinkConcurrency 表示单个 Redis 节点执行 DeletePattern 时的默认删除 worker 数，1 表示保持扫描与删除串行。
	defaultScanUnlinkConcurrency = 1
	// defaultWriteBatchChunkSize 表示一次写入 Pipeline 中最多聚合的 Entry 数量，避免全量刷新生成超大请求。
	defaultWriteBatchChunkSize = 256
)

// RedisStoreOption 表示 RedisStore 可选配置。
type RedisStoreOption func(*RedisStore)

// RedisStore 是基于 go-redis 的 Store 适配器，可直接服务 go-zero、Gin、Kratos 等 Go 框架。
type RedisStore struct {
	client                redis.UniversalClient // client 是 Redis 通用客户端，兼容单机、哨兵和集群
	encoder               Encoder               // encoder 是复杂值序列化函数
	pipelineRetries       int                   // pipelineRetries 表示批量 UNLINK/覆盖写在瞬时失败时的最大重试次数
	unlinkChunkSize       int                   // unlinkChunkSize 表示每个 Pipeline 批次最多聚合的 UNLINK 命令数，用于控制网络往返与内存占用平衡
	scanUnlinkConcurrency int                   // scanUnlinkConcurrency 表示 DeletePattern 扫描期间并发执行 UNLINK 的 worker 数，适合大 keyspace 加速删除
}

// PipelineExecError 表示 Pipeline 执行失败的增强错误信息。
// 在部分命令失败（例如 cluster 路由抖动、网络短暂错误）时可携带失败 key 列表，便于线上快速定位与重试。
type PipelineExecError struct {
	Operation  string   // Operation 表示本次 Pipeline 执行的操作名称（用于日志与排障归类）
	FailedKeys []string // FailedKeys 表示本次 Pipeline 中检测到失败的 key 列表（已排序去重）
	Cause      error    // Cause 是底层错误原因
}

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

func (e *PipelineExecError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type pipelineCmd struct {
	key string      // key 表示当前命令关联的业务 key，用于错误聚合时定位失败项
	cmd redis.Cmder // cmd 表示当前 pipeline 中的具体命令实例
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

// stringSliceToAny 把字符串切片转换为 go-redis 可接受的可变参数列表，避免每个调用点重复分配逻辑。
func stringSliceToAny(values []string) []any {
	args := make([]any, 0, len(values)) // args 是传给 Redis 命令的成员参数列表
	for _, value := range values {
		args = append(args, value)
	}
	return args
}

// cleanMutationEntries 清洗合并变更中的写入条目，只裁剪 key 空白；空 key 保留给 enqueueWrite 返回明确错误。
func cleanMutationEntries(entries []Entry) []Entry {
	cleanEntries := make([]Entry, 0, len(entries)) // cleanEntries 表示裁剪 key 空白后的写入条目列表
	for _, entry := range entries {
		entry.Key = strings.TrimSpace(entry.Key)
		cleanEntries = append(cleanEntries, entry)
	}
	return cleanEntries
}

// cleanPrefixIndexMutations 清洗前缀索引变更，过滤空索引 key 与空成员列表。
func cleanPrefixIndexMutations(mutations []PrefixIndexMutation) []PrefixIndexMutation {
	cleanMutations := make([]PrefixIndexMutation, 0, len(mutations)) // cleanMutations 表示可直接提交给 Redis 的索引变更列表
	for _, mutation := range mutations {
		mutation.IndexKey = strings.TrimSpace(mutation.IndexKey)
		mutation.Keys = cleanRedisKeys(mutation.Keys)
		if mutation.IndexKey != "" && len(mutation.Keys) > 0 {
			cleanMutations = append(cleanMutations, mutation)
		}
	}
	return cleanMutations
}

// mutationBatchCount 计算合并变更按 chunkSize 拆分后的批次数。
func mutationBatchCount(chunkSize int, primaryLen int, indexMutations []PrefixIndexMutation) int {
	maxLen := primaryLen // maxLen 表示所有主数据与索引成员中的最大列表长度
	for _, mutation := range indexMutations {
		if len(mutation.Keys) > maxLen {
			maxLen = len(mutation.Keys)
		}
	}
	if maxLen == 0 {
		return 0
	}
	if chunkSize <= 0 {
		chunkSize = defaultWriteBatchChunkSize
	}
	return (maxLen + chunkSize - 1) / chunkSize
}

// mutationBatchRange 返回第 batch 个批次在长度 total 上的左右边界。
func mutationBatchRange(total int, batch int, chunkSize int) (int, int, bool) {
	start := batch * chunkSize // start 表示当前批次起始下标
	if start >= total {
		return 0, 0, false
	}
	end := start + chunkSize // end 表示当前批次结束下标，不超过 total
	if end > total {
		end = total
	}
	return start, end, true
}

// NewRedisStore 创建 go-redis 存储适配器。
func NewRedisStore(client redis.UniversalClient, opts ...RedisStoreOption) *RedisStore {
	store := &RedisStore{
		client:                client,
		encoder:               defaultEncoder,
		pipelineRetries:       1,
		unlinkChunkSize:       defaultUnlinkChunkSize,
		scanUnlinkConcurrency: defaultScanUnlinkConcurrency,
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

// WithUnlinkChunkSize 设置批量删除时单个 Pipeline 的 UNLINK 命令数量。
// 较大的 chunk 可减少网络往返，较小的 chunk 可降低单次请求体积和 Redis 侧瞬时压力。
func WithUnlinkChunkSize(size int) RedisStoreOption {
	return func(store *RedisStore) {
		if size > 0 {
			store.unlinkChunkSize = size
		}
	}
}

// WithScanUnlinkConcurrency 设置 DeletePattern 在单个 Redis 节点上的并发删除 worker 数。
// SCAN 游标本身必须顺序推进；该配置只把扫描到的 key 分发给多个 UNLINK worker，用于大 keyspace 下重叠扫描和删除耗时。
func WithScanUnlinkConcurrency(concurrency int) RedisStoreOption {
	return func(store *RedisStore) {
		if concurrency > 0 {
			store.scanUnlinkConcurrency = concurrency
		}
	}
}

// WithRedisEncoder 设置 Redis 复杂值序列化函数。
func WithRedisEncoder(encoder Encoder) RedisStoreOption {
	return func(store *RedisStore) {
		if encoder != nil {
			store.encoder = encoder
		}
	}
}

// WithPipelineRetries 设置批量删除与覆盖写的瞬时失败重试次数。
func WithPipelineRetries(retries int) RedisStoreOption {
	return func(store *RedisStore) {
		if retries >= 0 {
			store.pipelineRetries = retries
		}
	}
}

// ApplyMutation 使用有界 Pipeline 合并提交删除、写入与前缀索引维护，减少刷新链路 Redis 往返。
func (s *RedisStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	if s == nil || s.client == nil {
		return errors.Errorf("Redis客户端未初始化")
	}
	deleteKeys := cleanRedisKeys(mutation.DeleteKeys)              // deleteKeys 表示本次需要 UNLINK 的真实 Redis key
	removeIndex := cleanPrefixIndexMutations(mutation.RemoveIndex) // removeIndex 表示本次需要从索引集合剔除的成员批次
	writeEntries := cleanMutationEntries(mutation.WriteEntries)    // writeEntries 表示本次需要写入的缓存条目
	addIndex := cleanPrefixIndexMutations(mutation.AddIndex)       // addIndex 表示本次需要加入索引集合的成员批次
	if err := s.validateWriteEntries(writeEntries); err != nil {
		return errors.Tag(err)
	}
	if err := s.applyDeleteIndexMutation(ctx, deleteKeys, removeIndex); err != nil {
		return errors.Tag(err)
	}
	if err := s.applyWriteIndexMutation(ctx, writeEntries, addIndex); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// AddPrefixIndexKeys 把一批真实 Redis key 写入前缀索引集合，供 DeleteByPrefix 优先走索引删除。
func (s *RedisStore) AddPrefixIndexKeys(ctx context.Context, indexKey string, ttl time.Duration, keys ...string) error {
	if s == nil || s.client == nil {
		return errors.Errorf("Redis客户端未初始化")
	}
	indexKey = strings.TrimSpace(indexKey)
	if indexKey == "" {
		return nil
	}
	cleanKeys := cleanRedisKeys(keys) // cleanKeys 表示需要加入索引的有效业务或元信息 key
	if len(cleanKeys) == 0 {
		return nil
	}
	chunkSize := s.unlinkChunkSize // chunkSize 表示单次 SADD 提交的成员数量，复用删除批次配置控制网络包大小
	if chunkSize <= 0 {
		chunkSize = defaultUnlinkChunkSize
	}
	for start := 0; start < len(cleanKeys); start += chunkSize {
		end := start + chunkSize // end 表示当前索引写入批次的右边界，避免一次 SADD 参数过多
		if end > len(cleanKeys) {
			end = len(cleanKeys)
		}
		pipe := s.client.Pipeline() // pipe 合并 SADD 与 EXPIRE，减少索引维护的网络往返
		// SADD 只把 indexKey 当作 Redis key，成员是普通字符串，因此兼容 Cluster 跨槽业务 key。
		pipe.SAdd(ctx, indexKey, stringSliceToAny(cleanKeys[start:end])...)
		if ttl > 0 {
			pipe.Expire(ctx, indexKey, ttl)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return errors.Tag(err)
		}
	}
	return nil
}

// RemovePrefixIndexKeys 从前缀索引集合移除一批 key，避免 DeleteByKey 或状态切换后留下大量过期成员。
func (s *RedisStore) RemovePrefixIndexKeys(ctx context.Context, indexKey string, keys ...string) error {
	if s == nil || s.client == nil {
		return errors.Errorf("Redis客户端未初始化")
	}
	indexKey = strings.TrimSpace(indexKey)
	if indexKey == "" {
		return nil
	}
	cleanKeys := cleanRedisKeys(keys) // cleanKeys 表示需要从索引中移除的有效成员
	if len(cleanKeys) == 0 {
		return nil
	}
	chunkSize := s.unlinkChunkSize // chunkSize 表示单次 SREM 提交的成员数量，避免大索引清理时请求体过大
	if chunkSize <= 0 {
		chunkSize = defaultUnlinkChunkSize
	}
	for start := 0; start < len(cleanKeys); start += chunkSize {
		end := start + chunkSize // end 表示当前索引移除批次的右边界
		if end > len(cleanKeys) {
			end = len(cleanKeys)
		}
		if err := s.client.SRem(ctx, indexKey, stringSliceToAny(cleanKeys[start:end])...).Err(); err != nil {
			return errors.Tag(err)
		}
	}
	return nil
}

// DeletePrefixIndexKeys 分批遍历前缀索引集合，并删除索引内记录的真实 Redis key。
// 该方法只读取索引集合本身，不扫描 Redis 全局 keyspace，适合 key 数特别多的线上实例。
func (s *RedisStore) DeletePrefixIndexKeys(ctx context.Context, indexKey string, count int64) (int64, error) {
	if s == nil || s.client == nil {
		return 0, nil
	}
	indexKey = strings.TrimSpace(indexKey)
	if indexKey == "" {
		return 0, nil
	}
	if count <= 0 {
		count = defaultScanCount
	}
	cursor := uint64(0)    // cursor 是 SSCAN 返回的集合游标；遍历期间不修改索引集合，避免游标遗漏成员
	var deletedCount int64 // deletedCount 表示按索引成员尝试删除的 key 数量，用于 DeleteByPrefix 指标
	for {
		keys, next, err := s.client.SScan(ctx, indexKey, cursor, "*", count).Result() // keys 是当前索引游标返回的一批真实 Redis key
		if err != nil {
			return deletedCount, errors.Tag(err)
		}
		if len(keys) > 0 {
			if err := s.unlinkKeys(ctx, s.client, keys); err != nil {
				return deletedCount, errors.Tag(err)
			}
			deletedCount += int64(len(keys))
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if err := s.unlinkKeys(ctx, s.client, []string{indexKey}); err != nil {
		return deletedCount, errors.Tag(err)
	}
	return deletedCount, nil
}

// Delete 删除一个或多个 Redis key。
func (s *RedisStore) Delete(ctx context.Context, keys ...string) error {
	if s == nil || s.client == nil || len(keys) == 0 {
		return nil
	}
	cleanKeys := cleanRedisKeys(keys)
	if len(cleanKeys) == 0 {
		return nil
	}
	return s.unlinkKeys(ctx, s.client, cleanKeys)
}

// applyDeleteIndexMutation 合并执行旧 key 删除与索引成员移除，保持“先删除真实 key，再清理索引”的语义。
func (s *RedisStore) applyDeleteIndexMutation(ctx context.Context, deleteKeys []string, removeIndex []PrefixIndexMutation) error {
	chunkSize := s.unlinkChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultUnlinkChunkSize
	}
	batches := mutationBatchCount(chunkSize, len(deleteKeys), removeIndex) // batches 表示删除和索引移除需要拆分的 Pipeline 批次数
	for batch := 0; batch < batches; batch++ {
		var finalCmds []pipelineCmd
		err := execPipelineWithRetry(ctx, s.pipelineRetries, func() error {
			pipe := s.client.Pipeline()
			cmds := make([]pipelineCmd, 0, chunkSize*(1+len(removeIndex))) // cmds 用于检查 Pipeline 内单命令错误并聚合失败 key
			if start, end, ok := mutationBatchRange(len(deleteKeys), batch, chunkSize); ok {
				for _, key := range deleteKeys[start:end] {
					cmds = append(cmds, pipelineCmd{key: key, cmd: pipe.Unlink(ctx, key)})
				}
			}
			for _, mutation := range removeIndex {
				start, end, ok := mutationBatchRange(len(mutation.Keys), batch, chunkSize)
				if !ok {
					continue
				}
				cmd := pipe.SRem(ctx, mutation.IndexKey, stringSliceToAny(mutation.Keys[start:end])...)
				cmds = append(cmds, pipelineCmd{key: mutation.IndexKey, cmd: cmd})
			}
			finalCmds = cmds
			if len(cmds) == 0 {
				return nil
			}
			return execAndCheckPipeline(ctx, pipe, cmds)
		})
		if err != nil {
			return errors.Tag(&PipelineExecError{
				Operation:  "apply_mutation_delete_index",
				FailedKeys: failedPipelineKeys(finalCmds),
				Cause:      err,
			})
		}
	}
	return nil
}

// applyWriteIndexMutation 合并执行缓存写入与索引成员添加，减少刷新写回阶段 Redis 往返。
func (s *RedisStore) applyWriteIndexMutation(ctx context.Context, entries []Entry, addIndex []PrefixIndexMutation) error {
	chunkSize := defaultWriteBatchChunkSize
	batches := mutationBatchCount(chunkSize, len(entries), addIndex) // batches 表示写入和索引添加需要拆分的 Pipeline 批次数
	retries := s.pipelineRetries
	if len(entries) > 0 && !entriesRetryable(entries) {
		// 非覆盖写可能包含 RPUSH/SADD/ZADD 等增量语义，失败后自动重试会放大写入效果，因此关闭重试。
		retries = 0
	}
	for batch := 0; batch < batches; batch++ {
		var finalCmds []pipelineCmd
		err := execPipelineWithRetry(ctx, retries, func() error {
			pipe := s.client.Pipeline()
			cmds := make([]pipelineCmd, 0, chunkSize*(2+len(addIndex))) // cmds 用于收集写入命令与索引命令的执行状态
			if start, end, ok := mutationBatchRange(len(entries), batch, chunkSize); ok {
				for _, entry := range entries[start:end] {
					if err := s.enqueueWrite(ctx, pipe, entry, &cmds); err != nil {
						return errors.Tag(err)
					}
				}
			}
			for _, mutation := range addIndex {
				start, end, ok := mutationBatchRange(len(mutation.Keys), batch, chunkSize)
				if !ok {
					continue
				}
				cmd := pipe.SAdd(ctx, mutation.IndexKey, stringSliceToAny(mutation.Keys[start:end])...)
				cmds = append(cmds, pipelineCmd{key: mutation.IndexKey, cmd: cmd})
				if mutation.TTL > 0 {
					expireCmd := pipe.Expire(ctx, mutation.IndexKey, mutation.TTL)
					cmds = append(cmds, pipelineCmd{key: mutation.IndexKey, cmd: expireCmd})
				}
			}
			finalCmds = cmds
			if len(cmds) == 0 {
				return nil
			}
			return execAndCheckPipeline(ctx, pipe, cmds)
		})
		if err != nil {
			return errors.Tag(&PipelineExecError{
				Operation:  "apply_mutation_write_index",
				FailedKeys: failedPipelineKeys(finalCmds),
				Cause:      err,
			})
		}
	}
	return nil
}

// DeletePattern 使用 SCAN 增量删除匹配 key，避免 KEYS 阻塞线上 Redis。
func (s *RedisStore) DeletePattern(ctx context.Context, pattern string, count int64) (int64, error) {
	if s == nil || s.client == nil {
		return 0, nil
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return 0, nil
	}
	if count <= 0 {
		count = defaultScanCount
	}
	if clusterClient, ok := s.client.(*redis.ClusterClient); ok {
		// deletedCount 汇总所有 master 的删除数量；Cluster SCAN 必须按节点执行，ForEachMaster 已提供节点级并发。
		var deletedCount atomic.Int64
		err := clusterClient.ForEachMaster(ctx, func(masterCtx context.Context, master *redis.Client) error {
			count, err := s.scanDeletePattern(masterCtx, master, pattern, count)
			if err != nil {
				return errors.Tag(err)
			}
			deletedCount.Add(count)
			return nil
		})
		return deletedCount.Load(), errors.Tag(err)
	}
	return s.scanDeletePattern(ctx, s.client, pattern, count)
}

// scanDeletePattern 在指定 Redis 客户端上执行一次完整的 SCAN + UNLINK。
func (s *RedisStore) scanDeletePattern(ctx context.Context, client redis.UniversalClient, pattern string, count int64) (int64, error) {
	if s.scanUnlinkConcurrency > 1 {
		return s.scanDeletePatternConcurrent(ctx, client, pattern, count)
	}
	return s.scanDeletePatternSerial(ctx, client, pattern, count)
}

// scanDeletePatternSerial 串行执行 SCAN 与 UNLINK，适合线上默认保守策略，避免清理任务过度抢占 Redis 连接和 CPU。
func (s *RedisStore) scanDeletePatternSerial(ctx context.Context, client redis.UniversalClient, pattern string, count int64) (int64, error) {
	cursor := uint64(0)
	var deletedCount int64
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			return deletedCount, errors.Tag(err)
		}
		if len(keys) > 0 {
			if err := s.unlinkKeys(ctx, client, keys); err != nil {
				return deletedCount, errors.Tag(err)
			}
			deletedCount += int64(len(keys))
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return deletedCount, nil
}

// scanDeletePatternConcurrent 顺序推进 SCAN 游标，并把扫描出的 key 批次交给多个 UNLINK worker。
// jobs 通道按 worker 数限流，避免 Redis 返回大量 key 时在客户端堆积无界内存。
func (s *RedisStore) scanDeletePatternConcurrent(ctx context.Context, client redis.UniversalClient, pattern string, count int64) (int64, error) {
	// workerCount 表示单节点内并发 UNLINK worker 数；只加速删除阶段，不改变 SCAN 游标顺序语义。
	workerCount := s.scanUnlinkConcurrency
	if workerCount <= 1 {
		return s.scanDeletePatternSerial(ctx, client, pattern, count)
	}
	// deletedCount 聚合 worker 删除数量；不同 worker 完成顺序不可预测，必须使用原子计数。
	var deletedCount atomic.Int64
	// groupCtx 在扫描或删除任一阶段失败时取消全链路，避免继续产生 Redis 压力。
	group, groupCtx := errgroup.WithContext(ctx)
	// jobs 是扫描到的 key 批次队列，容量按 worker 数限流，避免客户端因大 keyspace 堆积过多内存。
	jobs := make(chan []string, workerCount)
	for i := 0; i < workerCount; i++ {
		group.Go(func() error {
			for keys := range jobs {
				if len(keys) == 0 {
					continue
				}
				if err := s.unlinkKeys(groupCtx, client, keys); err != nil {
					return errors.Tag(err)
				}
				deletedCount.Add(int64(len(keys)))
			}
			return nil
		})
	}
	group.Go(func() error {
		defer close(jobs)
		// cursor 是 Redis SCAN 返回的游标，必须由单个扫描 goroutine 顺序推进到 0 才算完成。
		cursor := uint64(0)
		for {
			keys, next, err := client.Scan(groupCtx, cursor, pattern, count).Result()
			if err != nil {
				return errors.Tag(err)
			}
			if len(keys) > 0 {
				select {
				case jobs <- keys:
				case <-groupCtx.Done():
					return errors.Tag(groupCtx.Err())
				}
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	})
	if err := group.Wait(); err != nil {
		return deletedCount.Load(), errors.Tag(err)
	}
	return deletedCount.Load(), nil
}

// unlinkKeys 使用 Pipeline 单 key UNLINK，兼容 Redis Cluster 跨 slot 删除。
func (s *RedisStore) unlinkKeys(ctx context.Context, client redis.UniversalClient, keys []string) error {
	// chunkSize 控制单次 Pipeline 聚合的 UNLINK 命令数量，防止一次性提交过多 key 造成网络包和内存尖刺。
	chunkSize := s.unlinkChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultUnlinkChunkSize
	}
	for start := 0; start < len(keys); start += chunkSize {
		end := start + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		var finalCmds []pipelineCmd
		// 即使 Exec 返回 nil，也要检查各命令的 Err，避免漏掉部分失败（集群/网络抖动下更常见）。
		err := execPipelineWithRetry(ctx, s.pipelineRetries, func() error {
			pipe := client.Pipeline()
			cmds := make([]pipelineCmd, 0, end-start)
			for _, key := range keys[start:end] {
				cmds = append(cmds, pipelineCmd{key: key, cmd: pipe.Unlink(ctx, key)})
			}
			finalCmds = cmds
			_, execErr := pipe.Exec(ctx)
			if execErr == nil {
				for _, cmd := range cmds {
					if cmd.cmd.Err() != nil {
						execErr = cmd.cmd.Err()
						break
					}
				}
			}
			return execErr
		})
		if err != nil {
			failed := make([]string, 0, 8)
			for _, cmd := range finalCmds {
				if cmd.cmd.Err() != nil {
					failed = append(failed, cmd.key)
				}
			}
			return errors.Tag(&PipelineExecError{
				Operation:  "unlink_keys",
				FailedKeys: uniqueSortedKeys(failed),
				Cause:      err,
			})
		}
	}
	return nil
}

// Exists 判断 Redis key 是否存在。
func (s *RedisStore) Exists(ctx context.Context, key string) (bool, error) {
	if s == nil || s.client == nil {
		return false, nil
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return false, nil
	}
	count, err := s.client.Exists(ctx, key).Result()
	return count > 0, errors.Tag(err)
}

func (s *RedisStore) ExistsMulti(ctx context.Context, keys ...string) (map[string]bool, error) {
	if s == nil || s.client == nil {
		return map[string]bool{}, nil
	}
	cleanKeys := make([]string, 0, len(keys)) // cleanKeys 表示清洗后的有效 key 列表（去空白）
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key != "" {
			cleanKeys = append(cleanKeys, key)
		}
	}
	if len(cleanKeys) == 0 {
		return map[string]bool{}, nil
	}
	// Pipeline 合并 Exists，减少高频等待轮询阶段的网络往返。
	pipe := s.client.Pipeline() // pipe 用于合并多次 Exists，减少网络往返
	cmds := make([]*redis.IntCmd, 0, len(cleanKeys))
	for _, key := range cleanKeys {
		cmds = append(cmds, pipe.Exists(ctx, key))
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, errors.Tag(err)
	}
	result := make(map[string]bool, len(cleanKeys))
	for index, cmd := range cmds {
		count, cmdErr := cmd.Result()
		if cmdErr != nil {
			return nil, errors.Tag(cmdErr)
		}
		result[cleanKeys[index]] = count > 0
	}
	return result, nil
}

// Read 按缓存类型读取 Redis 原始值。
func (s *RedisStore) Read(ctx context.Context, key string, typ CacheType) (any, error) {
	if s == nil || s.client == nil {
		return nil, errors.Errorf("Redis客户端未初始化")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, ErrCacheMiss
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

// SetNX 设置 Redis 分布式轻量锁。
func (s *RedisStore) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil {
		return false, errors.Errorf("Redis客户端未初始化")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return false, errors.Errorf("Redis锁key不能为空")
	}
	return s.client.SetNX(ctx, key, value, ttl).Result()
}

// RefreshLock 仅当锁值与持有者标识一致时续期锁。
func (s *RedisStore) RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil {
		return false, nil
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" || ttl <= 0 {
		return false, nil
	}
	result, err := refreshLockScript.Run(ctx, s.client, []string{key}, value, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, errors.Tag(err)
	}
	return result > 0, nil
}

// ReleaseLock 仅当锁值与持有者标识一致时释放锁，避免误删其它实例刚抢到的锁。
func (s *RedisStore) ReleaseLock(ctx context.Context, key string, value string) (bool, error) {
	if s == nil || s.client == nil {
		return false, nil
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

// Write 按 Entry 类型写入 Redis，并统一处理 TTL 抖动。
func (s *RedisStore) Write(ctx context.Context, entry Entry) error {
	if s == nil || s.client == nil {
		return errors.Errorf("Redis客户端未初始化")
	}
	return s.WriteBatch(ctx, []Entry{entry})
}

// WriteBatch 使用 Redis Pipeline 批量写入缓存，降低大量 Entry 重建时的网络往返。
func (s *RedisStore) WriteBatch(ctx context.Context, entries []Entry) error {
	if s == nil || s.client == nil {
		return errors.Errorf("Redis客户端未初始化")
	}
	if len(entries) == 0 {
		return nil
	}
	retryable := entriesRetryable(entries)
	// 分批提交避免单个 pipeline 过大导致的网络包过大、内存尖刺与 Redis 侧处理压力。
	const chunkSize = 256
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
			cmds := make([]pipelineCmd, 0, (end-start)*3)
			for _, entry := range entries[start:end] {
				if err := s.enqueueWrite(ctx, pipe, entry, &cmds); err != nil {
					return errors.Tag(err)
				}
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

// entriesRetryable 判断当前批量写是否允许自动重试。
// 仅对“默认覆盖写”开放重试，避免 Overwrite=false 的增量写重复执行导致语义放大。
func entriesRetryable(entries []Entry) bool {
	for _, entry := range entries {
		if !entryShouldOverwrite(entry) {
			return false
		}
	}
	return true
}

// validateWriteEntries 在执行可能有删除副作用的合并变更前预先校验写入项，避免先删旧缓存再发现新值不可写。
func (s *RedisStore) validateWriteEntries(entries []Entry) error {
	for _, entry := range entries {
		if err := s.validateWriteEntry(entry); err != nil {
			return errors.Tag(err)
		}
	}
	return nil
}

// validateWriteEntry 复用写入路径的类型归一化逻辑，只做本地校验，不向 Redis 发送命令。
func (s *RedisStore) validateWriteEntry(entry Entry) error {
	entry.Key = strings.TrimSpace(entry.Key)
	if entry.Key == "" {
		return errors.Errorf("Redis缓存key不能为空")
	}
	if err := validateRedisClusterHashTagKey(entry.Key); err != nil {
		return errors.Tag(err)
	}
	switch entry.Type {
	case TypeString:
		_, err := s.encodeString(entry.Value)
		return errors.Tag(err)
	case TypeHash:
		_, err := s.normalizeMap(entry.Value)
		return errors.Tag(err)
	case TypeList, TypeSet:
		_, err := s.normalizeSlice(entry.Value)
		return errors.Tag(err)
	case TypeZSet:
		_, err := s.normalizeZSet(entry.Value)
		return errors.Tag(err)
	default:
		return errors.Errorf("不支持的Redis缓存类型: %s", entry.Type)
	}
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
	return execErr
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
	if retries < 0 {
		retries = 0
	}
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			if err := waitWithContext(ctx, pipelineRetryDelay(attempt)); err != nil {
				return errors.Tag(err)
			}
		}
		if err := exec(); err != nil {
			lastErr = err
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

// enqueueWrite 把单条缓存写入命令追加到 Pipeline。
func (s *RedisStore) enqueueWrite(ctx context.Context, pipe redis.Pipeliner, entry Entry, cmds *[]pipelineCmd) error {
	entry.Key = strings.TrimSpace(entry.Key)
	if entry.Key == "" {
		return errors.Errorf("Redis缓存key不能为空")
	}
	ttl := jitterDuration(entry.TTL, entry.Jitter)
	switch entry.Type {
	case TypeString:
		return s.enqueueString(ctx, pipe, entry, ttl, cmds)
	case TypeHash:
		return s.enqueueHash(ctx, pipe, entry, ttl, cmds)
	case TypeList:
		return s.enqueueList(ctx, pipe, entry, ttl, cmds)
	case TypeSet:
		return s.enqueueSet(ctx, pipe, entry, ttl, cmds)
	case TypeZSet:
		return s.enqueueZSet(ctx, pipe, entry, ttl, cmds)
	default:
		return errors.Errorf("不支持的Redis缓存类型: %s", entry.Type)
	}
}

// enqueueString 追加 String 缓存写入命令。
func (s *RedisStore) enqueueString(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	value, err := s.encodeString(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	cmd := pipe.Set(ctx, entry.Key, value, ttl)
	*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	return nil
}

// enqueueHash 追加 Hash 缓存写入命令。
func (s *RedisStore) enqueueHash(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	values, err := s.normalizeMap(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	if entryShouldOverwrite(entry) {
		args := make([]any, 0, len(values)*2)
		for field, value := range values {
			args = append(args, field, value)
		}
		s.enqueueCollectionReplace(ctx, pipe, entry.Key, entry.Type, ttl, args, cmds)
		return nil
	}
	if len(values) > 0 {
		cmd := pipe.HSet(ctx, entry.Key, values)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	s.enqueueExpire(ctx, pipe, entry.Key, ttl, cmds)
	return nil
}

// enqueueList 追加 List 缓存写入命令。
func (s *RedisStore) enqueueList(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	values, err := s.normalizeSlice(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	if entryShouldOverwrite(entry) {
		s.enqueueCollectionReplace(ctx, pipe, entry.Key, entry.Type, ttl, values, cmds)
		return nil
	}
	if len(values) > 0 {
		cmd := pipe.RPush(ctx, entry.Key, values...)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	s.enqueueExpire(ctx, pipe, entry.Key, ttl, cmds)
	return nil
}

// enqueueSet 追加 Set 缓存写入命令。
func (s *RedisStore) enqueueSet(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	values, err := s.normalizeSlice(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	if entryShouldOverwrite(entry) {
		s.enqueueCollectionReplace(ctx, pipe, entry.Key, entry.Type, ttl, values, cmds)
		return nil
	}
	if len(values) > 0 {
		cmd := pipe.SAdd(ctx, entry.Key, values...)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	s.enqueueExpire(ctx, pipe, entry.Key, ttl, cmds)
	return nil
}

// enqueueZSet 追加 ZSet 缓存写入命令。
func (s *RedisStore) enqueueZSet(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	values, err := s.normalizeZSet(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	if entryShouldOverwrite(entry) {
		args := make([]any, 0, len(values)*2)
		for _, value := range values {
			args = append(args, value.Score, value.Member)
		}
		s.enqueueCollectionReplace(ctx, pipe, entry.Key, entry.Type, ttl, args, cmds)
		return nil
	}
	if len(values) > 0 {
		cmd := pipe.ZAdd(ctx, entry.Key, values...)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	s.enqueueExpire(ctx, pipe, entry.Key, ttl, cmds)
	return nil
}

// enqueueCollectionReplace 使用单 key Lua 脚本原子覆盖集合结构，避免读到删除后尚未重建的半状态。
func (s *RedisStore) enqueueCollectionReplace(ctx context.Context, pipe redis.Pipeliner, key string, typ CacheType, ttl time.Duration, values []any, cmds *[]pipelineCmd) {
	args := make([]any, 0, len(values)+2)
	args = append(args, string(typ), ttlMilliseconds(ttl))
	args = append(args, values...)
	cmd := pipe.Eval(ctx, replaceCollectionScript, []string{key}, args...)
	*cmds = append(*cmds, pipelineCmd{key: key, cmd: cmd})
}

// enqueueExpire 追加过期时间设置；ttl<=0 表示保留 Redis 默认的永久 key 语义。
func (s *RedisStore) enqueueExpire(ctx context.Context, pipe redis.Pipeliner, key string, ttl time.Duration, cmds *[]pipelineCmd) {
	if ttl <= 0 {
		return
	}
	cmd := pipe.Expire(ctx, key, ttl)
	*cmds = append(*cmds, pipelineCmd{key: key, cmd: cmd})
}

// readString 读取 String 缓存。
func (s *RedisStore) readString(ctx context.Context, key string) (string, error) {
	value, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", ErrCacheMiss
	}
	return value, errors.Tag(err)
}

// readHash 读取 Hash 缓存。
func (s *RedisStore) readHash(ctx context.Context, key string) (map[string]string, error) {
	value, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, ErrCacheMiss
	}
	return value, nil
}

// readList 读取 List 缓存。
func (s *RedisStore) readList(ctx context.Context, key string) ([]string, error) {
	value, err := s.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, ErrCacheMiss
	}
	return value, nil
}

// readSet 读取 Set 缓存。
func (s *RedisStore) readSet(ctx context.Context, key string) ([]string, error) {
	value, err := s.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, ErrCacheMiss
	}
	return value, nil
}

// readZSet 读取 ZSet 缓存。
func (s *RedisStore) readZSet(ctx context.Context, key string) ([]ZMember, error) {
	value, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if len(value) == 0 {
		return nil, ErrCacheMiss
	}
	result := make([]ZMember, 0, len(value))
	for _, item := range value {
		result = append(result, ZMember{Member: item.Member, Score: item.Score})
	}
	return result, nil
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
	result := make(map[string]any, refValue.Len())
	for _, mapKey := range refValue.MapKeys() {
		item, err := s.encodeRedisValue(refValue.MapIndex(mapKey).Interface())
		if err != nil {
			return nil, errors.Tag(err)
		}
		result[fmt.Sprint(mapKey.Interface())] = item
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

// jitterDuration 给基础 TTL 增加抖动，降低同一类 key 同时过期导致的缓存雪崩。
func jitterDuration(base time.Duration, jitter time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if jitter <= 0 {
		jitter = base / 10
	}
	if jitter <= 0 {
		jitter = time.Nanosecond
	}
	var seed uint64
	if err := binary.Read(rand.Reader, binary.LittleEndian, &seed); err != nil {
		seed = uint64(time.Now().UnixNano())
	}
	return base + time.Duration(seed%uint64(jitter))
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
