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

// RedisStoreOption 表示 RedisStore 可选配置。
type RedisStoreOption func(*RedisStore)

// RedisStore 是基于 go-redis 的 Store 适配器，可直接服务 go-zero、Gin、Kratos 等 Go 框架。
type RedisStore struct {
	client          redis.UniversalClient // client 是 Redis 通用客户端，兼容单机、哨兵和集群
	encoder         Encoder               // encoder 是复杂值序列化函数
	pipelineRetries int                   // pipelineRetries 表示批量 UNLINK/覆盖写在瞬时失败时的最大重试次数
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
	if len(e.FailedKeys) == 0 {
		return fmt.Sprintf("tablecache pipeline exec failed op=%s", e.Operation)
	}
	return fmt.Sprintf("tablecache pipeline exec failed op=%s failed_keys=%d", e.Operation, len(e.FailedKeys))
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

// NewRedisStore 创建 go-redis 存储适配器。
func NewRedisStore(client redis.UniversalClient, opts ...RedisStoreOption) *RedisStore {
	store := &RedisStore{
		client:          client,
		encoder:         defaultEncoder,
		pipelineRetries: 1,
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
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

// Delete 删除一个或多个 Redis key。
func (s *RedisStore) Delete(ctx context.Context, keys ...string) error {
	if s == nil || s.client == nil || len(keys) == 0 {
		return nil
	}
	cleanKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key != "" {
			cleanKeys = append(cleanKeys, key)
		}
	}
	if len(cleanKeys) == 0 {
		return nil
	}
	return s.unlinkKeys(ctx, s.client, cleanKeys)
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

// unlinkKeys 使用 Pipeline 单 key UNLINK，兼容 Redis Cluster 跨 slot 删除。
func (s *RedisStore) unlinkKeys(ctx context.Context, client redis.UniversalClient, keys []string) error {
	const chunkSize = 512
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
			failed := make([]string, 0, 16)
			for _, cmd := range finalCmds {
				if cmd.cmd.Err() != nil {
					failed = append(failed, cmd.key)
				}
			}
			return errors.Tag(&PipelineExecError{
				Operation:  "write_batch",
				FailedKeys: uniqueSortedKeys(failed),
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
		cmd := pipe.Unlink(ctx, entry.Key)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if len(values) > 0 {
		cmd := pipe.HSet(ctx, entry.Key, values)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if ttl > 0 {
		cmd := pipe.Expire(ctx, entry.Key, ttl)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	return nil
}

// enqueueList 追加 List 缓存写入命令。
func (s *RedisStore) enqueueList(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	values, err := s.normalizeSlice(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	if entryShouldOverwrite(entry) {
		cmd := pipe.Unlink(ctx, entry.Key)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if len(values) > 0 {
		cmd := pipe.RPush(ctx, entry.Key, values...)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if ttl > 0 {
		cmd := pipe.Expire(ctx, entry.Key, ttl)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	return nil
}

// enqueueSet 追加 Set 缓存写入命令。
func (s *RedisStore) enqueueSet(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	values, err := s.normalizeSlice(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	if entryShouldOverwrite(entry) {
		cmd := pipe.Unlink(ctx, entry.Key)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if len(values) > 0 {
		cmd := pipe.SAdd(ctx, entry.Key, values...)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if ttl > 0 {
		cmd := pipe.Expire(ctx, entry.Key, ttl)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	return nil
}

// enqueueZSet 追加 ZSet 缓存写入命令。
func (s *RedisStore) enqueueZSet(ctx context.Context, pipe redis.Pipeliner, entry Entry, ttl time.Duration, cmds *[]pipelineCmd) error {
	values, err := s.normalizeZSet(entry.Value)
	if err != nil {
		return errors.Tag(err)
	}
	if entryShouldOverwrite(entry) {
		cmd := pipe.Unlink(ctx, entry.Key)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if len(values) > 0 {
		cmd := pipe.ZAdd(ctx, entry.Key, values...)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	if ttl > 0 {
		cmd := pipe.Expire(ctx, entry.Key, ttl)
		*cmds = append(*cmds, pipelineCmd{key: entry.Key, cmd: cmd})
	}
	return nil
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

// entryShouldOverwrite 判断当前写入是否需要先异步删除旧 key。
func entryShouldOverwrite(entry Entry) bool {
	if entry.Overwrite == nil {
		return true
	}
	return *entry.Overwrite
}
