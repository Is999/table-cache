package tablecache

import (
	"context"
	stdErrors "errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// write 写入单条测试数据，统一复用生产 StoreMutation 路径。
func (s *RedisStore) write(ctx context.Context, entry Entry) error {
	return s.ApplyMutation(ctx, StoreMutation{WriteEntries: []Entry{entry}})
}

// writeBatch 批量写入测试数据，统一复用生产 StoreMutation 路径。
func (s *RedisStore) writeBatch(ctx context.Context, entries []Entry) error {
	return s.ApplyMutation(ctx, StoreMutation{WriteEntries: entries})
}

// addPrefixIndexKeys 为索引回归测试写入无锁成员。
func (s *RedisStore) addPrefixIndexKeys(ctx context.Context, indexKey string, ttl time.Duration, keys ...string) error {
	if err := s.validate(); err != nil {
		return errors.Tag(err)
	}
	return s.addPrefixIndexKeysGuarded(ctx, indexKey, ttl, nil, keys...)
}

// addPrefixIndexKeysInternal 为索引故障测试直接写入已清洗成员。
func (s *RedisStore) addPrefixIndexKeysInternal(ctx context.Context, indexKey string, ttl time.Duration, keys []string) error {
	return s.addPrefixIndexKeysGuardedInternal(ctx, indexKey, ttl, keys, nil)
}

// deleteKeys 清理集成测试产生的 Redis key。
func (s *RedisStore) deleteKeys(ctx context.Context, keys ...string) error {
	if err := s.validate(); err != nil {
		return errors.Tag(err)
	}
	_, err := s.unlinkKeys(ctx, s.client, cleanRedisKeys(keys))
	return errors.Tag(err)
}

// deletePattern 使用无锁 SCAN 清理测试命名空间，不参与生产 Manager 路径。
func (s *RedisStore) deletePattern(ctx context.Context, pattern string, count int64) (int64, error) {
	if err := s.validate(); err != nil {
		return 0, errors.Tag(err)
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return 0, nil
	}
	count = normalizeScanCount(count)
	var deleted atomic.Int64
	err := s.forEachScanNode(ctx, func(nodeCtx context.Context, client redis.UniversalClient) error {
		var total int64
		for {
			var cursor uint64
			var candidates int64
			for {
				keys, next, err := client.Scan(nodeCtx, cursor, pattern, count).Result()
				if err != nil {
					return errors.Tag(err)
				}
				candidates += int64(len(keys))
				actual, err := s.unlinkKeys(nodeCtx, client, keys)
				total += actual
				if err != nil {
					return errors.Tag(err)
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
			if candidates == 0 {
				deleted.Add(total)
				return nil
			}
		}
	})
	return deleted.Load(), errors.Tag(err)
}

// TestRedisStoreReplaceWithMarker 验证 marker 脚本写入成功后会删除全部同槽旧 key。
func TestRedisStoreReplaceWithMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client, WithDefaultJitterRatio(0))
	businessKey := "hidden-fault:1"
	markerKey := tablecacheMetaKey("empty", businessKey)
	deleteKeys := []string{
		businessKey,
		tablecacheMetaKey("empty_collection", businessKey),
		tablecacheMetaKey("empty_fields", businessKey),
	}
	for _, key := range deleteKeys {
		if err := client.Set(ctx, key, "old", time.Hour).Err(); err != nil {
			t.Fatalf("Set(%s) error = %v", key, err)
		}
	}
	marker := Entry{Key: markerKey, Type: TypeString, Value: "empty", TTL: time.Minute}
	if err := store.ReplaceWithMarker(ctx, acquireTestLockGuard(t, store, marker.Key), marker, deleteKeys...); err != nil {
		t.Fatalf("ReplaceWithMarker() error = %v", err)
	}
	if got := client.Get(ctx, markerKey).Val(); got != "empty" {
		t.Fatalf("marker = %q, want empty", got)
	}
	if exists := client.Exists(ctx, deleteKeys...).Val(); exists != 0 {
		t.Fatalf("old keys exists = %d, want 0", exists)
	}
}

// TestRedisStoreAcquireRefreshLockReturnsCurrentOwner 验证抢锁失败与owner读取处于同一Lua执行边界。
func TestRedisStoreAcquireRefreshLockReturnsCurrentOwner(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	locked, owner, err := store.AcquireRefreshLock(ctx, "refresh-lock", "leader", time.Minute)
	if err != nil || !locked || owner != "leader" {
		t.Fatalf("AcquireRefreshLock(leader) = %v,%q,%v", locked, owner, err)
	}
	if err := client.PExpire(ctx, "refresh-lock", 10*time.Millisecond).Err(); err != nil {
		t.Fatalf("PExpire(refresh-lock) error = %v", err)
	}
	locked, owner, err = store.AcquireRefreshLock(ctx, "refresh-lock", "leader", time.Minute)
	if err != nil || !locked || owner != "leader" {
		t.Fatalf("AcquireRefreshLock(leader replay) = %v,%q,%v, want true,leader,nil", locked, owner, err)
	}
	if ttl := server.TTL("refresh-lock"); ttl != time.Minute {
		t.Fatalf("refresh lock replay TTL = %s, want 1m", ttl)
	}
	locked, owner, err = store.AcquireRefreshLock(ctx, "refresh-lock", "waiter", time.Minute)
	if err != nil || locked || owner != "leader" {
		t.Fatalf("AcquireRefreshLock(waiter) = %v,%q,%v, want false,leader,nil", locked, owner, err)
	}
	if err := client.Del(ctx, "refresh-lock").Err(); err != nil {
		t.Fatalf("Del(refresh-lock) error = %v", err)
	}
	locked, owner, err = store.AcquireRefreshLock(ctx, "refresh-lock", "waiter", time.Nanosecond)
	if err != nil || !locked || owner != "waiter" {
		t.Fatalf("AcquireRefreshLock(waiter retry) = %v,%q,%v", locked, owner, err)
	}
	if ttl := server.TTL("refresh-lock"); ttl != time.Millisecond {
		t.Fatalf("refresh lock TTL = %s, want 1ms", ttl)
	}
}

// TestRedisStoreWriteBatchPrevalidatesAllChunks 验证后续批次非法时首批数据不会提前提交。
func TestRedisStoreWriteBatchPrevalidatesAllChunks(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	entries := make([]Entry, defaultWriteBatchChunkSize+1)
	for index := range entries {
		entries[index] = Entry{Key: fmt.Sprintf("batch-prevalidate:%d", index), Type: TypeString, Value: index}
	}
	entries[len(entries)-1].Type = CacheType("invalid")
	if err := store.writeBatch(ctx, entries); err == nil {
		t.Fatalf("WriteBatch() error = nil, want invalid type")
	}
	if exists := client.Exists(ctx, entries[0].Key).Val(); exists != 0 {
		t.Fatalf("first batch key exists = %d, want 0", exists)
	}
}

// TestRedisStoreReadPageRejectsUnboundedOptions 验证所有分页入口都不能绕过单页硬上限。
func TestRedisStoreReadPageRejectsUnboundedOptions(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client, WithMaxCollectionReadCount(2))
	if err := client.RPush(ctx, "page:list", "a", "b", "c").Err(); err != nil {
		t.Fatalf("RPush() error = %v", err)
	}
	if err := client.HSet(ctx, "page:hash", "a", 1, "b", 2, "c", 3).Err(); err != nil {
		t.Fatalf("HSet() error = %v", err)
	}
	cases := []struct {
		name    string
		key     string
		typ     CacheType
		options ReadPageOptions
	}{
		{name: "negative stop", key: "page:list", typ: TypeList, options: ReadPageOptions{Stop: -1}},
		{name: "huge stop", key: "page:list", typ: TypeList, options: ReadPageOptions{Stop: math.MaxInt64}},
		{name: "huge zset stop", key: "page:zset", typ: TypeZSet, options: ReadPageOptions{Stop: math.MaxInt64}},
		{name: "large count", key: "page:list", typ: TypeList, options: ReadPageOptions{Count: 3}},
		{name: "large fields", key: "page:hash", typ: TypeHash, options: ReadPageOptions{Fields: []string{"a", "b", "c"}}},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			if _, err := store.ReadPage(ctx, item.key, item.typ, item.options); err == nil {
				t.Fatalf("ReadPage() error = nil, want bounded page error")
			}
		})
	}
	page, err := store.ReadPage(ctx, "page:list", TypeList, ReadPageOptions{Count: 2})
	if err != nil {
		t.Fatalf("ReadPage(valid) error = %v", err)
	}
	if values, ok := page.Value.([]string); !ok || len(values) != 2 {
		t.Fatalf("ReadPage(valid) value = %#v, want 2 items", page.Value)
	}
}

// TestRedisPrefixPatternEscapesGlobCharacters 验证危险前缀按字面量匹配，不会扩大删除范围。
func TestRedisPrefixPatternEscapesGlobCharacters(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	prefix := "glob:[x]*?\\:"
	target := prefix + "1"
	keep := "glob:x-other"
	if err := client.MSet(ctx, target, "delete", keep, "keep").Err(); err != nil {
		t.Fatalf("MSet() error = %v", err)
	}
	deleted, err := store.deletePattern(ctx, RedisPrefixPattern(prefix), 1)
	if err != nil {
		t.Fatalf("DeletePattern() error = %v", err)
	}
	if deleted != 1 || client.Exists(ctx, target).Val() != 0 || client.Exists(ctx, keep).Val() != 1 {
		t.Fatalf("deleted=%d target=%d keep=%d", deleted, client.Exists(ctx, target).Val(), client.Exists(ctx, keep).Val())
	}
}

// TestRedisStoreCollectionWriteTTLAndSafeReplace 验证增量集合TTL精度及覆盖失败保留旧值。
func TestRedisStoreCollectionWriteTTLAndSafeReplace(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client, WithDefaultJitterRatio(0))
	overwrite := false
	entry := Entry{Key: "ttl:set", Type: TypeSet, Value: []string{"a"}, TTL: 1500 * time.Millisecond, Overwrite: &overwrite}
	if err := store.write(ctx, entry); err != nil {
		t.Fatalf("Write(1500ms) error = %v", err)
	}
	if ttl := server.TTL(entry.Key); ttl != 1500*time.Millisecond {
		t.Fatalf("TTL = %s, want 1500ms", ttl)
	}
	entry.Value = []string{"b"}
	entry.TTL = 500 * time.Microsecond
	if err := store.write(ctx, entry); err != nil {
		t.Fatalf("Write(sub-ms) error = %v", err)
	}
	if ttl := server.TTL(entry.Key); ttl != time.Millisecond {
		t.Fatalf("sub-ms TTL = %s, want 1ms", ttl)
	}
	entry.Value = []string{"c"}
	entry.TTL = 0
	if err := store.write(ctx, entry); err != nil {
		t.Fatalf("Write(persist) error = %v", err)
	}
	if ttl := server.TTL(entry.Key); ttl != 0 {
		t.Fatalf("persistent TTL = %s, want 0", ttl)
	}

	if err := client.ZAdd(ctx, "replace:zset", redis.Z{Score: 1, Member: "old"}).Err(); err != nil {
		t.Fatalf("ZAdd(old) error = %v", err)
	}
	bad := Entry{Key: "replace:zset", Type: TypeZSet, Value: []ZMember{{Member: "bad", Score: math.NaN()}}, TTL: time.Minute}
	if err := store.write(ctx, bad); err == nil {
		t.Fatalf("Write(NaN) error = nil")
	}
	if score := client.ZScore(ctx, bad.Key, "old").Val(); score != 1 {
		t.Fatalf("old score = %v, want 1 after NaN failure", score)
	}
	temporaryKey := tablecacheMetaKey("replace:test", bad.Key)
	if err := client.Eval(ctx, replaceCollectionScript, []string{bad.Key, temporaryKey}, "zset", 1000, "nan", "bad").Err(); err == nil {
		t.Fatalf("replaceCollectionScript(NaN) error = nil")
	}
	if score := client.ZScore(ctx, bad.Key, "old").Val(); score != 1 {
		t.Fatalf("old score = %v, want 1 after Lua validation failure", score)
	}
	if exists := client.Exists(ctx, temporaryKey).Val(); exists != 0 {
		t.Fatalf("temporary key exists = %d, want 0", exists)
	}
}

// TestRedisStoreRejectsUnsupportedIncrementalTypes 验证无增量语义或非幂等的类型不会进入写路径。
func TestRedisStoreRejectsUnsupportedIncrementalTypes(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(client)
	for _, entry := range []Entry{
		{Key: "string-incremental", Type: TypeString, Value: "a", Overwrite: Bool(false)},
		{Key: "list-incremental", Type: TypeList, Value: []string{"a"}, Overwrite: Bool(false)},
	} {
		err := store.write(context.Background(), entry)
		if !stdErrors.Is(err, ErrInvalidConfig) {
			t.Fatalf("Write(%s incremental) error = %v, want ErrInvalidConfig", entry.Type, err)
		}
		if exists := client.Exists(context.Background(), entry.Key).Val(); exists != 0 {
			t.Fatalf("incremental key %s exists = %d, want 0", entry.Key, exists)
		}
	}
}

// TestRedisStoreReplacePrefixUsesKeyDifference 验证同毫秒和未来score不会掩盖stale成员。
func TestRedisStoreReplacePrefixUsesKeyDifference(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client, WithDefaultJitterRatio(0))
	indexKey := "tcm:pidx:score"
	readyKey := "tcm:pidx:ready:score"
	oldKeys := []string{"score:same", "score:future"}
	if err := client.MSet(ctx, oldKeys[0], "old", oldKeys[1], "old").Err(); err != nil {
		t.Fatalf("MSet(old) error = %v", err)
	}
	if err := store.addPrefixIndexKeysInternal(ctx, indexKey, time.Hour, oldKeys); err != nil {
		t.Fatalf("addPrefixIndexKeysInternal() error = %v", err)
	}
	futureKey := prefixIndexShardKey(indexKey, prefixIndexMemberShard(oldKeys[1]))
	if err := client.ZAdd(ctx, futureKey, redis.Z{Score: float64(time.Now().Add(24 * time.Hour).UnixMilli()), Member: oldKeys[1]}).Err(); err != nil {
		t.Fatalf("ZAdd(future score) error = %v", err)
	}
	if err := commitTestPrefixIndex(t, store, indexKey, time.Hour); err != nil {
		t.Fatalf("commitPrefixIndex() error = %v", err)
	}
	if err := client.Set(ctx, readyKey, "ready", time.Hour).Err(); err != nil {
		t.Fatalf("Set(ready) error = %v", err)
	}
	deleted, err := store.ReplacePrefix(ctx, PrefixReplaceMutation{
		Guards:         acquireTestLockGuard(t, store, "score:"),
		Prefix:         "score:",
		Entries:        []Entry{{Key: "score:new", Type: TypeString, Value: "new", TTL: time.Hour}},
		DeletePatterns: []string{RedisPrefixPattern("score:")},
		IndexKey:       indexKey,
		ReadyKey:       readyKey,
		IndexTTL:       time.Hour,
		ScanCount:      1,
	})
	if err != nil {
		t.Fatalf("ReplacePrefix() error = %v", err)
	}
	if deleted != 2 || client.Exists(ctx, oldKeys...).Val() != 0 {
		t.Fatalf("deleted = %d old_exists=%d, want 2/0", deleted, client.Exists(ctx, oldKeys...).Val())
	}
}

// TestRedisStoreReplacePrefixWriteFailureInvalidatesReady 验证部分写故障只留下冗余数据和索引。
func TestRedisStoreReplacePrefixWriteFailureInvalidatesReady(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client, WithPipelineRetries(0), WithDefaultJitterRatio(0))
	indexKey := "tcm:pidx:fault"
	readyKey := "tcm:pidx:ready:fault"
	if err := client.Set(ctx, "fault:old", "old", time.Hour).Err(); err != nil {
		t.Fatalf("Set(old) error = %v", err)
	}
	if err := store.addPrefixIndexKeysInternal(ctx, indexKey, time.Hour, []string{"fault:old"}); err != nil {
		t.Fatalf("addPrefixIndexKeysInternal(old) error = %v", err)
	}
	if err := commitTestPrefixIndex(t, store, indexKey, time.Hour); err != nil {
		t.Fatalf("commitPrefixIndex() error = %v", err)
	}
	if err := client.Set(ctx, readyKey, "ready", time.Hour).Err(); err != nil {
		t.Fatalf("Set(ready) error = %v", err)
	}
	hook := &redisFaultHook{
		pipelineMatch: func(cmds []redis.Cmder) bool {
			for _, cmd := range cmds {
				args := cmd.Args()
				if strings.EqualFold(cmd.Name(), "eval") && len(args) > 1 && fmt.Sprint(args[1]) == guardedMutationLua && pipelineContainsKey([]redis.Cmder{cmd}, "fault:new:1") {
					return true
				}
			}
			return false
		},
		partialPipelineCommands: 1,
	}
	client.AddHook(hook)
	_, err := store.ReplacePrefix(ctx, PrefixReplaceMutation{
		Guards: acquireTestLockGuard(t, store, "fault:"),
		Prefix: "fault:",
		Entries: []Entry{
			{Key: "fault:new:1", Type: TypeString, Value: "one", TTL: time.Hour},
			{Key: "fault:new:2", Type: TypeString, Value: "two", TTL: time.Hour},
		},
		DeletePatterns: []string{RedisPrefixPattern("fault:")},
		IndexKey:       indexKey,
		ReadyKey:       readyKey,
		IndexTTL:       time.Hour,
		ScanCount:      1,
	})
	if err == nil {
		t.Fatalf("ReplacePrefix() error = nil, want injected write failure")
	}
	if exists := client.Exists(ctx, readyKey).Val(); exists != 0 {
		t.Fatalf("ready exists = %d, want 0 after failure", exists)
	}
	if got := client.Get(ctx, "fault:old").Val(); got != "old" {
		t.Fatalf("fault:old = %q, want old preserved", got)
	}
	for _, key := range []string{"fault:new:1", "fault:new:2"} {
		if !testPrefixIndexContains(ctx, client, indexKey, key) {
			t.Fatalf("index missing %s after partial write failure", key)
		}
	}
	deleted, deleteErr := store.deletePattern(ctx, RedisPrefixPattern("fault:"), 1)
	if deleteErr != nil {
		t.Fatalf("DeletePattern() error = %v", deleteErr)
	}
	if deleted < 1 || client.Exists(ctx, "fault:old", "fault:new:1", "fault:new:2").Val() != 0 {
		t.Fatalf("fallback deleted=%d remaining=%d", deleted, client.Exists(ctx, "fault:old", "fault:new:1", "fault:new:2").Val())
	}
}

// TestRedisStoreApplyMutationDeleteFailureKeepsIndex 验证业务删除失败时不会先移除索引。
func TestRedisStoreApplyMutationDeleteFailureKeepsIndex(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client, WithPipelineRetries(0))
	indexKey := "tcm:pidx:delete-fault"
	key := "delete-fault:1"
	if err := client.Set(ctx, key, "value", 0).Err(); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, key); err != nil {
		t.Fatalf("AddPrefixIndexKeys() error = %v", err)
	}
	client.AddHook(&redisFaultHook{pipelineMatch: func(cmds []redis.Cmder) bool {
		return pipelineContainsKey(cmds, key) && pipelineContainsCommand(cmds, "unlink")
	}})
	err := store.ApplyMutation(ctx, StoreMutation{
		DeleteKeys: []string{key},
	})
	if err == nil {
		t.Fatalf("ApplyMutation() error = nil, want injected unlink failure")
	}
	if exists := client.Exists(ctx, key).Val(); exists != 1 {
		t.Fatalf("business key exists = %d, want 1", exists)
	}
	if !testPrefixIndexContains(ctx, client, indexKey, key) {
		t.Fatalf("index member removed before business delete")
	}
}

// TestRedisStorePrefixIndexShardsAndActualDeleteCount 验证索引分槽且删除指标使用真实UNLINK数量。
func TestRedisStorePrefixIndexShardsAndActualDeleteCount(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:sharded"
	keys := []string{"sharded:1", "sharded:2", "sharded:3", "sharded:4"}
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, keys...); err != nil {
		t.Fatalf("AddPrefixIndexKeys() error = %v", err)
	}
	if err := commitTestPrefixIndex(t, store, indexKey, time.Hour); err != nil {
		t.Fatalf("commitPrefixIndex() error = %v", err)
	}
	tags := make(map[string]struct{})
	for _, key := range keys {
		shardKey := prefixIndexShardKey(indexKey, prefixIndexMemberShard(key))
		tags[redisClusterHashTag(shardKey)] = struct{}{}
	}
	if len(tags) < 2 {
		t.Fatalf("shard hash tags = %d, want distributed slots", len(tags))
	}
	if err := client.Set(ctx, keys[0], "exists", 0).Err(); err != nil {
		t.Fatalf("Set(existing member) error = %v", err)
	}
	deleted, err := store.DeletePrefixIndexKeys(ctx, indexKey, 1, acquireTestLockGuard(t, store, "index-delete-1"))
	if err != nil {
		t.Fatalf("DeletePrefixIndexKeys() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want actual count 1", deleted)
	}
}

// TestRedisStoreValidateRejectsInvalidConfiguration 验证nil客户端和非法option不会静默成功。
func TestRedisStoreValidateRejectsInvalidConfiguration(t *testing.T) {
	if err := NewRedisStore(nil).ValidateTablecacheStore(); err == nil {
		t.Fatalf("Validate(nil client) error = nil")
	}
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	for name, store := range map[string]*RedisStore{
		"negative retries": NewRedisStore(client, WithPipelineRetries(-1)),
		"nan jitter":       NewRedisStore(client, WithDefaultJitterRatio(math.NaN())),
		"negative limit":   NewRedisStore(client, WithMaxCollectionReadCount(-1)),
	} {
		t.Run(name, func(t *testing.T) {
			if err := store.ValidateTablecacheStore(); err == nil {
				t.Fatalf("ValidateTablecacheStore() error = nil")
			}
		})
	}
	if err := NewRedisStore(client).write(context.Background(), Entry{Key: "bad-ttl", Type: TypeString, Value: "x", TTL: -1}); err == nil {
		t.Fatalf("Write(negative TTL) error = nil")
	}
}

// TestRedisStoreEncoderRunsOncePerValue 验证prepared写入不会跨预校验和分批提交重复编码。
func TestRedisStoreEncoderRunsOncePerValue(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var calls atomic.Int64
	encoder := func(value any) (string, error) {
		calls.Add(1)
		return fmt.Sprintf("encoded:%v", value), nil
	}
	store := NewRedisStore(client, WithRedisEncoder(encoder))
	entries := make([]Entry, defaultWriteBatchChunkSize+1)
	for index := range entries {
		entries[index] = Entry{Key: fmt.Sprintf("encode-once:%d", index), Type: TypeString, Value: struct{ ID int }{ID: index}}
	}
	if err := store.writeBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if got := calls.Load(); got != int64(len(entries)) {
		t.Fatalf("encoder calls = %d, want %d", got, len(entries))
	}

	calls.Store(0)
	failingStore := NewRedisStore(client, WithRedisEncoder(func(value any) (string, error) {
		if calls.Add(1) == int64(len(entries)) {
			return "", fmt.Errorf("injected encoder failure")
		}
		return "ok", nil
	}))
	for index := range entries {
		entries[index].Key = fmt.Sprintf("encode-fail:%d", index)
	}
	if err := failingStore.writeBatch(ctx, entries); err == nil {
		t.Fatalf("WriteBatch(failing encoder) error = nil")
	}
	if exists := client.Exists(ctx, entries[0].Key).Val(); exists != 0 {
		t.Fatalf("first key exists = %d, want 0", exists)
	}
	if got := calls.Load(); got != int64(len(entries)) {
		t.Fatalf("failing encoder calls = %d, want %d", got, len(entries))
	}
}

// TestRedisStoreFieldsEmptyRegistry 验证字段组合TTL彼此独立、过期有界且可精确清理。
func TestRedisStoreFieldsEmptyRegistry(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	start := time.Now().Truncate(time.Millisecond)
	server.SetTime(start)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	registryKey := "tcm:empty:fields:{user:1}"
	if err := store.ReplaceFieldsWithEmpty(ctx, acquireTestLockGuard(t, store, "user:1:name"), "user:1", registryKey, "name", []string{"name"}, time.Second); err != nil {
		t.Fatalf("ReplaceFieldsWithEmpty(name) error = %v", err)
	}
	if err := store.ReplaceFieldsWithEmpty(ctx, acquireTestLockGuard(t, store, "user:1:age"), "user:1", registryKey, "age", []string{"age"}, 3*time.Second); err != nil {
		t.Fatalf("ReplaceFieldsWithEmpty(age) error = %v", err)
	}
	for _, fieldsID := range []string{"name", "age"} {
		has, err := store.HasFieldsEmpty(ctx, registryKey, fieldsID)
		if err != nil || !has {
			t.Fatalf("HasFieldsEmpty(%s) = %v,%v, want true,nil", fieldsID, has, err)
		}
	}
	server.SetTime(start.Add(2 * time.Second))
	server.FastForward(2 * time.Second)
	name, err := store.HasFieldsEmpty(ctx, registryKey, "name")
	if err != nil || name {
		t.Fatalf("HasFieldsEmpty(name) = %v,%v, want false,nil", name, err)
	}
	age, err := store.HasFieldsEmpty(ctx, registryKey, "age")
	if err != nil || !age {
		t.Fatalf("HasFieldsEmpty(age) = %v,%v, want true,nil", age, err)
	}
	if size := client.ZCard(ctx, registryKey).Val(); size != 1 {
		t.Fatalf("registry size = %d, want 1 after pruning", size)
	}
	server.SetTime(start.Add(4 * time.Second))
	server.FastForward(2 * time.Second)
	age, err = store.HasFieldsEmpty(ctx, registryKey, "age")
	if err != nil || age {
		t.Fatalf("HasFieldsEmpty(age) after expiry = %v,%v, want false,nil", age, err)
	}
	if exists := client.Exists(ctx, registryKey).Val(); exists != 0 {
		t.Fatalf("registry exists = %d, want 0 after all members expire", exists)
	}
}

// TestRedisStoreReplaceFieldsWithEmptyPrevalidatesTypes 验证类型错误不会先删除旧 Hash 字段。
func TestRedisStoreReplaceFieldsWithEmptyPrevalidatesTypes(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	key := "fields-type:1"
	registryKey := tablecacheMetaKey("empty_fields", key)
	if err := client.HSet(ctx, key, "name", "alpha", "status", "old").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, registryKey, "wrong-type", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceFieldsWithEmpty(ctx, acquireTestLockGuard(t, store, key), key, registryKey, "status", []string{"status"}, time.Minute); err == nil {
		t.Fatal("ReplaceFieldsWithEmpty() error = nil, want registry type error")
	}
	if value := client.HGet(ctx, key, "status").Val(); value != "old" {
		t.Fatalf("status = %q, want old after rejected replacement", value)
	}
}

// TestRedisStorePrefixIndexEvictionInvalidatesTrust 验证分片被淘汰后不会继续信任ready索引。
func TestRedisStorePrefixIndexEvictionInvalidatesTrust(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	storeA := NewRedisStore(client)
	storeB := NewRedisStore(client)
	indexKey := "tcm:pidx:evict"
	readyKey := "tcm:pidx:ready:evict"
	oldKey := "evict:old"
	if err := client.Set(ctx, oldKey, "old", 0).Err(); err != nil {
		t.Fatalf("Set(old) error = %v", err)
	}
	if err := storeA.addPrefixIndexKeys(ctx, indexKey, time.Hour, oldKey); err != nil {
		t.Fatalf("AddPrefixIndexKeys(old) error = %v", err)
	}
	if err := commitTestPrefixIndex(t, storeA, indexKey, time.Hour); err != nil {
		t.Fatalf("commitPrefixIndex() error = %v", err)
	}
	if err := client.Set(ctx, readyKey, "ready", time.Hour).Err(); err != nil {
		t.Fatalf("Set(ready) error = %v", err)
	}
	shard := prefixIndexMemberShard(oldKey)
	shardKey := prefixIndexShardKey(indexKey, shard)
	if err := client.Unlink(ctx, shardKey).Err(); err != nil {
		t.Fatalf("Unlink(shard) error = %v", err)
	}
	if _, err := storeA.DeletePrefixIndexKeys(ctx, indexKey, 1, acquireTestLockGuard(t, storeA, "index-delete-untrusted")); !stdErrors.Is(err, ErrPrefixIndexUntrusted) {
		t.Fatalf("DeletePrefixIndexKeys() error = %v, want ErrPrefixIndexUntrusted", err)
	}
	if exists := client.Exists(ctx, oldKey).Val(); exists != 1 {
		t.Fatalf("old business exists = %d, want 1 before fallback", exists)
	}

	newKey := findPrefixIndexShardKey("evict:new:", shard)
	if err := storeB.ApplyMutation(ctx, StoreMutation{
		WriteEntries: []Entry{{Key: newKey, Type: TypeString, Value: "new", TTL: time.Hour}},
		AddIndex:     []PrefixIndexMutation{{IndexKey: indexKey, TTL: time.Hour, Keys: []string{newKey}}},
	}); err != nil {
		t.Fatalf("ApplyMutation(new same shard) error = %v", err)
	}
	if exists := client.Exists(ctx, indexKey).Val(); exists != 0 {
		t.Fatalf("manifest exists = %d, want 0 after rebuilding evicted shard", exists)
	}
	if got := client.Get(ctx, newKey).Val(); got != "new" {
		t.Fatalf("new key = %q, want new", got)
	}
	deleted, err := storeB.deletePattern(ctx, RedisPrefixPattern("evict:"), 1)
	if err != nil || deleted != 2 {
		t.Fatalf("fallback DeletePattern() = %d,%v, want 2,nil", deleted, err)
	}
}

// TestRedisStorePrefixIndexEvictionDuringTraversal 验证删除会失败关闭，全量替换会降级 SCAN 并重建索引。
func TestRedisStorePrefixIndexEvictionDuringTraversal(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		ctx := context.Background()
		server := runStandaloneRedis(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		evictClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = evictClient.Close() })
		store := NewRedisStore(client)
		indexKey := "tcm:pidx:evict-during-delete"
		businessKey := "evict-during-delete:1"
		shardKey := prefixIndexShardKey(indexKey, prefixIndexMemberShard(businessKey))
		if err := client.Set(ctx, businessKey, "value", time.Hour).Err(); err != nil {
			t.Fatalf("Set(business) error = %v", err)
		}
		if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, businessKey); err != nil {
			t.Fatalf("AddPrefixIndexKeys() error = %v", err)
		}
		if err := commitTestPrefixIndex(t, store, indexKey, time.Hour); err != nil {
			t.Fatalf("commitPrefixIndex() error = %v", err)
		}
		hook := &redisBeforeHook{
			match: func(cmd redis.Cmder) bool {
				return strings.EqualFold(cmd.Name(), "zscan") && pipelineContainsKey([]redis.Cmder{cmd}, shardKey)
			},
			before: func(context.Context, redis.Cmder) error {
				return evictClient.Unlink(ctx, shardKey).Err()
			},
		}
		client.AddHook(hook)
		deleted, err := store.DeletePrefixIndexKeys(ctx, indexKey, 1, acquireTestLockGuard(t, store, "index-delete-evicted"))
		if !stdErrors.Is(err, ErrPrefixIndexUntrusted) {
			t.Fatalf("DeletePrefixIndexKeys() = %d,%v, want ErrPrefixIndexUntrusted", deleted, err)
		}
		if !hook.fired.Load() {
			t.Fatal("eviction hook did not fire")
		}
		if exists := client.Exists(ctx, businessKey).Val(); exists != 1 {
			t.Fatalf("business key exists = %d, want 1 for fallback deletion", exists)
		}
	})

	t.Run("replace", func(t *testing.T) {
		ctx := context.Background()
		server := runStandaloneRedis(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		evictClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = evictClient.Close() })
		store := NewRedisStore(client)
		indexKey := "tcm:pidx:evict-during-replace"
		readyKey := "tcm:pidx:ready:evict-during-replace"
		oldKey := "evict-during-replace:old"
		oldShard := prefixIndexMemberShard(oldKey)
		newKey := findPrefixIndexShardKey("evict-during-replace:new:", (oldShard+1)%prefixIndexShardCount)
		shardKey := prefixIndexShardKey(indexKey, oldShard)
		if err := client.Set(ctx, oldKey, "old", time.Hour).Err(); err != nil {
			t.Fatalf("Set(old) error = %v", err)
		}
		if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, oldKey); err != nil {
			t.Fatalf("AddPrefixIndexKeys(old) error = %v", err)
		}
		if err := commitTestPrefixIndex(t, store, indexKey, time.Hour); err != nil {
			t.Fatalf("commitPrefixIndex() error = %v", err)
		}
		if err := client.Set(ctx, readyKey, "ready", time.Hour).Err(); err != nil {
			t.Fatalf("Set(ready) error = %v", err)
		}
		hook := &redisBeforeHook{
			match: func(cmd redis.Cmder) bool {
				return strings.EqualFold(cmd.Name(), "zscan") && pipelineContainsKey([]redis.Cmder{cmd}, shardKey)
			},
			before: func(context.Context, redis.Cmder) error {
				return evictClient.Unlink(ctx, shardKey).Err()
			},
		}
		client.AddHook(hook)
		deleted, err := store.ReplacePrefix(ctx, PrefixReplaceMutation{
			Guards:         acquireTestLockGuard(t, store, "evict-during-replace:"),
			Prefix:         "evict-during-replace:",
			Entries:        []Entry{{Key: newKey, Type: TypeString, Value: "new", TTL: time.Hour}},
			DeletePatterns: []string{RedisPrefixPattern("evict-during-replace:")},
			IndexKey:       indexKey,
			ReadyKey:       readyKey,
			IndexTTL:       time.Hour,
			ScanCount:      1,
		})
		if err != nil || deleted != 1 {
			t.Fatalf("ReplacePrefix() = %d,%v, want 1,nil", deleted, err)
		}
		if !hook.fired.Load() {
			t.Fatal("eviction hook did not fire")
		}
		if exists := client.Exists(ctx, oldKey).Val(); exists != 0 {
			t.Fatalf("old key exists = %d, want 0 after SCAN fallback", exists)
		}
		if value := client.Get(ctx, newKey).Val(); value != "new" {
			t.Fatalf("new key = %q, want new", value)
		}
		if exists := client.Exists(ctx, readyKey).Val(); exists != 0 {
			t.Fatalf("ready exists = %d, want 0 before Manager marks rebuilt index", exists)
		}
		if _, err := store.trustedPrefixIndexShards(ctx, indexKey); err != nil {
			t.Fatalf("trustedPrefixIndexShards() error = %v", err)
		}
		if !testPrefixIndexContains(ctx, client, indexKey, newKey) {
			t.Fatalf("index missing current key %s after SCAN fallback", newKey)
		}
	})
}

// TestManagerPrefixIndexEvictionFallsBackScan 验证Manager遇到缺片会撤销ready并全前缀扫描，不漏业务key。
func TestManagerPrefixIndexEvictionFallsBackScan(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "evict-manager",
		Key:   "evict-manager:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{
				{Key: defaultPhysicalKey("evict-manager:1"), Type: TypeString, Value: "one", TTL: time.Hour},
				{Key: defaultPhysicalKey("evict-manager:2"), Type: TypeString, Value: "two", TTL: time.Hour},
			}, nil
		},
	}}, WithPrefixKeyIndexTTL(time.Hour))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("evict-manager:")); err != nil {
		t.Fatalf("RefreshByKey(prefix) error = %v", err)
	}
	target := manager.prefixTargetsM[defaultPhysicalKey("evict-manager:")]
	indexKey := manager.prefixIndexKey(target)
	shardKey := prefixIndexShardKey(indexKey, prefixIndexMemberShard(defaultPhysicalKey("evict-manager:1")))
	if err := client.Unlink(ctx, shardKey).Err(); err != nil {
		t.Fatalf("Unlink(shard) error = %v", err)
	}
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("evict-manager:")); err != nil {
		t.Fatalf("DeleteByPrefix() error = %v", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("evict-manager:1"), defaultPhysicalKey("evict-manager:2"), manager.prefixIndexReadyKey(target)).Val(); exists != 0 {
		t.Fatalf("managed keys remain = %d, want 0", exists)
	}
}

// TestManagerFullRefreshRepairsMissingPrefixIndexShard 验证权威 SCAN 可恢复只剩旧成员的缺失分片并重新建立 ready。
func TestManagerFullRefreshRepairsMissingPrefixIndexShard(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	oldKey := defaultPhysicalKey("repair-index:old")
	oldShard := prefixIndexMemberShard(oldKey)
	newKey := findPrefixIndexShardKey(defaultPhysicalKey("repair-index:new:"), (oldShard+1)%prefixIndexShardCount)
	var useNew atomic.Bool
	manager, err := newTestManager(store, []Target{{
		Index: "repair-index",
		Key:   "repair-index:",
		Type:  TypeString,
		Loader: func(context.Context, LoadParams) ([]Entry, error) {
			if useNew.Load() {
				return []Entry{{Key: newKey, Type: TypeString, Value: "new", TTL: time.Hour}}, nil
			}
			return []Entry{{Key: oldKey, Type: TypeString, Value: "old", TTL: time.Hour}}, nil
		},
	}}, WithPrefixKeyIndexTTL(time.Hour))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	prefix := defaultPhysicalKey("repair-index:")
	if err := manager.RefreshByKey(ctx, prefix); err != nil {
		t.Fatalf("RefreshByKey(first) error = %v", err)
	}
	target := manager.prefixTargetsM[prefix]
	indexKey := manager.prefixIndexKey(target)
	readyKey := manager.prefixIndexReadyKey(target)
	oldShardKey := prefixIndexShardKey(indexKey, oldShard)
	if err := client.Unlink(ctx, oldShardKey).Err(); err != nil {
		t.Fatalf("Unlink(old shard) error = %v", err)
	}

	useNew.Store(true)
	if err := manager.RefreshByKey(ctx, prefix); err != nil {
		t.Fatalf("RefreshByKey(repair) error = %v", err)
	}
	if exists := client.Exists(ctx, oldKey).Val(); exists != 0 {
		t.Fatalf("old business key exists = %d, want 0", exists)
	}
	if value := client.Get(ctx, newKey).Val(); value != "new" {
		t.Fatalf("new business value = %q, want new", value)
	}
	if exists := client.Exists(ctx, readyKey).Val(); exists != 1 {
		t.Fatalf("ready exists = %d, want 1 after repair", exists)
	}
	if err := client.ZScore(ctx, oldShardKey, prefixIndexSentinel).Err(); err != nil {
		t.Fatalf("old shard sentinel was not restored: %v", err)
	}
	shards, err := store.trustedPrefixIndexShards(ctx, indexKey)
	if err != nil {
		t.Fatalf("trustedPrefixIndexShards() error = %v", err)
	}
	foundOldShard := false
	for _, shard := range shards {
		foundOldShard = foundOldShard || shard == oldShard
	}
	if !foundOldShard {
		t.Fatalf("active shards = %v, want retained old shard %d", shards, oldShard)
	}
	if !testPrefixIndexContains(ctx, client, indexKey, newKey) {
		t.Fatalf("index missing current key %s after repair", newKey)
	}
}

// TestManagerFullRefreshRecoversActiveBitmapLoss 验证全量写入中途丢失 active 时会降级 SCAN 并重建完整索引。
func TestManagerFullRefreshRecoversActiveBitmapLoss(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	evictClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = evictClient.Close() })
	store := NewRedisStore(client)
	oldKey := defaultPhysicalKey("active-loss:old")
	oldShard := prefixIndexMemberShard(oldKey)
	newKey := findPrefixIndexShardKey(defaultPhysicalKey("active-loss:new:"), (oldShard+1)%prefixIndexShardCount)
	var useNew atomic.Bool
	manager, err := newTestManager(store, []Target{{
		Index: "active-loss",
		Key:   "active-loss:",
		Type:  TypeString,
		Loader: func(context.Context, LoadParams) ([]Entry, error) {
			if useNew.Load() {
				return []Entry{{Key: newKey, Type: TypeString, Value: "new", TTL: time.Hour}}, nil
			}
			return []Entry{{Key: oldKey, Type: TypeString, Value: "old", TTL: time.Hour}}, nil
		},
	}}, WithPrefixKeyIndexTTL(time.Hour))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	prefix := defaultPhysicalKey("active-loss:")
	if err := manager.RefreshByKey(ctx, prefix); err != nil {
		t.Fatalf("RefreshByKey(first) error = %v", err)
	}
	target := manager.prefixTargetsM[prefix]
	indexKey := manager.prefixIndexKey(target)
	readyKey := manager.prefixIndexReadyKey(target)
	activeKey := prefixIndexActiveKey(indexKey)
	hook := &redisBeforeHook{
		match: func(cmd redis.Cmder) bool {
			args := cmd.Args()
			return strings.EqualFold(cmd.Name(), "eval") && len(args) > 1 && fmt.Sprint(args[1]) == guardedMutationLua && pipelineContainsKey([]redis.Cmder{cmd}, newKey)
		},
		before: func(context.Context, redis.Cmder) error {
			return evictClient.Unlink(ctx, activeKey).Err()
		},
	}
	client.AddHook(hook)

	useNew.Store(true)
	if err := manager.RefreshByKey(ctx, prefix); err != nil {
		t.Fatalf("RefreshByKey(active loss) error = %v", err)
	}
	if !hook.fired.Load() {
		t.Fatal("active eviction hook did not fire")
	}
	if exists := client.Exists(ctx, oldKey).Val(); exists != 0 {
		t.Fatalf("old business key exists = %d, want 0", exists)
	}
	if value := client.Get(ctx, newKey).Val(); value != "new" {
		t.Fatalf("new business value = %q, want new", value)
	}
	if exists := client.Exists(ctx, readyKey).Val(); exists != 1 {
		t.Fatalf("ready exists = %d, want 1 after active rebuild", exists)
	}
	if _, err := store.trustedPrefixIndexShards(ctx, indexKey); err != nil {
		t.Fatalf("trustedPrefixIndexShards() error = %v", err)
	}
	if !testPrefixIndexContains(ctx, client, indexKey, newKey) {
		t.Fatalf("index missing current key %s after active rebuild", newKey)
	}
}

// TestRedisStoreActiveBitmapEvictionInvalidatesManifest 验证active key淘汰后增量写不会掩盖旧分片。
func TestRedisStoreActiveBitmapEvictionInvalidatesManifest(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	storeA := NewRedisStore(client)
	storeB := NewRedisStore(client)
	indexKey := "tcm:pidx:active-evict"
	oldKey := "active-evict:old"
	if err := storeA.ApplyMutation(ctx, StoreMutation{
		WriteEntries: []Entry{{Key: oldKey, Type: TypeString, Value: "old"}},
		AddIndex:     []PrefixIndexMutation{{IndexKey: indexKey, TTL: time.Hour, Keys: []string{oldKey}}},
	}); err != nil {
		t.Fatalf("ApplyMutation(old) error = %v", err)
	}
	if err := commitTestPrefixIndex(t, storeA, indexKey, time.Hour); err != nil {
		t.Fatalf("commitPrefixIndex() error = %v", err)
	}
	if err := client.Unlink(ctx, prefixIndexActiveKey(indexKey)).Err(); err != nil {
		t.Fatalf("Unlink(active) error = %v", err)
	}
	newKey := "active-evict:new"
	if err := storeB.ApplyMutation(ctx, StoreMutation{
		WriteEntries: []Entry{{Key: newKey, Type: TypeString, Value: "new"}},
		AddIndex:     []PrefixIndexMutation{{IndexKey: indexKey, TTL: time.Hour, Keys: []string{newKey}}},
	}); err != nil {
		t.Fatalf("ApplyMutation(new) error = %v", err)
	}
	if exists := client.Exists(ctx, indexKey).Val(); exists != 0 {
		t.Fatalf("manifest exists = %d, want 0 after active eviction", exists)
	}
	deleted, err := storeB.deletePattern(ctx, RedisPrefixPattern("active-evict:"), 1)
	if err != nil || deleted != 2 {
		t.Fatalf("fallback DeletePattern() = %d,%v, want 2,nil", deleted, err)
	}
}

// TestRedisStorePrefixIndexSupersetKeepsSentinel 验证在线删除保留成员与分片完整性证据。
func TestRedisStorePrefixIndexSupersetKeepsSentinel(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:sentinel"
	key := "sentinel:1"
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, key); err != nil {
		t.Fatalf("AddPrefixIndexKeys() error = %v", err)
	}
	if err := commitTestPrefixIndex(t, store, indexKey, time.Hour); err != nil {
		t.Fatalf("commitPrefixIndex() error = %v", err)
	}
	shardKey := prefixIndexShardKey(indexKey, prefixIndexMemberShard(key))
	if scoreErr := client.ZScore(ctx, shardKey, prefixIndexSentinel).Err(); scoreErr != nil {
		t.Fatalf("sentinel missing after last member removal: %v", scoreErr)
	}
	deleted, err := store.DeletePrefixIndexKeys(ctx, indexKey, 1, acquireTestLockGuard(t, store, "index-delete-empty"))
	if err != nil || deleted != 0 {
		t.Fatalf("DeletePrefixIndexKeys() = %d,%v, want 0,nil", deleted, err)
	}
}

// TestRedisStorePrefixIndexRebuildPrunesExpiredMembers 验证空分片恢复也会裁剪过期安全超集成员。
func TestRedisStorePrefixIndexRebuildPrunesExpiredMembers(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:prune-empty"
	oldKey := "prune-empty:old"
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Second, oldKey); err != nil {
		t.Fatalf("addPrefixIndexKeys() error = %v", err)
	}
	shardKey := prefixIndexShardKey(indexKey, prefixIndexMemberShard(oldKey))
	if err := client.ZAdd(ctx, shardKey, redis.Z{Score: 1, Member: oldKey}).Err(); err != nil {
		t.Fatalf("ZAdd(old score) error = %v", err)
	}
	guards := acquireTestLockGuard(t, store, "prune-empty")
	if err := store.rebuildPrefixIndexAfterCleanup(ctx, indexKey, time.Second, nil, guards); err != nil {
		t.Fatalf("rebuildPrefixIndexAfterCleanup() error = %v", err)
	}
	if err := client.ZScore(ctx, shardKey, oldKey).Err(); err != redis.Nil {
		t.Fatalf("expired member error = %v, want redis.Nil", err)
	}
	if err := client.ZScore(ctx, shardKey, prefixIndexSentinel).Err(); err != nil {
		t.Fatalf("sentinel error = %v", err)
	}
	if _, err := store.trustedPrefixIndexShards(ctx, indexKey); err != nil {
		t.Fatalf("trustedPrefixIndexShards() error = %v", err)
	}
}

// TestRedisStoreStaleOwnerCannotRepairPrefixIndexTrust 验证失锁 owner 不能初始化位图或重新提交 manifest。
func TestRedisStoreStaleOwnerCannotRepairPrefixIndexTrust(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:stale-owner"
	lockKey := tablecacheMetaKey("test:lock", "stale-owner")
	if err := client.Set(ctx, indexKey, prefixIndexManifestValue, time.Hour).Err(); err != nil {
		t.Fatalf("Set(manifest) error = %v", err)
	}
	if err := client.Set(ctx, lockKey, "new-owner", time.Hour).Err(); err != nil {
		t.Fatalf("Set(lock) error = %v", err)
	}
	err := store.rebuildPrefixIndexAfterCleanup(ctx, indexKey, time.Hour, []string{"stale-owner:old"}, []LockGuard{{Key: lockKey, Owner: "old-owner"}})
	if !stdErrors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("rebuildPrefixIndexAfterCleanup() error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, prefixIndexActiveKey(indexKey)).Val(); exists != 0 {
		t.Fatalf("active bitmap exists = %d, want 0", exists)
	}
	if version := client.Get(ctx, indexKey).Val(); version != prefixIndexManifestValue {
		t.Fatalf("manifest = %q, want preserved %q", version, prefixIndexManifestValue)
	}
}

// TestRedisStorePrefixIndexEmptyBitmapRevokesManifest 验证空字符串位图不会被增量写修复成可信缺失索引。
func TestRedisStorePrefixIndexEmptyBitmapRevokesManifest(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:empty-bitmap"
	if err := client.Set(ctx, indexKey, prefixIndexManifestValue, time.Hour).Err(); err != nil {
		t.Fatalf("Set(manifest) error = %v", err)
	}
	if err := client.Set(ctx, prefixIndexActiveKey(indexKey), "", time.Hour).Err(); err != nil {
		t.Fatalf("Set(empty active) error = %v", err)
	}
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, "empty-bitmap:new"); err != nil {
		t.Fatalf("addPrefixIndexKeys() error = %v", err)
	}
	if exists := client.Exists(ctx, indexKey).Val(); exists != 0 {
		t.Fatalf("manifest exists = %d, want 0", exists)
	}
	if _, err := store.trustedPrefixIndexShards(ctx, indexKey); !stdErrors.Is(err, ErrPrefixIndexUntrusted) {
		t.Fatalf("trustedPrefixIndexShards() error = %v, want ErrPrefixIndexUntrusted", err)
	}
}

// TestPrefixIndexBatchRequiresSentinel 验证同一分片首批之后必须检查前一批写入的哨兵。
func TestPrefixIndexBatchRequiresSentinel(t *testing.T) {
	if prefixIndexBatchRequiresSentinel(nil, 7, 0) {
		t.Fatal("new shard first batch requires sentinel")
	}
	if !prefixIndexBatchRequiresSentinel(nil, 7, 1) {
		t.Fatal("new shard second batch does not require sentinel")
	}
	if !prefixIndexBatchRequiresSentinel(map[int]bool{7: true}, 7, 0) {
		t.Fatal("existing shard first batch does not require sentinel")
	}
}

// TestRedisStoreHashRejectsNonStringMapKeys 验证 Hash 输入不会把不同类型 key 静默格式化成同一字段。
func TestRedisStoreHashRejectsNonStringMapKeys(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	key := "hash:key-type"
	err := store.write(ctx, Entry{Key: key, Type: TypeHash, Value: map[any]any{1: "number", "1": "string"}})
	if err == nil {
		t.Fatal("write(non-string map key) error = nil")
	}
	if exists := client.Exists(ctx, key).Val(); exists != 0 {
		t.Fatalf("hash exists = %d, want 0", exists)
	}
	type fieldName string
	if err := store.write(ctx, Entry{Key: key, Type: TypeHash, Value: map[fieldName]string{"name": "alpha"}}); err != nil {
		t.Fatalf("write(string alias map key) error = %v", err)
	}
	if value := client.HGet(ctx, key, "name").Val(); value != "alpha" {
		t.Fatalf("hash name = %q, want alpha", value)
	}
}

// TestRedisStoreGuardKeyCannotBeMutated 验证公开 StoreMutation 不能覆盖或删除自身锁 key。
func TestRedisStoreGuardKeyCannotBeMutated(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	guard := acquireTestLockGuard(t, store, "self-mutation")[0]
	for _, mutation := range []StoreMutation{
		{Guards: []LockGuard{guard}, WriteEntries: []Entry{{Key: guard.Key, Type: TypeString, Value: "bad"}}},
		{Guards: []LockGuard{guard}, DeleteKeys: []string{guard.Key}},
	} {
		if err := store.ApplyMutation(ctx, mutation); err == nil {
			t.Fatal("ApplyMutation(guard key) error = nil")
		}
		if owner := client.Get(ctx, guard.Key).Val(); owner != guard.Owner {
			t.Fatalf("guard owner = %q, want %q", owner, guard.Owner)
		}
	}
}

// TestRedisStoreStaleMutationCannotActivatePrefixIndex 验证带失效 guard 的索引前置登记不会改写位图或 manifest。
func TestRedisStoreStaleMutationCannotActivatePrefixIndex(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:stale-mutation"
	lockKey := tablecacheMetaKey("test:lock", "stale-mutation")
	if err := client.Set(ctx, lockKey, "new-owner", time.Hour).Err(); err != nil {
		t.Fatalf("Set(lock) error = %v", err)
	}
	if err := client.Set(ctx, indexKey, prefixIndexManifestValue, time.Hour).Err(); err != nil {
		t.Fatalf("Set(manifest) error = %v", err)
	}
	err := store.ApplyMutation(ctx, StoreMutation{
		Guards: []LockGuard{{Key: lockKey, Owner: "old-owner"}},
		AddIndex: []PrefixIndexMutation{{
			IndexKey: indexKey,
			TTL:      time.Hour,
			Keys:     []string{"stale-mutation:1"},
		}},
	})
	if !stdErrors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("ApplyMutation() error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, prefixIndexActiveKey(indexKey)).Val(); exists != 0 {
		t.Fatalf("active bitmap exists = %d, want 0", exists)
	}
	if version := client.Get(ctx, indexKey).Val(); version != prefixIndexManifestValue {
		t.Fatalf("manifest = %q, want preserved %q", version, prefixIndexManifestValue)
	}
}

// TestRedisStoreRejectsUnsafePrefixArguments 验证短TTL和缺失删除pattern会在首个写命令前失败。
func TestRedisStoreRejectsUnsafePrefixArguments(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	if err := store.addPrefixIndexKeys(ctx, "short:index", time.Nanosecond, "short:1"); err == nil {
		t.Fatalf("AddPrefixIndexKeys(1ns) error = nil")
	}
	if _, err := store.ReplacePrefix(ctx, PrefixReplaceMutation{
		Guards:  acquireTestLockGuard(t, store, "missing-pattern:"),
		Prefix:  "missing-pattern:",
		Entries: []Entry{{Key: "missing-pattern:new", Type: TypeString, Value: "new"}},
	}); err == nil {
		t.Fatalf("ReplacePrefix(missing patterns) error = nil")
	}
	if exists := client.Exists(ctx, "missing-pattern:new").Val(); exists != 0 {
		t.Fatalf("new key exists = %d, want 0", exists)
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if err := store.write(ctx, Entry{Key: "ttl-overflow", Type: TypeString, Value: "x", TTL: maxDuration, Jitter: time.Nanosecond}); err == nil {
		t.Fatalf("Write(TTL overflow) error = nil")
	}
}

// TestRedisStoreDeletePatternTopology 验证master/shard能力会遍历全部节点，未知Cluster wrapper显式失败。
func TestRedisStoreDeletePatternTopology(t *testing.T) {
	ctx := context.Background()
	t.Run("master wrapper", func(t *testing.T) {
		servers := []*miniredis.Miniredis{runStandaloneRedis(t), runStandaloneRedis(t)}
		clients := []*redis.Client{
			redis.NewClient(&redis.Options{Addr: servers[0].Addr()}),
			redis.NewClient(&redis.Options{Addr: servers[1].Addr()}),
		}
		for index, client := range clients {
			if err := client.Set(ctx, fmt.Sprintf("topology:master:%d", index), "x", 0).Err(); err != nil {
				t.Fatalf("Set(master %d) error = %v", index, err)
			}
		}
		wrapper := &testMasterScanner{UniversalClient: clients[0], masters: clients}
		deleted, err := NewRedisStore(wrapper).deletePattern(ctx, RedisPrefixPattern("topology:master:"), 1)
		if err != nil || deleted != 2 {
			t.Fatalf("DeletePattern(master wrapper) = %d,%v, want 2,nil", deleted, err)
		}
	})

	t.Run("ring", func(t *testing.T) {
		serverA := runStandaloneRedis(t)
		serverB := runStandaloneRedis(t)
		clientA := redis.NewClient(&redis.Options{Addr: serverA.Addr()})
		clientB := redis.NewClient(&redis.Options{Addr: serverB.Addr()})
		if err := clientA.Set(ctx, "topology:ring:a", "x", 0).Err(); err != nil {
			t.Fatalf("Set(ring a) error = %v", err)
		}
		if err := clientB.Set(ctx, "topology:ring:b", "x", 0).Err(); err != nil {
			t.Fatalf("Set(ring b) error = %v", err)
		}
		ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"a": serverA.Addr(), "b": serverB.Addr()}})
		t.Cleanup(func() { _ = ring.Close() })
		deleted, err := NewRedisStore(ring).deletePattern(ctx, RedisPrefixPattern("topology:ring:"), 1)
		if !stdErrors.Is(err, ErrRedisTopologyUnsupported) || deleted != 0 {
			t.Fatalf("DeletePattern(ring) = %d,%v, want 0/ErrRedisTopologyUnsupported", deleted, err)
		}
		if exists := clientA.Exists(ctx, "topology:ring:a").Val() + clientB.Exists(ctx, "topology:ring:b").Val(); exists != 2 {
			t.Fatalf("Ring拒绝全分片扫描后剩余key = %d, want 2", exists)
		}
	})

	t.Run("unknown cluster wrapper", func(t *testing.T) {
		server := runStandaloneRedis(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		wrapper := &testClusterInfoClient{UniversalClient: client}
		store := NewRedisStore(wrapper)
		for range 2 {
			if _, err := store.deletePattern(ctx, "*", 1); !stdErrors.Is(err, ErrRedisTopologyUnsupported) {
				t.Fatalf("deletePattern(unknown cluster wrapper) error = %v, want ErrRedisTopologyUnsupported", err)
			}
		}
		if calls := wrapper.clusterInfoCalls.Load(); calls != 1 {
			t.Fatalf("ClusterInfo calls = %d, want cached 1", calls)
		}
	})

	t.Run("unknown cluster info response", func(t *testing.T) {
		server := runStandaloneRedis(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		wrapper := &testClusterInfoClient{UniversalClient: client, clusterInfo: "unexpected response"}
		if _, err := NewRedisStore(wrapper).deletePattern(ctx, "*", 1); !stdErrors.Is(err, ErrRedisTopologyUnsupported) {
			t.Fatalf("deletePattern(unknown response) error = %v, want ErrRedisTopologyUnsupported", err)
		}
	})

	t.Run("cluster info errors fail closed", func(t *testing.T) {
		for name, probeErr := range map[string]error{
			"unknown command": stdErrors.New("ERR unknown command 'cluster'"),
			"acl denied":      stdErrors.New("NOPERM this user has no permissions to run the 'cluster|info' command"),
		} {
			t.Run(name, func(t *testing.T) {
				server := runStandaloneRedis(t)
				client := redis.NewClient(&redis.Options{Addr: server.Addr()})
				if err := client.Set(ctx, "topology:must-stay", "x", 0).Err(); err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				wrapper := &testClusterInfoClient{UniversalClient: client, clusterInfoErr: probeErr}
				store := NewRedisStore(wrapper)
				for range 2 {
					if _, err := store.deletePattern(ctx, "*", 1); err == nil {
						t.Fatal("deletePattern() error = nil")
					}
				}
				if calls := wrapper.clusterInfoCalls.Load(); calls != 2 {
					t.Fatalf("ClusterInfo calls = %d, want uncached 2", calls)
				}
				if exists := client.Exists(ctx, "topology:must-stay").Val(); exists != 1 {
					t.Fatalf("key exists = %d, want 1", exists)
				}
			})
		}
		if !isClusterDisabledError(stdErrors.New("ERR This instance has cluster support disabled")) {
			t.Fatal("standard standalone error was not recognized")
		}
	})
}

func findPrefixIndexShardKey(prefix string, shard int) string {
	for index := 0; ; index++ {
		key := fmt.Sprintf("%s%d", prefix, index)
		if prefixIndexMemberShard(key) == shard {
			return key
		}
	}
}

type redisFaultHook struct {
	processMatch            func(redis.Cmder) bool
	pipelineMatch           func([]redis.Cmder) bool
	partialPipelineCommands int
	fired                   atomic.Bool
}

// redisBeforeHook 在匹配命令执行前注入一次确定性并发动作。
type redisBeforeHook struct {
	match  func(redis.Cmder) bool
	before func(context.Context, redis.Cmder) error
	fired  atomic.Bool
}

type testMasterScanner struct {
	redis.UniversalClient
	masters []*redis.Client
}

func (s *testMasterScanner) ForEachMaster(ctx context.Context, visit func(context.Context, *redis.Client) error) error {
	for _, client := range s.masters {
		if err := visit(ctx, client); err != nil {
			return err
		}
	}
	return nil
}

type testClusterInfoClient struct {
	redis.UniversalClient
	clusterInfoCalls atomic.Int64 // clusterInfoCalls 统计拓扑探测次数，验证结果缓存
	clusterInfo      string       // clusterInfo 覆盖测试返回内容；空值表示标准Cluster响应
	clusterInfoErr   error        // clusterInfoErr 模拟拓扑探测失败
}

func (c *testClusterInfoClient) ClusterInfo(ctx context.Context) *redis.StringCmd {
	c.clusterInfoCalls.Add(1)
	cmd := redis.NewStringCmd(ctx)
	if c.clusterInfoErr != nil {
		cmd.SetErr(c.clusterInfoErr)
		return cmd
	}
	info := c.clusterInfo
	if info == "" {
		info = "cluster_state:ok"
	}
	cmd.SetVal(info)
	return cmd
}

func (h *redisFaultHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *redisFaultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.processMatch != nil && h.processMatch(cmd) && h.fired.CompareAndSwap(false, true) {
			return fmt.Errorf("injected redis command failure")
		}
		return next(ctx, cmd)
	}
}

func (h *redisFaultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.pipelineMatch != nil && h.pipelineMatch(cmds) && h.fired.CompareAndSwap(false, true) {
			if h.partialPipelineCommands > 0 {
				end := min(h.partialPipelineCommands, len(cmds))
				if err := next(ctx, cmds[:end]); err != nil {
					return err
				}
			}
			return fmt.Errorf("injected redis pipeline failure")
		}
		return next(ctx, cmds)
	}
}

func (h *redisBeforeHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *redisBeforeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.match != nil && h.match(cmd) && h.fired.CompareAndSwap(false, true) && h.before != nil {
			if err := h.before(ctx, cmd); err != nil {
				return err
			}
		}
		return next(ctx, cmd)
	}
}

func (h *redisBeforeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if h.match != nil && h.match(cmd) && h.fired.CompareAndSwap(false, true) && h.before != nil {
				if err := h.before(ctx, cmd); err != nil {
					return err
				}
			}
		}
		return next(ctx, cmds)
	}
}

func pipelineContainsCommand(cmds []redis.Cmder, name string) bool {
	for _, cmd := range cmds {
		if strings.EqualFold(cmd.Name(), name) {
			return true
		}
	}
	return false
}

func pipelineContainsKey(cmds []redis.Cmder, key string) bool {
	for _, cmd := range cmds {
		for _, argument := range cmd.Args()[1:] {
			if fmt.Sprint(argument) == key {
				return true
			}
		}
	}
	return false
}

// TestRedisStoreCollectionReadLimitIsAtomic 验证集合上限检查与全量读取由单个Lua原子完成。
func TestRedisStoreCollectionReadLimitIsAtomic(t *testing.T) {
	const limit = 3
	tests := []struct {
		name string
		typ  CacheType
		seed func(context.Context, *redis.Client, string, int) error
	}{
		{
			name: "hash",
			typ:  TypeHash,
			seed: func(ctx context.Context, client *redis.Client, key string, count int) error {
				values := make([]any, 0, count*2)
				for index := 0; index < count; index++ {
					values = append(values, fmt.Sprintf("field-%d", index), fmt.Sprintf("value-%d", index))
				}
				return client.HSet(ctx, key, values...).Err()
			},
		},
		{
			name: "list",
			typ:  TypeList,
			seed: func(ctx context.Context, client *redis.Client, key string, count int) error {
				values := make([]any, 0, count)
				for index := 0; index < count; index++ {
					values = append(values, fmt.Sprintf("value-%d", index))
				}
				return client.RPush(ctx, key, values...).Err()
			},
		},
		{
			name: "set",
			typ:  TypeSet,
			seed: func(ctx context.Context, client *redis.Client, key string, count int) error {
				values := make([]any, 0, count)
				for index := 0; index < count; index++ {
					values = append(values, fmt.Sprintf("value-%d", index))
				}
				return client.SAdd(ctx, key, values...).Err()
			},
		},
		{
			name: "zset",
			typ:  TypeZSet,
			seed: func(ctx context.Context, client *redis.Client, key string, count int) error {
				values := make([]redis.Z, 0, count)
				for index := 0; index < count; index++ {
					values = append(values, redis.Z{Member: fmt.Sprintf("value-%d", index), Score: float64(index)})
				}
				return client.ZAdd(ctx, key, values...).Err()
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			server := runStandaloneRedis(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			for _, item := range []struct {
				key   string
				count int
			}{
				{key: test.name + ":below", count: limit - 1},
				{key: test.name + ":limit", count: limit},
				{key: test.name + ":above", count: limit + 1},
			} {
				if err := test.seed(ctx, client, item.key, item.count); err != nil {
					t.Fatalf("seed(%s, %d) error = %v", item.key, item.count, err)
				}
			}

			// 受限读取只允许发送EVAL/EVALSHA；若退回cardinality+全读两条命令，hook会直接让测试失败。
			forbidden := map[string]struct{}{
				"hgetall": {}, "hlen": {}, "llen": {}, "lrange": {},
				"scard": {}, "smembers": {}, "zcard": {}, "zrange": {},
			}
			hook := &redisFaultHook{processMatch: func(cmd redis.Cmder) bool {
				_, exists := forbidden[strings.ToLower(cmd.Name())]
				return exists
			}}
			client.AddHook(hook)
			store := NewRedisStore(client, WithMaxCollectionReadCount(limit))

			for _, item := range []struct {
				key       string
				wantCount int
				wantErr   error
			}{
				{key: test.name + ":below", wantCount: limit - 1},
				{key: test.name + ":limit", wantCount: limit},
				{key: test.name + ":above", wantErr: ErrCollectionTooLarge},
				{key: test.name + ":missing", wantErr: ErrCacheMiss},
			} {
				value, err := store.Read(ctx, item.key, test.typ)
				if item.wantErr != nil {
					if !stdErrors.Is(err, item.wantErr) {
						t.Fatalf("Read(%s) error = %v, want %v", item.key, err, item.wantErr)
					}
					continue
				}
				if err != nil {
					t.Fatalf("Read(%s) error = %v", item.key, err)
				}
				var count int
				switch typed := value.(type) {
				case map[string]string:
					count = len(typed)
				case []string:
					count = len(typed)
				case []ZMember:
					count = len(typed)
				default:
					t.Fatalf("Read(%s) value type = %T", item.key, value)
				}
				if count != item.wantCount {
					t.Fatalf("Read(%s) count = %d, want %d", item.key, count, item.wantCount)
				}
			}
			if hook.fired.Load() {
				t.Fatal("受限集合读取发送了独立cardinality或全读命令")
			}
		})
	}
}
