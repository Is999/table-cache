package tablecache

import (
	"context"
	stdErrors "errors"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// delayedBusinessMutationStore 在业务写入 Lua 前提供确定性失锁窗口。
type delayedBusinessMutationStore struct {
	*RedisStore
	key     string
	started chan struct{}
	resume  chan struct{}
	once    atomic.Bool
}

// opaqueRedisClient 隐藏底层拓扑类型，验证前缀操作按未知拓扑 fail-closed。
type opaqueRedisClient struct{ redis.UniversalClient }

// ApplyMutation 只阻塞目标业务 Entry，不干扰刷新开始时的结果 marker 清理。
func (s *delayedBusinessMutationStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	for _, entry := range mutation.WriteEntries {
		if entry.Key == s.key && s.once.CompareAndSwap(false, true) {
			close(s.started)
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-s.resume:
			}
			break
		}
	}
	return s.RedisStore.ApplyMutation(ctx, mutation)
}

// TestManagerGuardRejectsOldOwnerWrite 验证 owner 复核与业务写之间不再存在 TOCTOU。
func TestManagerGuardRejectsOldOwnerWrite(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	key := "guard-normal"
	store := &delayedBusinessMutationStore{
		RedisStore: NewRedisStore(client),
		key:        defaultPhysicalKey(key),
		started:    make(chan struct{}),
		resume:     make(chan struct{}),
	}
	manager, err := newTestManager(store, []Target{{
		Index: "guard-normal", Key: key, Type: TypeString,
		Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Type: TypeString, Value: "old-owner"}}, nil
		},
	}}, WithLockTTL(time.Minute))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- manager.RefreshByKey(ctx, defaultPhysicalKey(key)) }()
	<-store.started
	physicalKey := defaultPhysicalKey(key)
	if err := client.Set(ctx, manager.lockKey(physicalKey), "new-owner", time.Minute).Err(); err != nil {
		t.Fatalf("replace lock owner error = %v", err)
	}
	close(store.resume)
	if err := <-result; !stdErrors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("RefreshByKey() error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, physicalKey).Val(); exists != 0 {
		t.Fatalf("stale owner wrote business key, exists = %d", exists)
	}
}

// TestRedisStoreGuardRejectsOldOwnerStateTransitions 覆盖普通写删、marker 与 fields-empty 的旧 owner 拒绝。
func TestRedisStoreGuardRejectsOldOwnerStateTransitions(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	key := "guard-state"
	lockKey := tablecacheMetaKey("rebuild:lock", key)
	old := LockGuard{Key: lockKey, Owner: "old-owner"}
	if ok, _, err := store.AcquireRefreshLock(ctx, lockKey, old.Owner, time.Minute); err != nil || !ok {
		t.Fatalf("AcquireRefreshLock(old) = %v,%v", ok, err)
	}
	if _, err := store.ReleaseLock(ctx, lockKey, old.Owner); err != nil {
		t.Fatalf("ReleaseLock(old) error = %v", err)
	}
	current := LockGuard{Key: lockKey, Owner: "new-owner"}
	if ok, _, err := store.AcquireRefreshLock(ctx, lockKey, current.Owner, time.Minute); err != nil || !ok {
		t.Fatalf("AcquireRefreshLock(new) = %v,%v", ok, err)
	}
	if err := store.ApplyMutation(ctx, StoreMutation{
		Guards: []LockGuard{current}, WriteEntries: []Entry{{Key: key, Type: TypeString, Value: "new"}},
	}); err != nil {
		t.Fatalf("ApplyMutation(new) error = %v", err)
	}
	for _, mutation := range []StoreMutation{
		{Guards: []LockGuard{old}, WriteEntries: []Entry{{Key: key, Type: TypeString, Value: "old"}}},
		{Guards: []LockGuard{old}, DeleteKeys: []string{key}},
	} {
		if err := store.ApplyMutation(ctx, mutation); !stdErrors.Is(err, ErrRefreshLockLost) {
			t.Fatalf("ApplyMutation(old) error = %v, want ErrRefreshLockLost", err)
		}
	}
	if value := client.Get(ctx, key).Val(); value != "new" {
		t.Fatalf("business value = %q, want new", value)
	}
	collectionTests := []struct {
		name    string
		typ     CacheType
		current any
		stale   any
		merge   bool
	}{
		{name: "hash overwrite", typ: TypeHash, current: map[string]any{"a": "new"}, stale: map[string]any{"a": "old"}},
		{name: "hash incremental", typ: TypeHash, current: map[string]any{"a": "new"}, stale: map[string]any{"b": "old"}, merge: true},
		{name: "list overwrite", typ: TypeList, current: []string{"new"}, stale: []string{"old"}},
		{name: "set overwrite", typ: TypeSet, current: []string{"new"}, stale: []string{"old"}},
		{name: "set incremental", typ: TypeSet, current: []string{"new"}, stale: []string{"old"}, merge: true},
		{name: "zset overwrite", typ: TypeZSet, current: []ZMember{{Member: "new", Score: 1}}, stale: []ZMember{{Member: "old", Score: 2}}},
		{name: "zset incremental", typ: TypeZSet, current: []ZMember{{Member: "new", Score: 1}}, stale: []ZMember{{Member: "old", Score: 2}}, merge: true},
	}
	for _, test := range collectionTests {
		t.Run(test.name, func(t *testing.T) {
			collectionKey := "guard-" + test.name
			if err := store.write(ctx, Entry{Key: collectionKey, Type: test.typ, Value: test.current}); err != nil {
				t.Fatalf("Write(current) error = %v", err)
			}
			before, err := store.Read(ctx, collectionKey, test.typ)
			if err != nil {
				t.Fatalf("Read(before) error = %v", err)
			}
			overwrite := Bool(!test.merge)
			err = store.ApplyMutation(ctx, StoreMutation{
				Guards: []LockGuard{old}, WriteEntries: []Entry{{
					Key: collectionKey, Type: test.typ, Value: test.stale, Overwrite: overwrite,
				}},
			})
			if !stdErrors.Is(err, ErrRefreshLockLost) {
				t.Fatalf("ApplyMutation(old) error = %v, want ErrRefreshLockLost", err)
			}
			after, err := store.Read(ctx, collectionKey, test.typ)
			if err != nil {
				t.Fatalf("Read(after) error = %v", err)
			}
			if !reflect.DeepEqual(normalizeGuardTestValue(test.typ, before), normalizeGuardTestValue(test.typ, after)) {
				t.Fatalf("cache changed: before=%v after=%v", before, after)
			}
		})
	}
	marker := Entry{Key: tablecacheMetaKey("empty", key), Type: TypeString, Value: "empty", TTL: time.Minute}
	if err := store.ReplaceWithMarker(ctx, []LockGuard{old}, marker, key); !stdErrors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("ReplaceWithMarker(old) error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, marker.Key).Val(); exists != 0 {
		t.Fatalf("stale marker exists = %d, want 0", exists)
	}
	fieldsKey := "guard-fields"
	registryKey := tablecacheMetaKey("empty_fields", fieldsKey)
	if err := client.HSet(ctx, fieldsKey, "name", "new").Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceFieldsWithEmpty(ctx, []LockGuard{old}, fieldsKey, registryKey, "name", []string{"name"}, time.Minute); !stdErrors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("ReplaceFieldsWithEmpty(old) error = %v, want ErrRefreshLockLost", err)
	}
	if value := client.HGet(ctx, fieldsKey, "name").Val(); value != "new" {
		t.Fatalf("hash field = %q, want new", value)
	}
	if exists := client.Exists(ctx, registryKey).Val(); exists != 0 {
		t.Fatalf("stale fields registry exists = %d, want 0", exists)
	}
}

// normalizeGuardTestValue 收敛 Set 的无序返回，便于比较旧 owner 调用前后状态。
func normalizeGuardTestValue(typ CacheType, value any) any {
	if typ != TypeSet {
		return value
	}
	items := append([]string(nil), value.([]string)...)
	sort.Strings(items)
	return items
}

// TestRedisStoreGuardRejectsExpiredFullSnapshotNewKey 覆盖旧全量快照尚未写出的新 key 反例。
func TestRedisStoreGuardRejectsExpiredFullSnapshotNewKey(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	prefix := "guard-full:"
	lockKey := tablecacheMetaKey("rebuild:lock", prefix)
	old := LockGuard{Key: lockKey, Owner: "old-owner"}
	if ok, _, err := store.AcquireRefreshLock(ctx, lockKey, old.Owner, time.Minute); err != nil || !ok {
		t.Fatalf("AcquireRefreshLock(old) = %v,%v", ok, err)
	}
	_, _ = store.ReleaseLock(ctx, lockKey, old.Owner)
	current := LockGuard{Key: lockKey, Owner: "new-owner"}
	if ok, _, err := store.AcquireRefreshLock(ctx, lockKey, current.Owner, time.Minute); err != nil || !ok {
		t.Fatalf("AcquireRefreshLock(new) = %v,%v", ok, err)
	}
	base := PrefixReplaceMutation{
		Prefix: prefix, DeletePatterns: []string{RedisPrefixPattern(prefix)}, ScanCount: 10,
	}
	currentSnapshot := base
	currentSnapshot.Guards = []LockGuard{current}
	currentSnapshot.Entries = []Entry{{Key: prefix + "current", Type: TypeString, Value: "new"}}
	if _, err := store.ReplacePrefix(ctx, currentSnapshot); err != nil {
		t.Fatalf("ReplacePrefix(new) error = %v", err)
	}
	oldSnapshot := base
	oldSnapshot.Guards = []LockGuard{old}
	oldSnapshot.Entries = []Entry{{Key: prefix + "unseen", Type: TypeString, Value: "old"}}
	if _, err := store.ReplacePrefix(ctx, oldSnapshot); !stdErrors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("ReplacePrefix(old) error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, prefix+"unseen").Val(); exists != 0 {
		t.Fatalf("old unseen key exists = %d, want 0", exists)
	}
	if value := client.Get(ctx, prefix+"current").Val(); value != "new" {
		t.Fatalf("current value = %q, want new", value)
	}
	oldDelete := base
	oldDelete.Guards = []LockGuard{old}
	if _, err := store.ReplacePrefix(ctx, oldDelete); !stdErrors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("ReplacePrefix(old delete) error = %v, want ErrRefreshLockLost", err)
	}
	if value := client.Get(ctx, prefix+"current").Val(); value != "new" {
		t.Fatalf("old snapshot deleted current value, got %q", value)
	}
}

// TestRingUntaggedPrefixBoundary 验证 Ring 故障重映射风险在构造期整体 fail-fast。
func TestRingUntaggedPrefixBoundary(t *testing.T) {
	server := runStandaloneRedis(t)
	ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"one": server.Addr()}})
	t.Cleanup(func() { _ = ring.Close() })
	store := NewRedisStore(ring)
	if err := store.ValidateTablecacheStore(); !stdErrors.Is(err, ErrRedisTopologyUnsupported) {
		t.Fatalf("ValidateTablecacheStore() error = %v, want ErrRedisTopologyUnsupported", err)
	}
	if _, err := newTestManager(store, nil); !stdErrors.Is(err, ErrInvalidConfig) {
		t.Fatalf("newTestManager() error = %v, want ErrInvalidConfig", err)
	}
}

// TestClusterClientIsNotMisclassifiedAsRing 验证 ClusterClient 同时实现 ForEachShard 时仍按 Cluster 拓扑处理。
func TestClusterClientIsNotMisclassifiedAsRing(t *testing.T) {
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(client)
	if store.isRingTopology() {
		t.Fatal("isRingTopology(ClusterClient) = true")
	}
	if !store.AllowsPrefixMutation("tc:{cluster}:users:") {
		t.Fatal("AllowsPrefixMutation(tagged ClusterClient) = false, want true")
	}
	if store.AllowsPrefixMutation("tc:cluster:users:") {
		t.Fatal("AllowsPrefixMutation(untagged ClusterClient) = true, want false")
	}
}

// TestPrefixCommitScopeCannotCollideWithMember 验证业务后缀不能与内部 commit 锁域重名。
func TestPrefixCommitScopeCannotCollideWithMember(t *testing.T) {
	ctx := context.Background()
	for _, targetKey := range []string{"scope-collision:", "scope:{tenant}:"} {
		t.Run(targetKey, func(t *testing.T) {
			server := runStandaloneRedis(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			manager, err := newTestManager(NewRedisStore(client), []Target{{
				Index: "scope-collision", Key: targetKey, Type: TypeString,
				Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
					return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
				},
			}}, WithWait(time.Millisecond, 5))
			if err != nil {
				t.Fatalf("newTestManager() error = %v", err)
			}
			target := manager.prefixTargets[0]
			member := target.Key + "|commit"
			if manager.lockKey(member) == manager.lockKey(manager.prefixCommitLockName(target)) {
				t.Fatal("member lock collides with prefix commit lock")
			}
			if err := manager.RefreshByKey(ctx, member); err != nil {
				t.Fatalf("RefreshByKey(collision suffix) error = %v", err)
			}
			if value := client.Get(ctx, member).Val(); value != "ok" {
				t.Fatalf("member value = %q, want ok", value)
			}
		})
	}
}

// TestFieldsRebuildResultKeepsBusinessSlot 验证 fields 完成标记始终沿用原业务 key 的 hash tag。
func TestFieldsRebuildResultKeepsBusinessSlot(t *testing.T) {
	manager := &Manager{}
	for _, key := range []string{"tc:fields:1", "tc:fields:{tenant}:1"} {
		fields := []string{"name", "age"}
		resultKey := manager.fieldsRebuildResultKey(key, fields)
		if err := validateSameRedisSlot([]string{manager.lockKey(key), resultKey}); err != nil {
			t.Fatalf("key=%s result=%s error=%v", key, resultKey, err)
		}
		collisionKey := key + ":fields:" + fieldsID(fields)
		if resultKey == manager.rebuildResultKey(collisionKey) {
			t.Fatalf("fields result collides with full result: %s", resultKey)
		}
	}
}

// TestRefreshReadyNameHasUnambiguousScope 验证业务 key 不能伪装成另一 key 的 fields flight 名。
func TestRefreshReadyNameHasUnambiguousScope(t *testing.T) {
	manager := &Manager{}
	fields := []string{"name"}
	key := "tc:flight:1"
	oldCollision := key + "|fields=" + fieldsID(fields)
	if manager.refreshReadyName(key, fields) == manager.refreshReadyName(oldCollision, nil) {
		t.Fatal("fields flight key collides with full-key flight")
	}
}

// TestValidateManagerKeyPrefixRejectsReservedRootPrefixes 验证保留根的父前缀和子前缀都不能作为业务命名空间。
func TestValidateManagerKeyPrefixRejectsReservedRootPrefixes(t *testing.T) {
	for _, prefix := range []string{"t", "tc", "tcm", "tcm:"} {
		if err := validateManagerKeyPrefix(prefix); !stdErrors.Is(err, ErrInvalidKeyPrefix) {
			t.Fatalf("validateManagerKeyPrefix(%q) error = %v, want ErrInvalidKeyPrefix", prefix, err)
		}
	}
	for _, prefix := range []string{"tc:", "app:"} {
		if err := validateManagerKeyPrefix(prefix); err != nil {
			t.Fatalf("validateManagerKeyPrefix(%q) error = %v", prefix, err)
		}
	}
}

// TestPrefixSingleLoaderCannotWriteSiblingKey 验证单 key 回源不能借前缀范围批量写入其它成员。
func TestPrefixSingleLoaderCannotWriteSiblingKey(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "single-scope", Key: "single-scope:", Type: TypeString,
		Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Target.Key + "sibling", Type: TypeString, Value: "bad"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("single-scope:1")); !stdErrors.Is(err, ErrEntryKeyOutOfScope) {
		t.Fatalf("RefreshByKey() error = %v, want ErrEntryKeyOutOfScope", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("single-scope:1"), defaultPhysicalKey("single-scope:sibling")).Val(); exists != 0 {
		t.Fatalf("out-of-scope loader wrote %d keys", exists)
	}
}

// TestUnknownTopologyTaggedPrefixFailsBeforeLoader 验证未知 wrapper 即使带 tag 也不会先写后报拓扑错误。
func TestUnknownTopologyTaggedPrefixFailsBeforeLoader(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := &opaqueRedisClient{UniversalClient: redis.NewClient(&redis.Options{Addr: server.Addr()})}
	var loads atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "opaque", Key: "opaque:{tenant}:", Type: TypeString,
		Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
			loads.Add(1)
			return []Entry{{Key: params.Target.Key + "1", Type: TypeString, Value: "bad"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	prefix := defaultPhysicalKey("opaque:{tenant}:")
	if err := manager.RefreshByKey(ctx, prefix); !stdErrors.Is(err, ErrRedisTopologyUnsupported) {
		t.Fatalf("RefreshByKey(full) error = %v, want ErrRedisTopologyUnsupported", err)
	}
	if loads.Load() != 0 {
		t.Fatalf("loader calls = %d, want 0", loads.Load())
	}
	if err := manager.DeleteByPrefix(ctx, prefix); !stdErrors.Is(err, ErrRedisTopologyUnsupported) {
		t.Fatalf("DeleteByPrefix() error = %v, want ErrRedisTopologyUnsupported", err)
	}
}

// TestIncrementalEmptyCollectionIsRejected 验证空集合不会把增量写误转换为覆盖删除 marker。
func TestIncrementalEmptyCollectionIsRejected(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		typ     CacheType
		current any
		empty   any
	}{
		{name: "hash", typ: TypeHash, current: map[string]any{"name": "alice"}, empty: map[string]any{}},
		{name: "list", typ: TypeList, current: []string{"current"}, empty: []string{}},
		{name: "set", typ: TypeSet, current: []string{"current"}, empty: []string{}},
		{name: "zset", typ: TypeZSet, current: []ZMember{{Member: "current", Score: 1}}, empty: []ZMember{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := runStandaloneRedis(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			store := NewRedisStore(client)
			logicalKey := "incremental-empty-" + test.name
			physicalKey := defaultPhysicalKey(logicalKey)
			manager, err := newTestManager(store, []Target{{
				Index: logicalKey, Key: logicalKey, Type: test.typ,
				Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
					return []Entry{{Key: params.Key, Type: test.typ, Value: test.empty, Overwrite: Bool(false)}}, nil
				},
			}})
			if err != nil {
				t.Fatalf("newTestManager() error = %v", err)
			}
			if err := store.write(ctx, Entry{Key: physicalKey, Type: test.typ, Value: test.current}); err != nil {
				t.Fatalf("Write(current) error = %v", err)
			}
			before, err := store.Read(ctx, physicalKey, test.typ)
			if err != nil {
				t.Fatalf("Read(before) error = %v", err)
			}
			if err := manager.RefreshByKey(ctx, physicalKey); !stdErrors.Is(err, ErrInvalidConfig) {
				t.Fatalf("RefreshByKey() error = %v, want ErrInvalidConfig", err)
			}
			after, err := store.Read(ctx, physicalKey, test.typ)
			if err != nil {
				t.Fatalf("Read(after) error = %v", err)
			}
			if !reflect.DeepEqual(normalizeGuardTestValue(test.typ, before), normalizeGuardTestValue(test.typ, after)) {
				t.Fatalf("current collection changed: before=%v after=%v", before, after)
			}
			if exists := client.Exists(ctx, manager.emptyCollectionKey(physicalKey)).Val(); exists != 0 {
				t.Fatalf("empty collection marker exists = %d, want 0", exists)
			}
		})
	}
}

// TestEmptyCollectionValueTypeContract 验证非法空容器不会伪装成空集合，合法空 ZSet map 会生成 marker。
func TestEmptyCollectionValueTypeContract(t *testing.T) {
	ctx := context.Background()
	invalid := []struct {
		name    string
		typ     CacheType
		current any
		value   any
	}{
		{name: "hash_slice", typ: TypeHash, current: map[string]any{"name": "current"}, value: []string{}},
		{name: "list_map", typ: TypeList, current: []string{"current"}, value: map[string]string{}},
		{name: "set_map", typ: TypeSet, current: []string{"current"}, value: map[string]string{}},
		{name: "zset_slice", typ: TypeZSet, current: []ZMember{{Member: "current", Score: 1}}, value: []string{}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			server := runStandaloneRedis(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			store := NewRedisStore(client)
			logicalKey := "invalid-empty-" + test.name
			physicalKey := defaultPhysicalKey(logicalKey)
			manager, err := newTestManager(store, []Target{{
				Index: logicalKey, Key: logicalKey, Type: test.typ,
				Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
					return []Entry{{Key: params.Key, Type: test.typ, Value: test.value}}, nil
				},
			}})
			if err != nil {
				t.Fatalf("newTestManager() error = %v", err)
			}
			if err := store.write(ctx, Entry{Key: physicalKey, Type: test.typ, Value: test.current}); err != nil {
				t.Fatal(err)
			}
			before, err := store.Read(ctx, physicalKey, test.typ)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.RefreshByKey(ctx, physicalKey); err == nil {
				t.Fatal("RefreshByKey() error = nil, want invalid collection value")
			}
			after, err := store.Read(ctx, physicalKey, test.typ)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(normalizeGuardTestValue(test.typ, before), normalizeGuardTestValue(test.typ, after)) {
				t.Fatalf("current collection changed: before=%v after=%v", before, after)
			}
			if exists := client.Exists(ctx, manager.emptyCollectionKey(physicalKey)).Val(); exists != 0 {
				t.Fatalf("empty marker exists = %d, want 0", exists)
			}
		})
	}

	t.Run("valid map", func(t *testing.T) {
		server := runStandaloneRedis(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		store := NewRedisStore(client)
		logicalKey := "zset-valid-empty"
		physicalKey := defaultPhysicalKey(logicalKey)
		manager, err := newTestManager(store, []Target{{
			Index: logicalKey, Key: logicalKey, Type: TypeZSet,
			Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeZSet, Value: map[string]float64{}}}, nil
			},
		}})
		if err != nil {
			t.Fatalf("newTestManager() error = %v", err)
		}
		if err := store.write(ctx, Entry{Key: physicalKey, Type: TypeZSet, Value: []ZMember{{Member: "old", Score: 1}}}); err != nil {
			t.Fatal(err)
		}
		if err := manager.RefreshByKey(ctx, physicalKey); err != nil {
			t.Fatalf("RefreshByKey() error = %v", err)
		}
		if exists := client.Exists(ctx, physicalKey).Val(); exists != 0 {
			t.Fatalf("old ZSet exists = %d, want 0", exists)
		}
		if exists := client.Exists(ctx, manager.emptyCollectionKey(physicalKey)).Val(); exists != 1 {
			t.Fatalf("empty marker exists = %d, want 1", exists)
		}
	})
}
