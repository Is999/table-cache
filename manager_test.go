package tablecache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

// runStandaloneRedis 创建明确模拟 standalone 拓扑的 miniredis，避免测试放宽生产 fail-close 判定。
func runStandaloneRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	instance := miniredis.RunT(t)
	instance.Server().SetPreHook(func(peer *miniredisserver.Peer, command string, args ...string) bool {
		if !strings.EqualFold(command, "cluster") || len(args) != 1 || !strings.EqualFold(args[0], "info") {
			return false
		}
		peer.WriteError("ERR This instance has cluster support disabled")
		return true
	})
	return instance
}

// newTestManager 使用生产默认前缀并显式开放测试 SCAN，避免无关用例依赖生产 fail-close 默认值。
func newTestManager(store Store, targets []Target, opts ...Option) (*Manager, error) {
	testOptions := append([]Option{WithScanFallback(true)}, opts...)
	return NewManager(store, targets, testOptions...)
}

// rebuildResultKey 返回测试默认请求型完成标记。
func (m *Manager) rebuildResultKey(key string) string {
	return m.refreshResultKey(key, nil, refreshOptions{requested: true})
}

// fieldsRebuildResultKey 返回测试默认字段组合完成标记。
func (m *Manager) fieldsRebuildResultKey(key string, fields []string) string {
	return m.refreshResultKey(key, fields, refreshOptions{requested: true})
}

// defaultPhysicalKey 返回使用生产默认命名空间的实际 Redis key。
func defaultPhysicalKey(key string) string {
	return defaultKeyPrefix + key
}

// TestManagerItemsExposeTitle 验证管理列表及其 JSON 契约不会丢失目标展示名称。
func TestManagerItemsExposeTitle(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index:    "users",
		Title:    "用户缓存",
		Key:      "users:",
		KeyTitle: "users:{id}",
		Type:     TypeHash,
		Remark:   "用户资料",
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	items := manager.Items()
	if len(items) != 1 || items[0].Title != "用户缓存" ||
		items[0].Key != defaultPhysicalKey("users:") ||
		items[0].KeyTitle != defaultPhysicalKey("users:{id}") {
		t.Fatalf("Items() = %+v, want distinct registered key and display template", items)
	}
	encoded, err := json.Marshal(items[0])
	if err != nil {
		t.Fatalf("json.Marshal(Item) error = %v", err)
	}
	for _, field := range []string{
		`"title":"用户缓存"`,
		`"key":"tc:users:"`,
		`"keyTitle":"tc:users:{id}"`,
	} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("Item JSON = %s, want field %s", encoded, field)
		}
	}
}

// TestManagerRefreshByKey 验证指定 key 刷新、TTL 抖动和空值占位能力。
func TestManagerRefreshByKey(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "demo",
			Title:            "演示缓存",
			Key:              "demo:",
			KeyTitle:         "demo:{id}",
			Type:             TypeHash,
			TTL:              10 * time.Minute,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if len(params.KeyParts) == 0 || params.KeyParts[0] != "1" {
					return nil, nil
				}
				return []Entry{{
					Key:   params.Key,
					Type:  TypeHash,
					Value: map[string]any{"name": "alpha"},
				}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}

	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("demo:1")); err != nil {
		t.Fatalf("RefreshByKey(demo:1) error = %v", err)
	}
	if got := client.HGet(ctx, defaultPhysicalKey("demo:1"), "name").Val(); got != "alpha" {
		t.Fatalf("demo:1 name = %q, want alpha", got)
	}
	if ttl := client.TTL(ctx, defaultPhysicalKey("demo:1")).Val(); ttl <= 0 {
		t.Fatalf("demo:1 ttl = %v, want positive", ttl)
	}

	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("demo:404")); err != nil {
		t.Fatalf("RefreshByKey(demo:404) error = %v", err)
	}
	if got := client.Get(ctx, manager.emptyKey(defaultPhysicalKey("demo:404"))).Val(); got != DefaultEmptyMarker {
		t.Fatalf("demo:404 empty marker = %q, want %q", got, DefaultEmptyMarker)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("demo:404")).Val(); exists != 0 {
		t.Fatalf("demo:404 business key exists = %d, want 0", exists)
	}
}

// TestManagerRefreshAll 验证批量刷新只处理允许全量刷新的固定缓存目标。
func TestManagerRefreshAll(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	called := 0
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:      "fixed",
			Title:      "固定缓存",
			Key:        "fixed",
			Type:       TypeString,
			RefreshAll: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				called++
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
		{
			Index: "detail",
			Title: "明细缓存",
			Key:   "detail:",
			Type:  TypeHash,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				called++
				return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{"id": 1}}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshAll(ctx); err != nil {
		t.Fatalf("RefreshAll() error = %v", err)
	}
	if called != 1 {
		t.Fatalf("loader called = %d, want 1", called)
	}
	if got := client.Get(ctx, defaultPhysicalKey("fixed")).Val(); got != "ok" {
		t.Fatalf("fixed value = %q, want ok", got)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("detail:1")).Val(); exists != 0 {
		t.Fatalf("detail:1 exists = %d, want 0", exists)
	}
}

// TestManagerRefreshAllWithSummary 验证全量刷新可返回逐项结果、汇总信息和聚合错误。
func TestManagerRefreshAllWithSummary(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var fixedCalls atomic.Int64
	var failCalls atomic.Int64
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:      "fixed-ok",
			Title:      "固定成功缓存",
			Key:        "fixed-ok",
			Type:       TypeString,
			RefreshAll: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				fixedCalls.Add(1)
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
		{
			Index:      "fixed-fail",
			Title:      "固定失败缓存",
			Key:        "fixed-fail",
			Type:       TypeString,
			RefreshAll: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				failCalls.Add(1)
				return nil, errors.Errorf("mock refresh all fail")
			},
		},
		{
			Index: "detail-ignore",
			Title: "忽略前缀缓存",
			Key:   "detail-ignore:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				t.Fatal("RefreshAllWithSummary() 不应执行未开启 RefreshAll 的目标")
				return nil, nil
			},
		},
	}, WithMetrics(metrics), WithLogger(logger))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	results, summary, err := manager.RefreshAllWithSummary(ctx)
	if len(results) != 2 {
		t.Fatalf("results len = %d, want 2", len(results))
	}
	batchErr := &RefreshBatchError{}
	if !errors.As(err, &batchErr) {
		t.Fatalf("RefreshAllWithSummary() error = %v, want RefreshBatchError", err)
	}
	if summary.Total != 2 || summary.Success != 1 || summary.Failed != 1 {
		t.Fatalf("summary = %+v, want total=2 success=1 failed=1", summary)
	}
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != defaultPhysicalKey("fixed-fail") {
		t.Fatalf("summary failed keys = %#v, want fixed-fail", summary.FailedKeys)
	}
	if got := fixedCalls.Load(); got != 1 {
		t.Fatalf("fixedCalls = %d, want 1", got)
	}
	if got := failCalls.Load(); got != 1 {
		t.Fatalf("failCalls = %d, want 1", got)
	}
	if got := client.Get(ctx, defaultPhysicalKey("fixed-ok")).Val(); got != "ok" {
		t.Fatalf("fixed-ok value = %q, want ok", got)
	}
	if got := metrics.refreshBatchCount.Load(); got != 1 {
		t.Fatalf("refreshBatchCount = %d, want 1", got)
	}
	if got := metrics.refreshBatchSuccess.Load(); got != 1 {
		t.Fatalf("refreshBatchSuccess = %d, want 1", got)
	}
	if got := metrics.refreshBatchFailed.Load(); got != 1 {
		t.Fatalf("refreshBatchFailed = %d, want 1", got)
	}
	if !logger.contains("event=\"refresh_all_done\"") {
		t.Fatalf("refresh all done log not found, logs=%v", logger.messages())
	}
}

// TestManagerRejectsOutOfScopeEntryKeys 验证 Loader 不能越过已注册目标范围写入 Redis key。
func TestManagerRejectsOutOfScopeEntryKeys(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "scope",
			Title: "作用域缓存",
			Key:   "scope:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: defaultPhysicalKey("other:1"), Type: TypeString, Value: "bad"}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("scope:1"))
	if !errors.Is(err, ErrEntryKeyOutOfScope) {
		t.Fatalf("RefreshByKey(scope:1) error = %v, want ErrEntryKeyOutOfScope", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("other:1")).Val(); exists != 0 {
		t.Fatalf("other:1 exists = %d, want 0", exists)
	}
}

// TestManagerEmptyCollectionEntriesHit 验证真实空集合会被记录为命中，而不是反复 miss 回源。
func TestManagerEmptyCollectionEntriesHit(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cases := []struct {
		name  string
		typ   CacheType
		key   string
		value any
		dest  any
		check func(t *testing.T, dest any)
	}{
		{
			name:  "hash",
			typ:   TypeHash,
			key:   "empty-hash:1",
			value: map[string]any{},
			dest:  &map[string]string{},
			check: func(t *testing.T, dest any) {
				if got := *(dest.(*map[string]string)); len(got) != 0 {
					t.Fatalf("hash dest = %#v, want empty", got)
				}
			},
		},
		{
			name:  "list",
			typ:   TypeList,
			key:   "empty-list:1",
			value: []any{},
			dest:  &[]string{},
			check: func(t *testing.T, dest any) {
				if got := *(dest.(*[]string)); len(got) != 0 {
					t.Fatalf("list dest = %#v, want empty", got)
				}
			},
		},
		{
			name:  "set",
			typ:   TypeSet,
			key:   "empty-set:1",
			value: []string{},
			dest:  &[]string{},
			check: func(t *testing.T, dest any) {
				if got := *(dest.(*[]string)); len(got) != 0 {
					t.Fatalf("set dest = %#v, want empty", got)
				}
			},
		},
		{
			name:  "zset",
			typ:   TypeZSet,
			key:   "empty-zset:1",
			value: []ZMember{},
			dest:  &[]ZMember{},
			check: func(t *testing.T, dest any) {
				if got := *(dest.(*[]ZMember)); len(got) != 0 {
					t.Fatalf("zset dest = %#v, want empty", got)
				}
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			physicalKey := defaultPhysicalKey(tt.key)
			manager, err := newTestManager(NewRedisStore(client), []Target{
				{
					Index: tt.name,
					Title: tt.name,
					Key:   strings.TrimSuffix(tt.key, "1"),
					Type:  tt.typ,
					Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
						return []Entry{{Key: params.Key, Type: tt.typ, Value: tt.value}}, nil
					},
				},
			})
			if err != nil {
				t.Fatalf("newTestManager() error = %v", err)
			}
			if err := manager.RefreshByKey(ctx, physicalKey); err != nil {
				t.Fatalf("RefreshByKey(%s) error = %v", tt.key, err)
			}
			if exists := client.Exists(ctx, physicalKey).Val(); exists != 0 {
				t.Fatalf("%s business key exists = %d, want 0", tt.key, exists)
			}
			if exists := client.Exists(ctx, manager.emptyCollectionKey(physicalKey)).Val(); exists != 1 {
				t.Fatalf("%s empty collection marker exists = %d, want 1", tt.key, exists)
			}
			result, err := manager.GetState(ctx, tt.key, tt.dest)
			if err != nil {
				t.Fatalf("GetState(%s) error = %v", tt.key, err)
			}
			if result.State != LookupStateHit {
				t.Fatalf("GetState(%s) state = %s, want hit", tt.key, result.State)
			}
			tt.check(t, tt.dest)
		})
	}
}

// TestManagerEmptyCollectionMarkerClearedByNormalWrite 验证空集合元信息会在后续正常写入时清理。
func TestManagerEmptyCollectionMarkerClearedByNormalWrite(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var calls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "empty-clear",
			Title: "空集合清理",
			Key:   "empty-clear:",
			Type:  TypeList,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if calls.Add(1) == 1 {
					return []Entry{{Key: params.Key, Type: TypeList, Value: []string{}}}, nil
				}
				return []Entry{{Key: params.Key, Type: TypeList, Value: []string{"alpha"}}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("empty-clear:1")); err != nil {
		t.Fatalf("RefreshByKey(empty-clear:1) first error = %v", err)
	}
	if exists := client.Exists(ctx, manager.emptyCollectionKey(defaultPhysicalKey("empty-clear:1"))).Val(); exists != 1 {
		t.Fatalf("empty collection marker exists = %d, want 1", exists)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("empty-clear:1")); err != nil {
		t.Fatalf("RefreshByKey(empty-clear:1) second error = %v", err)
	}
	if exists := client.Exists(ctx, manager.emptyCollectionKey(defaultPhysicalKey("empty-clear:1"))).Val(); exists != 0 {
		t.Fatalf("empty collection marker exists after normal write = %d, want 0", exists)
	}
	if got := client.LRange(ctx, defaultPhysicalKey("empty-clear:1"), 0, -1).Val(); len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("empty-clear:1 list = %#v, want alpha", got)
	}
}

// TestManagerWaitRebuildTimeout 验证等待其它实例重建超时时返回明确错误。
func TestManagerWaitRebuildTimeout(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "timeout",
			Title: "超时缓存",
			Key:   "timeout",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				t.Fatal("等待已有锁时不应执行加载器")
				return nil, nil
			},
		},
	}, WithWait(time.Millisecond, 2))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, manager.lockKey(defaultPhysicalKey("timeout")), "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock error = %v", err)
	}
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("timeout"))
	if !errors.Is(err, ErrWaitRebuildTimeout) {
		t.Fatalf("RefreshByKey(timeout) error = %v, want ErrWaitRebuildTimeout", err)
	}
}

// TestManagerReleaseLockSafely 验证锁过期后旧 owner 不会误删新锁，也不会继续写入脏缓存。
func TestManagerReleaseLockSafely(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var manager *Manager
	var err error
	manager, err = newTestManager(NewRedisStore(client), []Target{
		{
			Index: "safe",
			Title: "安全锁缓存",
			Key:   "safe:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				// 模拟当前持锁实例加载耗时超过锁 TTL，随后其它实例重新持有同一把锁。
				server.FastForward(2 * time.Second)
				if err := client.Set(ctx, manager.lockKey(params.Key), "other-owner", time.Minute).Err(); err != nil {
					return nil, errors.Tag(err)
				}
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	}, WithLockTTL(time.Second))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("safe:1"))
	if !errors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("RefreshByKey(safe:1) error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("safe:1")).Val(); exists != 0 {
		t.Fatalf("safe:1 exists = %d, want 0", exists)
	}
	if got := client.Get(ctx, manager.lockKey(defaultPhysicalKey("safe:1"))).Val(); got != "other-owner" {
		t.Fatalf("lock owner = %q, want other-owner", got)
	}
}

// TestManagerRefreshStopsAfterLockLost 验证锁续期失败后会立即中止刷新并禁止后续写回。
func TestManagerRefreshStopsAfterLockLost(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &lockLostStore{Store: NewRedisStore(client)}
	logger := &recordingLogger{}
	manager, err := newTestManager(store, []Target{
		{
			Index: "lock-lost",
			Title: "失锁缓存",
			Key:   "lock-lost",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				time.Sleep(60 * time.Millisecond)
				return []Entry{{Key: params.Key, Type: TypeString, Value: "should-not-write"}}, nil
			},
		},
	}, WithLockTTL(30*time.Millisecond), WithLogger(logger))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("lock-lost"))
	if !errors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("RefreshByKey(lock-lost) error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("lock-lost")).Val(); exists != 0 {
		t.Fatalf("lock-lost exists = %d, want 0", exists)
	}
	if !logger.contains("event=\"lock_lost\"") {
		t.Fatalf("lock lost log not found, logs=%v", logger.messages())
	}
}

// TestManagerPrefixRefreshUsesLayeredLock 验证前缀全量刷新与单 key 刷新采用分层锁域。
func TestManagerPrefixRefreshUsesLayeredLock(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &recordingLockStore{Store: NewRedisStore(client)}
	manager, err := newTestManager(store, []Target{
		{
			Index: "prefix-lock",
			Title: "前缀锁缓存",
			Key:   "prefix-lock:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if params.Key == defaultPhysicalKey("prefix-lock:1") {
					return []Entry{{Key: params.Key, Type: TypeString, Value: "one"}}, nil
				}
				return nil, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-lock:1")); err != nil {
		t.Fatalf("RefreshByKey(prefix-lock:1) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-lock:2")); err != nil {
		t.Fatalf("RefreshByKey(prefix-lock:2) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-lock:")); err != nil {
		t.Fatalf("RefreshByKey(prefix-lock:) error = %v", err)
	}
	lockKeys := store.lockKeys()
	if len(lockKeys) != 9 {
		t.Fatalf("refresh lock keys len = %d, want 9", len(lockKeys))
	}
	if lockKeys[0] != manager.lockKey(defaultPhysicalKey("prefix-lock:1")) {
		t.Fatalf("lock key[0] = %q, want %q", lockKeys[0], manager.lockKey(defaultPhysicalKey("prefix-lock:1")))
	}
	commitKey := manager.lockKey(manager.prefixCommitLockName(manager.prefixTargets[0]))
	if lockKeys[1] != commitKey {
		t.Fatalf("lock key[1] = %q, want %q", lockKeys[1], commitKey)
	}
	if lockKeys[2] != commitKey || lockKeys[3] != manager.lockKey(defaultPhysicalKey("prefix-lock:2")) || lockKeys[4] != commitKey || lockKeys[5] != commitKey {
		t.Fatalf("single key lock sequence = %v", lockKeys[:6])
	}
	if lockKeys[6] != manager.lockKey(defaultPhysicalKey("prefix-lock:")) || lockKeys[7] != commitKey || lockKeys[8] != commitKey {
		t.Fatalf("full refresh lock sequence = %v", lockKeys[6:])
	}
}

// TestManagerPrefixSingleKeyWaitsFullRefresh 验证单 key 刷新在同前缀全量刷新期间会等待锁释放后再执行。
func TestManagerPrefixSingleKeyWaitsFullRefresh(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "prefix-wait",
			Title: "前缀等待缓存",
			Key:   "prefix-wait:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	}, WithWait(20*time.Millisecond, 10), WithMetrics(metrics), WithLogger(logger))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	commitLockKey := manager.lockKey(manager.prefixCommitLockName(manager.prefixTargets[0]))
	if err := client.Set(ctx, commitLockKey, "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set prefix lock error = %v", err)
	}
	go func() {
		time.Sleep(60 * time.Millisecond)
		_ = client.Del(ctx, commitLockKey).Err()
	}()
	startedAt := time.Now()
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-wait:1")); err != nil {
		t.Fatalf("RefreshByKey(prefix-wait:1) error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < 60*time.Millisecond {
		t.Fatalf("RefreshByKey(prefix-wait:1) elapsed = %v, want >= 60ms", elapsed)
	}
	if got := client.Get(ctx, defaultPhysicalKey("prefix-wait:1")).Val(); got != "ok" {
		t.Fatalf("prefix-wait:1 value = %q, want ok", got)
	}
	if got := metrics.prefixWait.Load(); got != 1 {
		t.Fatalf("prefix wait = %d, want 1", got)
	}
	if !logger.contains("event=\"prefix_wait\"") {
		t.Fatalf("prefix wait log not found, logs=%v", logger.messages())
	}
}

// TestManagerPrefixSingleKeyRetryMetrics 验证单 key 在写回前遇到前缀全量刷新时会记录重试指标与日志。
func TestManagerPrefixSingleKeyRetryMetrics(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	firstStarted := make(chan struct{}, 1)
	firstReleased := make(chan struct{})
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "prefix-retry",
			Title: "前缀重试缓存",
			Key:   "prefix-retry:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				call := loaderCalls.Add(1)
				if call == 1 {
					firstStarted <- struct{}{}
					<-firstReleased
				}
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	}, WithWait(20*time.Millisecond, 20), WithMetrics(metrics), WithLogger(logger))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-retry:1"))
	}()
	<-firstStarted
	commitLockKey := manager.lockKey(manager.prefixCommitLockName(manager.prefixTargets[0]))
	if err := client.Set(ctx, commitLockKey, "full-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set prefix lock error = %v", err)
	}
	if err := manager.writePrefixRefreshEpoch(ctx, manager.prefixTargets[0], "new-generation", acquireTestLockGuard(t, manager.store, defaultPhysicalKey("prefix-epoch:"))); err != nil {
		t.Fatalf("write prefix epoch error = %v", err)
	}
	close(firstReleased)
	time.Sleep(60 * time.Millisecond)
	if err := client.Del(ctx, commitLockKey).Err(); err != nil {
		t.Fatalf("del prefix lock error = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("RefreshByKey(prefix-retry:1) error = %v", err)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
	if got := metrics.prefixRetry.Load(); got != 1 {
		t.Fatalf("prefix retry = %d, want 1", got)
	}
	if got := metrics.prefixWait.Load(); got != 1 {
		t.Fatalf("prefix wait = %d, want 1", got)
	}
	if !logger.contains("event=\"prefix_retry\"") {
		t.Fatalf("prefix retry log not found, logs=%v", logger.messages())
	}
}

// TestManagerPrefixEpochBlocksStaleSingleKeyWrite 验证前缀全量刷新已完成后，旧单 key 刷新也会因代际变化放弃旧结果并重试。
func TestManagerPrefixEpochBlocksStaleSingleKeyWrite(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	firstStarted := make(chan struct{}, 1)
	firstReleased := make(chan struct{})
	var singleCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "prefix-epoch",
			Title: "前缀代际缓存",
			Key:   "prefix-epoch:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case defaultPhysicalKey("prefix-epoch:1"):
					call := singleCalls.Add(1)
					if call == 1 {
						firstStarted <- struct{}{}
						<-firstReleased
						return []Entry{{Key: params.Key, Type: TypeString, Value: "stale"}}, nil
					}
					return []Entry{{Key: params.Key, Type: TypeString, Value: "fresh"}}, nil
				case defaultPhysicalKey("prefix-epoch:"):
					return []Entry{{Key: defaultPhysicalKey("prefix-epoch:1"), Type: TypeString, Value: "full-refresh"}}, nil
				default:
					return nil, nil
				}
			},
		},
	}, WithWait(20*time.Millisecond, 20), WithMetrics(metrics), WithLogger(logger))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-epoch:1"))
	}()
	<-firstStarted
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-epoch:")); err != nil {
		t.Fatalf("RefreshByKey(prefix-epoch:) error = %v", err)
	}
	close(firstReleased)
	if err := <-done; err != nil {
		t.Fatalf("RefreshByKey(prefix-epoch:1) error = %v", err)
	}
	if got := singleCalls.Load(); got != 2 {
		t.Fatalf("single loader calls = %d, want 2", got)
	}
	if got := client.Get(ctx, defaultPhysicalKey("prefix-epoch:1")).Val(); got != "fresh" {
		t.Fatalf("prefix-epoch:1 value = %q, want fresh", got)
	}
	if got := metrics.prefixRetry.Load(); got != 1 {
		t.Fatalf("prefix retry = %d, want 1", got)
	}
	if !logger.contains("event=\"prefix_retry\"") {
		t.Fatalf("prefix retry log not found, logs=%v", logger.messages())
	}
}

// TestManagerDefaultWaitUsesLoaderTimeout 验证默认等待策略会参考 lockTTL/loaderTTL，避免固定 240ms 误超时。
func TestManagerDefaultWaitUsesLoaderTimeout(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "wait-adaptive",
			Title: "自适应等待缓存",
			Key:   "wait-adaptive",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				t.Fatal("等待其它实例重建时不应执行加载器")
				return nil, nil
			},
		},
	}, WithLockTTL(100*time.Millisecond), WithLoaderTimeout(320*time.Millisecond))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, manager.lockKey(defaultPhysicalKey("wait-adaptive")), "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock error = %v", err)
	}
	go func() {
		time.Sleep(260 * time.Millisecond)
		_ = client.Set(ctx, manager.rebuildResultKey(defaultPhysicalKey("wait-adaptive")), "other-owner", time.Minute).Err()
		_ = client.Del(ctx, manager.lockKey(defaultPhysicalKey("wait-adaptive"))).Err()
	}()
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("wait-adaptive")); err != nil {
		t.Fatalf("RefreshByKey(wait-adaptive) error = %v, want nil", err)
	}
}

// TestManagerSingleflight 验证同进程内同一个 key 的并发刷新只打到 Redis 一次。
func TestManagerSingleflight(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &countingStore{Store: NewRedisStore(client)}
	var loaderCalls atomic.Int64
	manager, err := newTestManager(store, []Target{
		{
			Index: "sf",
			Title: "单飞缓存",
			Key:   "sf:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				time.Sleep(120 * time.Millisecond)
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	}, WithWait(10*time.Millisecond, 20))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}

	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			errs <- manager.RefreshByKey(ctx, defaultPhysicalKey("sf:1"))
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("RefreshByKey(sf:1) error = %v", err)
		}
	}
	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
	if got := store.acquireCalls.Load(); got != 3 {
		t.Fatalf("AcquireRefreshLock calls = %d, want 3 key+two commit windows", got)
	}
}

// TestRedisStoreWriteBatch 验证批量写入可以一次提交多种缓存结构。
func TestRedisStoreWriteBatch(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	err := store.writeBatch(ctx, []Entry{
		{Key: "batch:string", Type: TypeString, Value: "ok"},
		{Key: "batch:hash", Type: TypeHash, Value: map[string]any{"name": "alpha"}},
		{Key: "batch:set", Type: TypeSet, Value: []any{"a", "b"}},
		{Key: "batch:zset", Type: TypeZSet, Value: []ZMember{{Member: "a", Score: 1}}},
	})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if got := client.Get(ctx, "batch:string").Val(); got != "ok" {
		t.Fatalf("batch:string = %q, want ok", got)
	}
	if got := client.HGet(ctx, "batch:hash", "name").Val(); got != "alpha" {
		t.Fatalf("batch:hash name = %q, want alpha", got)
	}
	if ok := client.SIsMember(ctx, "batch:set", "a").Val(); !ok {
		t.Fatalf("batch:set missing member a")
	}
	if score := client.ZScore(ctx, "batch:zset", "a").Val(); score != 1 {
		t.Fatalf("batch:zset score = %v, want 1", score)
	}
}

// TestManagerGet 验证 Manager.Get 可以读取、反序列化并记录命中/未命中指标。
func TestManagerGet(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "profile",
			Title: "资料缓存",
			Key:   "profile:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{
					Key:   params.Key,
					Type:  TypeString,
					Value: map[string]any{"name": "alpha"},
				}}, nil
			},
		},
	}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("profile:1")); err != nil {
		t.Fatalf("RefreshByKey(profile:1) error = %v", err)
	}
	var profile struct {
		Name string `json:"name"`
	}
	ok, err := manager.Get(ctx, "profile:1", &profile)
	if err != nil {
		t.Fatalf("Get(profile:1) error = %v", err)
	}
	if !ok || profile.Name != "alpha" {
		t.Fatalf("Get(profile:1) ok=%v profile=%+v, want alpha", ok, profile)
	}
	ok, err = manager.Get(ctx, "profile:404", &profile)
	if err != nil {
		t.Fatalf("Get(profile:404) error = %v", err)
	}
	if ok {
		t.Fatalf("Get(profile:404) ok = true, want false")
	}
	if got := metrics.hit.Load(); got != 1 {
		t.Fatalf("metrics hit = %d, want 1", got)
	}
	if got := metrics.miss.Load(); got != 1 {
		t.Fatalf("metrics miss = %d, want 1", got)
	}
	if got := metrics.refresh.Load(); got != 1 {
		t.Fatalf("metrics refresh = %d, want 1", got)
	}
}

// TestManagerErrNotFoundWritesEmptyMarker 验证加载器返回 ErrNotFound 时写入空值占位。
func TestManagerErrNotFoundWritesEmptyMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "missing",
			Title:            "不存在缓存",
			Key:              "missing:",
			Type:             TypeHash,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, errors.Tag(ErrNotFound)
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("missing:1")); err != nil {
		t.Fatalf("RefreshByKey(missing:1) error = %v", err)
	}
	var value map[string]string
	ok, err := manager.Get(ctx, "missing:1", &value)
	if err != nil {
		t.Fatalf("Get(missing:1) error = %v", err)
	}
	if ok {
		t.Fatalf("Get(missing:1) ok = true, want false")
	}
	if got := client.Get(ctx, manager.emptyKey(defaultPhysicalKey("missing:1"))).Val(); got != DefaultEmptyMarker {
		t.Fatalf("empty marker = %q, want %q", got, DefaultEmptyMarker)
	}
}

// TestManagerDeleteByKeyAndPrefix 验证精确删除和前缀删除语义分离。
func TestManagerDeleteByKeyAndPrefix(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "demo",
			Title: "演示缓存",
			Key:   "demo:",
			Type:  TypeString,
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	demoRoot := defaultPhysicalKey("demo:")
	demo1 := defaultPhysicalKey("demo:1")
	demo2 := defaultPhysicalKey("demo:2")
	if err := client.MSet(ctx,
		demoRoot, "root",
		demo1, "one",
		demo2, "two",
		manager.emptyKey(demo1), DefaultEmptyMarker,
		manager.rebuildResultKey(demo1), "done",
		manager.emptyKey(demo2), DefaultEmptyMarker,
		manager.rebuildResultKey(demo2), "done",
	).Err(); err != nil {
		t.Fatalf("MSet error = %v", err)
	}
	if err := manager.DeleteByKey(ctx, defaultPhysicalKey("demo:1")); err != nil {
		t.Fatalf("DeleteByKey(demo:1) error = %v", err)
	}
	if exists := client.Exists(ctx, demo1, manager.emptyKey(demo1), manager.rebuildResultKey(demo1)).Val(); exists != 0 {
		t.Fatalf("demo:1 related keys exists = %d, want 0", exists)
	}
	if exists := client.Exists(ctx, demoRoot, demo2, manager.emptyKey(demo2), manager.rebuildResultKey(demo2)).Val(); exists != 4 {
		t.Fatalf("demo root/demo:2 related keys exists = %d, want 4", exists)
	}
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("demo:")); err != nil {
		t.Fatalf("DeleteByPrefix(demo:) error = %v", err)
	}
	if exists := client.Exists(ctx, demoRoot, demo2, manager.emptyKey(demo2), manager.rebuildResultKey(demo2)).Val(); exists != 0 {
		t.Fatalf("demo prefix related keys exists = %d, want 0", exists)
	}
}

// TestManagerDeleteByKeyWaitsInFlightRefresh 验证单 key 删除会等待同 key 刷新结束后再删除。
func TestManagerDeleteByKeyWaitsInFlightRefresh(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "delete-race",
		Title: "删除刷新竞态缓存",
		Key:   "delete-race:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			close(loaderStarted)
			<-releaseLoader
			return []Entry{{Key: params.Key, Type: TypeString, Value: "stale"}}, nil
		},
	}}, WithWait(5*time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("delete-race:1"))
	}()
	<-loaderStarted
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- manager.DeleteByKey(ctx, defaultPhysicalKey("delete-race:1"))
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("DeleteByKey completed before refresh released, err=%v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseLoader)
	if err := <-refreshDone; err != nil {
		t.Fatalf("RefreshByKey(delete-race:1) error = %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteByKey(delete-race:1) error = %v", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("delete-race:1")).Val(); exists != 0 {
		t.Fatalf("delete-race:1 exists = %d, want 0 after DeleteByKey", exists)
	}
}

// TestManagerDeleteByPrefixInvalidatesInFlightRefresh 验证前缀删除会阻断已开始但尚未写回的旧单 key 刷新。
func TestManagerDeleteByPrefixInvalidatesInFlightRefresh(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "prefix-delete-race",
		Title: "前缀删除刷新竞态缓存",
		Key:   "prefix-delete-race:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			close(loaderStarted)
			<-releaseLoader
			return []Entry{{Key: params.Key, Type: TypeString, Value: "stale"}}, nil
		},
	}}, WithWait(5*time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("prefix-delete-race:1"))
	}()
	<-loaderStarted
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("prefix-delete-race:")); err != nil {
		t.Fatalf("DeleteByPrefix(prefix-delete-race:) error = %v", err)
	}
	close(releaseLoader)
	err = <-refreshDone
	if !errors.Is(err, ErrRefreshInvalidated) {
		t.Fatalf("RefreshByKey after prefix delete error = %v, want ErrRefreshInvalidated", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("prefix-delete-race:1")).Val(); exists != 0 {
		t.Fatalf("prefix-delete-race:1 exists = %d, want 0 after invalidated refresh", exists)
	}
}

// TestManagerDeleteRejectsUnregisteredKey 验证删除能力只允许操作已注册缓存目标。
func TestManagerDeleteRejectsUnregisteredKey(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "demo",
			Title: "演示缓存",
			Key:   "demo:",
			Type:  TypeString,
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	err = manager.DeleteByKey(ctx, defaultPhysicalKey("other:1"))
	if !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("DeleteByKey(other:1) error = %v, want ErrTargetNotFound", err)
	}
	err = manager.DeleteByPrefix(ctx, defaultPhysicalKey("other:"))
	if !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("DeleteByPrefix(other:) error = %v, want ErrTargetNotFound", err)
	}
	managedKey := defaultPhysicalKey("demo:1")
	if err := client.Set(ctx, managedKey, "keep", time.Hour).Err(); err != nil {
		t.Fatalf("Set(managed key) error = %v", err)
	}
	err = manager.DeleteByPrefix(ctx, defaultPhysicalKey("demo:*"))
	if !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("DeleteByPrefix(demo:*) error = %v, want ErrTargetNotFound", err)
	}
	if exists := client.Exists(ctx, managedKey).Val(); exists != 1 {
		t.Fatalf("managed key exists after wildcard request = %d, want 1", exists)
	}
}

// TestManagerDeleteByPrefixConcurrentPatterns 验证大 keyspace 场景下多个删除 pattern 可以按配置并发执行。
func TestManagerDeleteByPrefixConcurrentPatterns(t *testing.T) {
	ctx := context.Background()              // ctx 表示本次前缀删除测试的生命周期上下文
	store := &concurrentDeletePatternStore{} // store 模拟慢速 DeletePattern，用于观察 Manager 是否并发调度多个 pattern
	targets := []Target{{                    // targets 只注册一个前缀目标，确保 DeleteByPrefix 的并发来自业务 key 与内部元信息 pattern
		Index: "fast-delete",
		Title: "快速前缀删除缓存",
		Key:   "fast:",
		Type:  TypeString,
	}}
	manager, err := newTestManager(store, targets, WithPrefixDeleteConcurrency(4))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("fast:")); err != nil {
		t.Fatalf("DeleteByPrefix(fast:) error = %v", err)
	}
	if got := store.calls.Load(); got != 6 {
		t.Fatalf("DeletePattern calls = %d, want 6 business/meta patterns", got)
	}
	if got := store.maxActive.Load(); got <= 1 {
		t.Fatalf("max active DeletePattern = %d, want concurrent execution", got)
	}
}

// TestManagerDeleteByPrefixUsesPrefixIndex 验证前缀全量刷新建立索引后，DeleteByPrefix 可绕开全库 SCAN。
func TestManagerDeleteByPrefixUsesPrefixIndex(t *testing.T) {
	ctx := context.Background()                                             // ctx 表示本次索引快删测试的生命周期上下文
	server := runStandaloneRedis(t)                                         // server 提供内存 Redis，用于观察索引集合与业务 key 状态
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})          // client 是 RedisStore 使用的底层客户端
	store := &deletePatternCountingStore{RedisStore: NewRedisStore(client)} // store 统计 DeletePattern 调用次数，用于确认第二次删除没有走全库 SCAN
	manager, err := newTestManager(store, []Target{{                        // manager 注册前缀目标，RefreshByKey(prefix) 会触发全量刷新并建立可信索引
		Index: "indexed",
		Title: "索引前缀缓存",
		Key:   "indexed:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{
				{Key: defaultPhysicalKey("indexed:1"), Type: TypeString, Value: "one"},
				{Key: defaultPhysicalKey("indexed:2"), Type: TypeString, Value: "two"},
			}, nil
		},
	}}, WithPrefixKeyIndexTTL(time.Hour))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("indexed:")); err != nil {
		t.Fatalf("RefreshByKey(indexed:) error = %v", err)
	}
	target := manager.prefixTargetsM[defaultPhysicalKey("indexed:")] // target 表示已注册的前缀目标，用于定位内部索引 key
	indexKey := manager.prefixIndexKey(target)                       // indexKey 是当前前缀目标维护的 key 索引集合
	readyKey := manager.prefixIndexReadyKey(target)                  // readyKey 是索引可信标记，存在时 DeleteByPrefix 可走索引快路径
	if exists := client.Exists(ctx, readyKey).Val(); exists != 1 {
		t.Fatalf("prefix index ready exists = %d, want 1", exists)
	}
	if ok := testPrefixIndexContains(ctx, client, indexKey, defaultPhysicalKey("indexed:1")); !ok {
		t.Fatalf("index missing indexed:1 member")
	}
	store.deletePatternCalls.Store(0)
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("indexed:")); err != nil {
		t.Fatalf("DeleteByPrefix(indexed:) error = %v", err)
	}
	if got := store.deletePatternCalls.Load(); got != 0 {
		t.Fatalf("DeletePattern calls = %d, want 0 when prefix index is ready", got)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("indexed:1"), defaultPhysicalKey("indexed:2")).Val(); exists != 0 {
		t.Fatalf("indexed business keys exist = %d, want 0", exists)
	}
	if exists := client.Exists(ctx, indexKey, readyKey).Val(); exists != 2 {
		t.Fatalf("safe superset index/ready exists = %d, want 2", exists)
	}
}

// TestManagerDeleteByPrefixFallsBackScanWithoutReadyIndex 验证索引未可信时仍降级全库 SCAN。
func TestManagerDeleteByPrefixFallsBackScanWithoutReadyIndex(t *testing.T) {
	ctx := context.Background()                                             // ctx 表示本次降级删除测试的生命周期上下文
	server := runStandaloneRedis(t)                                         // server 提供内存 Redis，用于写入未被索引覆盖的历史 key
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})          // client 是 RedisStore 使用的底层客户端
	store := &deletePatternCountingStore{RedisStore: NewRedisStore(client)} // store 统计 DeletePattern 调用次数，用于确认未就绪索引不会误用
	manager, err := newTestManager(store, []Target{{Index: "fallback", Key: "fallback:", Type: TypeString}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, defaultPhysicalKey("fallback:stale"), "old", 0).Err(); err != nil {
		t.Fatalf("Set(fallback:stale) error = %v", err)
	}
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("fallback:")); err != nil {
		t.Fatalf("DeleteByPrefix(fallback:) error = %v", err)
	}
	if got := store.deletePatternCalls.Load(); got == 0 {
		t.Fatalf("DeletePattern calls = 0, want scan fallback for non-ready index")
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("fallback:stale")).Val(); exists != 0 {
		t.Fatalf("fallback:stale exists = %d, want 0", exists)
	}
}

// TestManagerDeleteByPrefixFallsBackWhenReadyIndexMissing 验证 ready 标记残留但索引集合缺失时会降级 SCAN，避免漏删业务 key。
func TestManagerDeleteByPrefixFallsBackWhenReadyIndexMissing(t *testing.T) {
	ctx := context.Background()                                             // ctx 表示本次不可信索引降级测试的生命周期上下文
	server := runStandaloneRedis(t)                                         // server 提供内存 Redis，用于构造 ready 残留但 index 缺失的状态
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})          // client 是 RedisStore 使用的底层客户端
	store := &deletePatternCountingStore{RedisStore: NewRedisStore(client)} // store 统计 DeletePattern 调用次数，用于确认发生降级扫描
	manager, err := newTestManager(store, []Target{{Index: "fallback-missing", Key: "fallback-missing:", Type: TypeString}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	target := manager.prefixTargetsM[defaultPhysicalKey("fallback-missing:")] // target 表示当前前缀目标，用于定位 ready/index 元信息
	readyKey := manager.prefixIndexReadyKey(target)                           // readyKey 被手动写入，模拟索引集合过期或被驱逐后的残留可信标记
	if err := client.Set(ctx, defaultPhysicalKey("fallback-missing:stale"), "old", 0).Err(); err != nil {
		t.Fatalf("Set(fallback-missing:stale) error = %v", err)
	}
	if err := client.Set(ctx, readyKey, "ready", time.Hour).Err(); err != nil {
		t.Fatalf("Set(%s) error = %v", readyKey, err)
	}
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("fallback-missing:")); err != nil {
		t.Fatalf("DeleteByPrefix(fallback-missing:) error = %v", err)
	}
	if got := store.deletePatternCalls.Load(); got == 0 {
		t.Fatalf("DeletePattern calls = 0, want scan fallback when ready exists but index is missing")
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("fallback-missing:stale"), readyKey).Val(); exists != 0 {
		t.Fatalf("fallback-missing stale/ready exists = %d, want 0", exists)
	}
}

// TestManagerDeleteByPrefixCanDisableScanFallback 验证生产可关闭索引未就绪时的全库 SCAN 降级。
func TestManagerDeleteByPrefixCanDisableScanFallback(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &deletePatternCountingStore{RedisStore: NewRedisStore(client)}
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	manager, err := newTestManager(
		store,
		[]Target{{Index: "no-scan", Key: "no-scan:", Type: TypeString}},
		WithScanFallback(false),
		WithMetrics(metrics),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, defaultPhysicalKey("no-scan:stale"), "old", 0).Err(); err != nil {
		t.Fatalf("Set(no-scan:stale) error = %v", err)
	}
	err = manager.DeleteByPrefix(ctx, defaultPhysicalKey("no-scan:"))
	if !errors.Is(err, ErrScanFallbackDisabled) {
		t.Fatalf("DeleteByPrefix(no-scan:) error = %v, want ErrScanFallbackDisabled", err)
	}
	if got := store.deletePatternCalls.Load(); got != 0 {
		t.Fatalf("DeletePattern calls = %d, want 0 when scan fallback disabled", got)
	}
	if got := metrics.scanFallback.Load(); got != 0 {
		t.Fatalf("scanFallback metric = %d, want 0 when fallback is rejected", got)
	}
	if logger.contains("event=\"prefix_scan_fallback\"") {
		t.Fatalf("unexpected prefix scan fallback log, logs=%v", logger.messages())
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("no-scan:stale")).Val(); exists != 1 {
		t.Fatalf("no-scan:stale exists = %d, want 1 after disabled scan fallback", exists)
	}
}

// TestManagerDeleteByKeyKeepsPrefixIndexSuperset 验证精确删除保留安全超集成员，避免旧 owner 误删新索引。
func TestManagerDeleteByKeyKeepsPrefixIndexSuperset(t *testing.T) {
	ctx := context.Background()                                    // ctx 表示本次精确删除索引维护测试的生命周期上下文
	server := runStandaloneRedis(t)                                // server 提供内存 Redis，用于检查索引成员状态
	client := redis.NewClient(&redis.Options{Addr: server.Addr()}) // client 是 RedisStore 使用的底层客户端
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "remove-index",
		Title: "删除索引成员缓存",
		Key:   "remove-index:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
		},
	}}, WithPrefixKeyIndexTTL(time.Hour))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("remove-index:1")); err != nil {
		t.Fatalf("RefreshByKey(remove-index:1) error = %v", err)
	}
	target := manager.prefixTargetsM[defaultPhysicalKey("remove-index:")] // target 表示当前前缀目标，用于定位内部索引集合
	indexKey := manager.prefixIndexKey(target)                            // indexKey 是当前前缀目标维护的 key 索引集合
	if ok := testPrefixIndexContains(ctx, client, indexKey, defaultPhysicalKey("remove-index:1")); !ok {
		t.Fatalf("index missing remove-index:1 before DeleteByKey")
	}
	if err := manager.DeleteByKey(ctx, defaultPhysicalKey("remove-index:1")); err != nil {
		t.Fatalf("DeleteByKey(remove-index:1) error = %v", err)
	}
	if ok := testPrefixIndexContains(ctx, client, indexKey, defaultPhysicalKey("remove-index:1")); !ok {
		t.Fatalf("safe superset should retain remove-index:1 after DeleteByKey")
	}
}

// TestManagerRefreshUsesApplyMutation 验证刷新写回使用合并变更契约，减少删除、写入和索引维护的 Redis 往返。
func TestManagerRefreshUsesApplyMutation(t *testing.T) {
	ctx := context.Background()                                    // ctx 表示本次合并写入测试的生命周期上下文
	server := runStandaloneRedis(t)                                // server 提供内存 Redis，用于验证真实写入结果
	client := redis.NewClient(&redis.Options{Addr: server.Addr()}) // client 是 RedisStore 使用的底层客户端
	store := &mutationCountingStore{RedisStore: NewRedisStore(client)}
	manager, err := newTestManager(store, []Target{{
		Index: "mutation",
		Title: "合并写入缓存",
		Key:   "mutation:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
		},
	}}, WithPrefixKeyIndexTTL(time.Hour))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("mutation:1")); err != nil {
		t.Fatalf("RefreshByKey(mutation:1) error = %v", err)
	}
	if got := store.mutationCalls.Load(); got == 0 {
		t.Fatalf("mutation calls = 0, want ApplyMutation fast path")
	}
	if got := client.Get(ctx, defaultPhysicalKey("mutation:1")).Val(); got != "ok" {
		t.Fatalf("mutation:1 value = %q, want ok", got)
	}
	target := manager.prefixTargetsM[defaultPhysicalKey("mutation:")] // target 表示当前前缀目标，用于定位索引集合
	if ok := testPrefixIndexContains(ctx, client, manager.prefixIndexKey(target), defaultPhysicalKey("mutation:1")); !ok {
		t.Fatalf("prefix index missing mutation:1 after refresh")
	}
}

// TestManagerDefaultKeyPrefix 验证默认业务 key 前缀会隔离真实 Redis 数据，刷新和删除必须使用实际 Redis key。
func TestManagerDefaultKeyPrefix(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	// physicalKey 是默认前缀生效后的真实 Redis key，用于确认逻辑 key 不会裸写入 Redis。
	physicalKey := defaultKeyPrefix + "ns:1"
	// loaderCalls 记录回源次数，用于验证未带前缀的刷新请求不会触发写入。
	var loaderCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:    "ns",
			Title:    "默认命名空间缓存",
			Key:      "ns:",
			KeyTitle: "ns:{id}",
			Type:     TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				if params.Key != physicalKey {
					t.Fatalf("loader params key = %q, want %q", params.Key, physicalKey)
				}
				if len(params.KeyParts) != 1 || params.KeyParts[0] != "1" {
					t.Fatalf("loader key parts = %#v, want [1]", params.KeyParts)
				}
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "ns:1"); !errors.Is(err, ErrKeyPrefixRequired) {
		t.Fatalf("RefreshByKey(ns:1) error = %v, want ErrKeyPrefixRequired", err)
	}
	if got := loaderCalls.Load(); got != 0 {
		t.Fatalf("loader calls after logical refresh = %d, want 0", got)
	}
	if err := manager.RefreshByKeys(ctx, []string{physicalKey, physicalKey}); err != nil {
		t.Fatalf("RefreshByKeys(physicalKey) error = %v", err)
	}
	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1 after physical dedupe", got)
	}
	if got := client.Get(ctx, physicalKey).Val(); got != "ok" {
		t.Fatalf("%s value = %q, want ok", physicalKey, got)
	}
	if exists := client.Exists(ctx, "ns:1").Val(); exists != 0 {
		t.Fatalf("logical key exists = %d, want 0", exists)
	}
	var value string
	result, err := manager.GetState(ctx, "ns:1", &value)
	if err != nil {
		t.Fatalf("GetState(ns:1) error = %v", err)
	}
	if result.State != LookupStateHit || value != "ok" {
		t.Fatalf("GetState(ns:1) = %+v value=%q, want hit ok", result, value)
	}
	if err := manager.DeleteByKey(ctx, "ns:1"); !errors.Is(err, ErrKeyPrefixRequired) {
		t.Fatalf("DeleteByKey(ns:1) error = %v, want ErrKeyPrefixRequired", err)
	}
	if exists := client.Exists(ctx, physicalKey).Val(); exists != 1 {
		t.Fatalf("physical key exists after logical delete = %d, want 1", exists)
	}
	if err := manager.DeleteByKey(ctx, physicalKey); err != nil {
		t.Fatalf("DeleteByKey(physicalKey) error = %v", err)
	}
	if exists := client.Exists(ctx, physicalKey).Val(); exists != 0 {
		t.Fatalf("physical key exists after delete = %d, want 0", exists)
	}
}

// TestManagerRejectsUnsafeKeyPrefix 验证构造期会拒绝空前缀和危险前缀。
func TestManagerRejectsUnsafeKeyPrefix(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	_, err := NewManager(
		NewRedisStore(client),
		[]Target{{Index: "strict", Key: "strict:", Type: TypeString}},
		WithKeyPrefix(""),
	)
	if !errors.Is(err, ErrInvalidKeyPrefix) {
		t.Fatalf("NewManager(empty prefix) error = %v, want ErrInvalidKeyPrefix", err)
	}
	_, err = NewManager(
		NewRedisStore(client),
		[]Target{{Index: "strict", Key: "strict:", Type: TypeString}},
		WithKeyPrefix("bad * prefix:"),
	)
	if !errors.Is(err, ErrInvalidKeyPrefix) {
		t.Fatalf("NewManager(wildcard prefix) error = %v, want ErrInvalidKeyPrefix", err)
	}
	_, err = NewManager(
		NewRedisStore(client),
		[]Target{{Index: "strict", Key: "strict:", Type: TypeString}},
		WithKeyPrefix("app"),
	)
	if !errors.Is(err, ErrInvalidKeyPrefix) {
		t.Fatalf("NewManager(prefix without delimiter) error = %v, want ErrInvalidKeyPrefix", err)
	}
	for _, prefix := range []string{"tcm", "tcm:", "tcm:business:"} {
		_, err = NewManager(
			NewRedisStore(client),
			[]Target{{Index: "strict", Key: "strict:", Type: TypeString}},
			WithKeyPrefix(prefix),
		)
		if !errors.Is(err, ErrInvalidKeyPrefix) {
			t.Fatalf("NewManager(reserved prefix %q) error = %v, want ErrInvalidKeyPrefix", prefix, err)
		}
	}
}

// TestManagerLogKeyRedaction 验证开启日志脱敏后不会输出完整业务 key。
func TestManagerLogKeyRedaction(t *testing.T) {
	logger := &recordingLogger{}
	manager, err := newTestManager(
		NewRedisStore(redis.NewClient(&redis.Options{Addr: runStandaloneRedis(t).Addr()})),
		nil,
		WithLogger(logger),
		WithLogKeyRedaction(true),
	)
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	manager.logInfoEvent("redact", "user", "user:secret:42", "prefix", "user:secret:", "lock_name", "user:secret:42")
	manager.logWarnEvent("redact_error", "user", "user:secret:99", "err", errors.Errorf("缓存key[user:secret:99]写入失败"))
	logs := strings.Join(logger.messages(), "\n")
	if strings.Contains(logs, "user:secret") {
		t.Fatalf("redacted logs contain raw key, logs=%v", logger.messages())
	}
	if !strings.Contains(logs, "sha1:") {
		t.Fatalf("redacted logs missing hash marker, logs=%v", logger.messages())
	}
}

// TestManagerUnprefixedLoadThroughReadOnly 验证未带指定前缀的 key 只允许查询，不会在 miss 时触发回源写入。
func TestManagerUnprefixedLoadThroughReadOnly(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	physicalHitKey := defaultKeyPrefix + "readonly:hit"
	if err := client.Set(ctx, physicalHitKey, "cached", 0).Err(); err != nil {
		t.Fatalf("Set(%s) error = %v", physicalHitKey, err)
	}
	var loaderCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "readonly",
			Title:            "只读逻辑 key",
			Key:              "readonly:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				return []Entry{{Key: params.Key, Type: TypeString, Value: "fresh"}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value string
	result, err := manager.LoadThrough(ctx, "readonly:hit", &value, nil)
	if err != nil {
		t.Fatalf("LoadThrough(readonly:hit) error = %v", err)
	}
	if result.State != LookupStateHit || result.Refreshed || value != "cached" {
		t.Fatalf("LoadThrough(readonly:hit) = %+v value=%q, want readonly hit cached", result, value)
	}
	result, err = manager.LoadThrough(ctx, "readonly:miss", &value, nil)
	if err != nil {
		t.Fatalf("LoadThrough(readonly:miss) error = %v", err)
	}
	if result.State != LookupStateMiss || result.Refreshed {
		t.Fatalf("LoadThrough(readonly:miss) = %+v, want readonly miss without refresh", result)
	}
	if got := loaderCalls.Load(); got != 0 {
		t.Fatalf("loader calls = %d, want 0 for unprefixed read-through miss", got)
	}
	if exists := client.Exists(ctx, defaultKeyPrefix+"readonly:miss", manager.emptyKey(defaultKeyPrefix+"readonly:miss")).Val(); exists != 0 {
		t.Fatalf("readonly miss related keys exists = %d, want 0", exists)
	}
}

// TestManagerDefaultPrefixIsolatesBareKeyAndTreatsMarkerFieldAsData 验证命名空间隔离和普通 Hash 字段语义。
func TestManagerDefaultPrefixIsolatesBareKeyAndTreatsMarkerFieldAsData(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "namespace",
			Title:            "命名空间隔离缓存",
			Key:              "namespace:",
			Type:             TypeHash,
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := client.HSet(ctx, "namespace:1", "name", "bare").Err(); err != nil {
		t.Fatalf("HSet(namespace:1) error = %v", err)
	}
	result, err := manager.GetState(ctx, "namespace:1", &map[string]string{})
	if err != nil {
		t.Fatalf("GetState(namespace:1) error = %v", err)
	}
	if result.State != LookupStateMiss {
		t.Fatalf("GetState(namespace:1) state = %s, want miss for bare key", result.State)
	}
	physicalKey := defaultKeyPrefix + "namespace:2"
	if err := client.HSet(ctx, physicalKey, "value", DefaultEmptyMarker).Err(); err != nil {
		t.Fatalf("HSet(%s) error = %v", physicalKey, err)
	}
	value := map[string]string{}
	result, err = manager.GetState(ctx, "namespace:2", &value)
	if err != nil {
		t.Fatalf("GetState(namespace:2) error = %v", err)
	}
	if result.State != LookupStateHit || value["value"] != DefaultEmptyMarker {
		t.Fatalf("GetState(namespace:2) = %+v value=%#v, want hit with literal marker field", result, value)
	}
}

// TestManagerLiteralEmptyMarkerCanBeBusinessValue 验证真实业务值等于空值占位字符串时不会被误判为空值命中。
func TestManagerLiteralEmptyMarkerCanBeBusinessValue(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "literal",
			Title: "真实占位符值缓存",
			Key:   "literal:",
			Type:  TypeString,
		},
		{
			Index:            "hidden-literal",
			Title:            "隐藏空值真实占位符",
			Key:              "hidden-literal:",
			Type:             TypeString,
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, defaultPhysicalKey("literal:1"), DefaultEmptyMarker, 0).Err(); err != nil {
		t.Fatalf("Set(literal:1) error = %v", err)
	}
	if err := client.Set(ctx, defaultPhysicalKey("hidden-literal:1"), DefaultEmptyMarker, 0).Err(); err != nil {
		t.Fatalf("Set(hidden-literal:1) error = %v", err)
	}
	var literal string
	result, err := manager.GetState(ctx, "literal:1", &literal)
	if err != nil {
		t.Fatalf("GetState(literal:1) error = %v", err)
	}
	if result.State != LookupStateHit || literal != DefaultEmptyMarker {
		t.Fatalf("literal result=%+v value=%q, want hit literal marker", result, literal)
	}
	var hiddenLiteral string
	result, err = manager.GetState(ctx, "hidden-literal:1", &hiddenLiteral)
	if err != nil {
		t.Fatalf("GetState(hidden-literal:1) error = %v", err)
	}
	if result.State != LookupStateHit || hiddenLiteral != DefaultEmptyMarker {
		t.Fatalf("hidden literal result=%+v value=%q, want hit literal marker", result, hiddenLiteral)
	}
}

// TestManagerCustomKeyPrefix 验证业务可通过 WithKeyPrefix 设置项目级 Redis 命名空间，并避免落到默认前缀下。
func TestManagerCustomKeyPrefix(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	// customPrefix 表示业务项目自定义的 Redis 命名空间，用于验证默认前缀可被外部覆盖。
	customPrefix := "app:cache:"
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "custom-user",
			Title: "自定义前缀用户缓存",
			Key:   "user:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "custom"}}, nil
			},
		},
	}, WithKeyPrefix(customPrefix), WithScanFallback(true))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "user:9"); !errors.Is(err, ErrKeyPrefixRequired) {
		t.Fatalf("RefreshByKey(user:9) error = %v, want ErrKeyPrefixRequired", err)
	}
	if err := manager.RefreshByKey(ctx, customPrefix+"user:9"); err != nil {
		t.Fatalf("RefreshByKey(custom prefixed user:9) error = %v", err)
	}
	if got := client.Get(ctx, customPrefix+"user:9").Val(); got != "custom" {
		t.Fatalf("custom prefixed value = %q, want custom", got)
	}
	if exists := client.Exists(ctx, defaultKeyPrefix+"user:9").Val(); exists != 0 {
		t.Fatalf("default prefixed key exists = %d, want 0", exists)
	}
	if err := manager.DeleteByPrefix(ctx, "user:"); !errors.Is(err, ErrKeyPrefixRequired) {
		t.Fatalf("DeleteByPrefix(user:) error = %v, want ErrKeyPrefixRequired", err)
	}
	if err := manager.DeleteByPrefix(ctx, customPrefix+"user:"); err != nil {
		t.Fatalf("DeleteByPrefix(custom prefixed user:) error = %v", err)
	}
	if exists := client.Exists(ctx, customPrefix+"user:9").Val(); exists != 0 {
		t.Fatalf("custom prefixed key exists after delete = %d, want 0", exists)
	}
}

// TestManagerRejectsPrefixedLogicalTarget 验证 Target.Key 与 KeyTitle 不再接受预拼项目级前缀。
func TestManagerRejectsPrefixedLogicalTarget(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	for _, target := range []Target{
		{Index: "prefixed-key", Key: "app:user:", Type: TypeString},
		{Index: "prefixed-title", Key: "user:", KeyTitle: "app:user:{id}", Type: TypeString},
	} {
		if _, err := NewManager(NewRedisStore(client), []Target{target}, WithKeyPrefix("app:")); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewManager(%s) error = %v, want ErrInvalidConfig", target.Index, err)
		}
	}
}

// TestRedisStoreDeletePatternIncremental 验证 DeletePattern 使用小批次 SCAN 与 UNLINK 完成增量删除。
func TestRedisStoreDeletePatternIncremental(t *testing.T) {
	ctx := context.Background()                                    // ctx 表示本次 Redis 删除测试的生命周期上下文
	server := runStandaloneRedis(t)                                // server 提供内存 Redis，用于验证 SCAN 与 UNLINK 行为
	client := redis.NewClient(&redis.Options{Addr: server.Addr()}) // client 是被 RedisStore 包装的 go-redis 客户端
	store := NewRedisStore(client, WithUnlinkChunkSize(4))         // store 启用小批次删除，覆盖大 keyspace 增量清理路径
	keys := make([]string, 0, 25)                                  // keys 记录本次应被 pattern 命中的业务 key，便于最终批量断言
	for index := 0; index < 25; index++ {                          // index 构造足够多的 key，确保 SCAN 会经历多个批次
		key := fmt.Sprintf("scan-fast:%02d", index) // key 表示属于待删除前缀的 Redis key
		keys = append(keys, key)
		if err := client.Set(ctx, key, "value", 0).Err(); err != nil {
			t.Fatalf("Set(%s) error = %v", key, err)
		}
	}
	if err := client.Set(ctx, "scan-slow:keep", "value", 0).Err(); err != nil {
		t.Fatalf("Set(scan-slow:keep) error = %v", err)
	}
	if store.unlinkChunkSize != 4 {
		t.Fatalf("store unlink chunk size = %d, want 4", store.unlinkChunkSize)
	}
	deleted, err := store.deletePattern(ctx, "scan-fast:*", 5)
	if err != nil {
		t.Fatalf("DeletePattern(scan-fast:*) error = %v", err)
	}
	if deleted != int64(len(keys)) {
		t.Fatalf("deleted = %d, want %d", deleted, len(keys))
	}
	if exists := client.Exists(ctx, keys...).Val(); exists != 0 {
		t.Fatalf("scan-fast keys exists = %d, want 0", exists)
	}
	if exists := client.Exists(ctx, "scan-slow:keep").Val(); exists != 1 {
		t.Fatalf("scan-slow:keep exists = %d, want 1", exists)
	}
}

// TestRedisStorePrefixIndexMethods 验证 RedisStore 可按安全超集索引删除真实 key 并保留索引证据。
func TestRedisStorePrefixIndexMethods(t *testing.T) {
	ctx := context.Background()                                    // ctx 表示本次索引存储测试的生命周期上下文
	server := runStandaloneRedis(t)                                // server 提供内存 Redis，用于验证 Set 索引行为
	client := redis.NewClient(&redis.Options{Addr: server.Addr()}) // client 是 RedisStore 使用的底层客户端
	store := NewRedisStore(client, WithUnlinkChunkSize(2))         // store 使用小 chunk 覆盖索引删除的分批路径
	indexKey := "tcm:pidx:test"                                    // indexKey 表示测试使用的前缀索引集合 key
	if err := client.MSet(ctx, "indexed-store:1", "one", "indexed-store:2", "two", "indexed-store:keep", "keep").Err(); err != nil {
		t.Fatalf("MSet indexed-store keys error = %v", err)
	}
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, "indexed-store:1", "indexed-store:2", ""); err != nil {
		t.Fatalf("AddPrefixIndexKeys() error = %v", err)
	}
	if err := commitTestPrefixIndex(t, store, indexKey, time.Hour); err != nil {
		t.Fatalf("commitPrefixIndex() error = %v", err)
	}
	deleted, err := store.DeletePrefixIndexKeys(ctx, indexKey, 1, acquireTestLockGuard(t, store, "index-delete"))
	if err != nil {
		t.Fatalf("DeletePrefixIndexKeys() error = %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if exists := client.Exists(ctx, "indexed-store:1", "indexed-store:2").Val(); exists != 0 {
		t.Fatalf("indexed store business keys exist = %d, want 0", exists)
	}
	if exists := client.Exists(ctx, indexKey).Val(); exists != 1 {
		t.Fatalf("safe superset index exists = %d, want 1", exists)
	}
	if exists := client.Exists(ctx, "indexed-store:keep").Val(); exists != 1 {
		t.Fatalf("indexed-store:keep exists = %d, want 1", exists)
	}
}

// TestRedisStoreApplyMutation 验证 RedisStore 能用合并 Pipeline 同时完成删除、写入和索引维护。
func TestRedisStoreApplyMutation(t *testing.T) {
	ctx := context.Background()                                    // ctx 表示本次合并变更存储测试的生命周期上下文
	server := runStandaloneRedis(t)                                // server 提供内存 Redis，用于验证变更结果
	client := redis.NewClient(&redis.Options{Addr: server.Addr()}) // client 是 RedisStore 使用的底层客户端
	store := NewRedisStore(client, WithUnlinkChunkSize(2))
	indexKey := "tcm:pidx:mutation" // indexKey 表示本次测试使用的前缀索引集合 key
	if err := client.MSet(ctx, "mutation:old", "old", "mutation:stale", "stale").Err(); err != nil {
		t.Fatalf("MSet old keys error = %v", err)
	}
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, "mutation:old", "mutation:stale"); err != nil {
		t.Fatalf("AddPrefixIndexKeys() error = %v", err)
	}
	mutation := StoreMutation{
		DeleteKeys:   []string{"mutation:old"},
		WriteEntries: []Entry{{Key: "mutation:new", Type: TypeString, Value: "new", TTL: time.Hour}},
		AddIndex:     []PrefixIndexMutation{{IndexKey: indexKey, TTL: time.Hour, Keys: []string{"mutation:new"}}},
	}
	if err := store.ApplyMutation(ctx, mutation); err != nil {
		t.Fatalf("ApplyMutation() error = %v", err)
	}
	if exists := client.Exists(ctx, "mutation:old").Val(); exists != 0 {
		t.Fatalf("mutation:old exists = %d, want 0", exists)
	}
	if got := client.Get(ctx, "mutation:new").Val(); got != "new" {
		t.Fatalf("mutation:new = %q, want new", got)
	}
	if ok := testPrefixIndexContains(ctx, client, indexKey, "mutation:new"); !ok {
		t.Fatalf("index missing mutation:new")
	}
	if ok := testPrefixIndexContains(ctx, client, indexKey, "mutation:old"); !ok {
		t.Fatalf("safe superset should retain mutation:old")
	}
	if ok := testPrefixIndexContains(ctx, client, indexKey, "mutation:stale"); !ok {
		t.Fatalf("index should keep unrelated mutation:stale member")
	}
}

// TestRedisStoreApplyMutationRejectsOverlap 验证合并变更在写入前拒绝危险重叠。
func TestRedisStoreApplyMutationRejectsOverlap(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	mutation := StoreMutation{
		DeleteKeys:   []string{"mutation:overlap"},
		WriteEntries: []Entry{{Key: "mutation:overlap", Type: TypeString, Value: "new"}},
	}
	if err := client.Set(ctx, "mutation:overlap", "old", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyMutation(ctx, mutation); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ApplyMutation() error = %v, want ErrInvalidConfig", err)
	}
	if value := client.Get(ctx, "mutation:overlap").Val(); value != "old" {
		t.Fatalf("rejected overlap value = %q, want old", value)
	}
}

// TestRedisStoreApplyMutationPrevalidatesWriteEntries 验证合并变更会在删除旧 key 前校验写入项，避免局部失败导致旧缓存丢失。
func TestRedisStoreApplyMutationPrevalidatesWriteEntries(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:prevalidate"
	if err := client.Set(ctx, "prevalidate:old", "old", 0).Err(); err != nil {
		t.Fatalf("Set(prevalidate:old) error = %v", err)
	}
	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, "prevalidate:old"); err != nil {
		t.Fatalf("AddPrefixIndexKeys() error = %v", err)
	}
	err := store.ApplyMutation(ctx, StoreMutation{
		DeleteKeys:   []string{"prevalidate:old"},
		WriteEntries: []Entry{{Key: "prevalidate:new", Type: CacheType("stream"), Value: "bad"}},
	})
	if err == nil {
		t.Fatalf("ApplyMutation() error = nil, want unsupported type")
	}
	if got := client.Get(ctx, "prevalidate:old").Val(); got != "old" {
		t.Fatalf("prevalidate:old = %q, want old after failed preflight", got)
	}
	if ok := testPrefixIndexContains(ctx, client, indexKey, "prevalidate:old"); !ok {
		t.Fatalf("index missing prevalidate:old after failed preflight")
	}
	if exists := client.Exists(ctx, "prevalidate:new").Val(); exists != 0 {
		t.Fatalf("prevalidate:new exists = %d, want 0", exists)
	}
}

// TestRedisStoreCollectionLimitsAndReadPage 验证集合全量读写可限流，分页读取仍可小步读取大集合。
func TestRedisStoreCollectionLimitsAndReadPage(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client, WithMaxCollectionReadCount(2), WithMaxCollectionWriteCount(2))
	err := store.write(ctx, Entry{Key: "limit:list", Type: TypeList, Value: []string{"a", "b", "c"}})
	if !errors.Is(err, ErrCollectionTooLarge) {
		t.Fatalf("Write oversized list error = %v, want ErrCollectionTooLarge", err)
	}
	if exists := client.Exists(ctx, "limit:list").Val(); exists != 0 {
		t.Fatalf("limit:list exists = %d, want 0 after rejected write", exists)
	}
	if err := client.RPush(ctx, "limit:list", "a", "b", "c").Err(); err != nil {
		t.Fatalf("RPush(limit:list) error = %v", err)
	}
	if _, err := store.Read(ctx, "limit:list", TypeList); !errors.Is(err, ErrCollectionTooLarge) {
		t.Fatalf("Read oversized list error = %v, want ErrCollectionTooLarge", err)
	}
	page, err := store.ReadPage(ctx, "limit:list", TypeList, ReadPageOptions{Start: 0, Count: 2})
	if err != nil {
		t.Fatalf("ReadPage(limit:list) error = %v", err)
	}
	values, ok := page.Value.([]string)
	if !ok || len(values) != 2 || values[0] != "a" || values[1] != "b" {
		t.Fatalf("ReadPage(limit:list) = %#v, want first two values", page.Value)
	}
	if err := client.HSet(ctx, "limit:hash", "name", "alpha", "age", "18").Err(); err != nil {
		t.Fatalf("HSet(limit:hash) error = %v", err)
	}
	hashPage, err := store.ReadPage(ctx, "limit:hash", TypeHash, ReadPageOptions{Fields: []string{"age"}})
	if err != nil {
		t.Fatalf("ReadPage(limit:hash fields) error = %v", err)
	}
	hashValues, ok := hashPage.Value.(map[string]string)
	if !ok || len(hashValues) != 1 || hashValues["age"] != "18" {
		t.Fatalf("ReadPage(limit:hash fields) = %#v, want age=18", hashPage.Value)
	}
}

// TestRedisStoreOverwriteFlag 验证 Overwrite=false 时保留旧数据并执行增量写入。
func TestRedisStoreOverwriteFlag(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	if err := store.write(ctx, Entry{Key: "merge:hash", Type: TypeHash, Value: map[string]any{"a": "1"}}); err != nil {
		t.Fatalf("Write initial hash error = %v", err)
	}
	if err := store.write(ctx, Entry{Key: "merge:hash", Type: TypeHash, Value: map[string]any{"b": "2"}, Overwrite: Bool(false)}); err != nil {
		t.Fatalf("Write merge hash error = %v", err)
	}
	if got := client.HGetAll(ctx, "merge:hash").Val(); len(got) != 2 || got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("merged hash = %#v, want a+b", got)
	}
	if err := store.write(ctx, Entry{Key: "merge:hash", Type: TypeHash, Value: map[string]any{"c": "3"}}); err != nil {
		t.Fatalf("Write overwrite hash error = %v", err)
	}
	if got := client.HGetAll(ctx, "merge:hash").Val(); len(got) != 1 || got["c"] != "3" {
		t.Fatalf("overwritten hash = %#v, want only c", got)
	}
}

// TestRedisStoreCollectionOverwriteUsesAtomicScript 验证集合结构默认覆盖写通过单条 Lua 脚本完成。
func TestRedisStoreCollectionOverwriteUsesAtomicScript(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	recorder := &pipelineCommandRecorder{}
	client.AddHook(recorder)
	store := NewRedisStore(client)
	if err := store.write(ctx, Entry{Key: "atomic:hash", Type: TypeHash, Value: map[string]any{"a": "1"}, TTL: time.Minute}); err != nil {
		t.Fatalf("Write(atomic hash) error = %v", err)
	}
	names := recorder.pipelineNames()
	if len(names) != 1 || names[0] != "eval" {
		t.Fatalf("pipeline commands = %#v, want single eval", names)
	}
	if got := client.HGet(ctx, "atomic:hash", "a").Val(); got != "1" {
		t.Fatalf("atomic:hash a = %q, want 1", got)
	}
	if ttl := client.TTL(ctx, "atomic:hash").Val(); ttl <= 0 {
		t.Fatalf("atomic:hash ttl = %v, want positive ttl", ttl)
	}
}

// TestTablecacheMetaKeysUseClusterHashTags 验证内部元信息 key 遵循 Redis Cluster 同槽 tag 规范。
func TestTablecacheMetaKeysUseClusterHashTags(t *testing.T) {
	if got := redisClusterHashTag("user:1"); got != "user:1" {
		t.Fatalf("redisClusterHashTag(user:1) = %q, want user:1", got)
	}
	if got := redisClusterHashTag("user:{42}:profile"); got != "42" {
		t.Fatalf("redisClusterHashTag(user:{42}:profile) = %q, want 42", got)
	}
	if hasRedisClusterHashTag("user:1") {
		t.Fatalf("hasRedisClusterHashTag(user:1) = true, want false")
	}
	if !hasRedisClusterHashTag("user:{42}:profile") {
		t.Fatalf("hasRedisClusterHashTag(user:{42}:profile) = false, want true")
	}
	manager, err := newTestManager(NewRedisStore(redis.NewClient(&redis.Options{Addr: runStandaloneRedis(t).Addr()})), []Target{
		{Index: "user", Key: "user:", Type: TypeString, Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) { return nil, nil }},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	for _, metaKey := range []string{
		manager.lockKey("user:1"),
		manager.emptyKey("user:1"),
		manager.emptyCollectionKey("user:1"),
		manager.rebuildResultKey("user:1"),
		manager.prefixEpochKey("user:"),
	} {
		if !strings.Contains(metaKey, "{") || !strings.Contains(metaKey, "}") {
			t.Fatalf("meta key %q missing cluster hash tag", metaKey)
		}
	}
	if got := manager.emptyKey("user:1"); got != "tcm:empty:{user:1}" {
		t.Fatalf("emptyKey(user:1) = %q, want compact tagged meta key", got)
	}
	if got := manager.lockKey("user:{42}:profile"); got != "tcm:rebuild:lock:user:{42}:profile" {
		t.Fatalf("lockKey(user:{42}:profile) = %q, want existing tag reused", got)
	}
	if got := tablecacheMetaKeyPattern("empty", "user:"); got != "tcm:empty:{user:*" {
		t.Fatalf("tablecacheMetaKeyPattern(empty,user:) = %q, want tcm:empty:{user:*", got)
	}
}

// TestPrefixIndexKeysUseCompactNames 验证前缀索引内部 key 使用短命名且不暴露版本口径。
func TestPrefixIndexKeysUseCompactNames(t *testing.T) {
	manager := &Manager{}
	legacyVersionToken := "v" + "2"
	legacyIndexKeysToken := "index" + ":keys"
	legacyIndexReadyToken := "index" + ":ready"
	for _, item := range []struct {
		name      string
		target    Target
		indexKey  string
		readyKey  string
		activeKey string
	}{
		{
			name:      "plain",
			target:    Target{Key: "user:"},
			indexKey:  "tcm:pidx:user:",
			readyKey:  "tcm:pidx:ready:user:",
			activeKey: "tcm:pidx:active:{tcm:pidx:user:}",
		},
		{
			name:      "hash tag",
			target:    Target{Key: "tc:{all}:user:"},
			indexKey:  "tcm:pidx:tc:{all}:user:",
			readyKey:  "tcm:pidx:ready:tc:{all}:user:",
			activeKey: "tcm:pidx:tc:{all}:user:active",
		},
	} {
		indexKey := manager.prefixIndexKey(item.target)
		readyKey := manager.prefixIndexReadyKey(item.target)
		activeKey := prefixIndexActiveKey(indexKey)
		for label, key := range map[string]string{"index": indexKey, "ready": readyKey, "active": activeKey} {
			if strings.Contains(key, legacyVersionToken) || strings.Contains(key, legacyIndexKeysToken) || strings.Contains(key, legacyIndexReadyToken) {
				t.Fatalf("%s %s key = %q, want compact pidx name", item.name, label, key)
			}
		}
		if indexKey != item.indexKey || readyKey != item.readyKey || activeKey != item.activeKey {
			t.Fatalf("%s keys = %q,%q,%q, want %q,%q,%q", item.name, indexKey, readyKey, activeKey, item.indexKey, item.readyKey, item.activeKey)
		}
	}
	if shardKey := prefixIndexShardKey("tcm:pidx:user:", 7); strings.Contains(shardKey, legacyVersionToken) || !strings.HasPrefix(shardKey, "tcm:pidx:") {
		t.Fatalf("shard key = %q, want compact pidx shard key", shardKey)
	}
	if shardKey := prefixIndexShardKey("tcm:pidx:tc:{all}:user:", 7); shardKey != "tcm:pidx:tc:{all}:user:s:07" {
		t.Fatalf("tagged shard key = %q, want child key without repeated root", shardKey)
	}
}

// TestManagerRefreshByKeys 验证批量刷新会去重并刷新每个有效 key。
func TestManagerRefreshByKeys(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "multi",
			Title: "批量缓存",
			Key:   "multi:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				return []Entry{{Key: params.Key, Type: TypeString, Value: params.Key}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKeys(ctx, []string{defaultPhysicalKey("multi:1"), defaultPhysicalKey("multi:1"), "", defaultPhysicalKey("multi:2")}); err != nil {
		t.Fatalf("RefreshByKeys() error = %v", err)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
	if got := client.Get(ctx, defaultPhysicalKey("multi:2")).Val(); got != defaultPhysicalKey("multi:2") {
		t.Fatalf("multi:2 = %q, want multi:2", got)
	}
}

// TestManagerWaitRebuiltByResultMarker 验证空结果刷新也能通过重建结果元信息通知等待方完成。
func TestManagerWaitRebuiltByResultMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "result",
			Title: "结果元信息缓存",
			Key:   "result",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				t.Fatal("等待已有锁时不应执行加载器")
				return nil, nil
			},
		},
	}, WithWait(5*time.Millisecond, 20), WithRebuildResultTTL(time.Minute))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, manager.lockKey(defaultPhysicalKey("result")), "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock error = %v", err)
	}
	go func() {
		time.Sleep(15 * time.Millisecond)
		_ = client.Set(ctx, manager.rebuildResultKey(defaultPhysicalKey("result")), "other-owner", time.Minute).Err()
		_ = client.Del(ctx, manager.lockKey(defaultPhysicalKey("result"))).Err()
	}()
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("result")); err != nil {
		t.Fatalf("RefreshByKey(result) error = %v, want nil", err)
	}
}

// TestManagerWaitRebuiltByOwnerResultMarker 验证锁释放失败时等待方可通过同 owner 的完成标记判定刷新已完成。
func TestManagerWaitRebuiltByOwnerResultMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "owner-result",
			Title: "owner完成标记缓存",
			Key:   "owner-result",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				t.Fatal("等待已有锁时不应执行加载器")
				return nil, nil
			},
		},
	}, WithWait(2*time.Millisecond, 2), WithRebuildResultTTL(time.Minute))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	lockKey := manager.lockKey(defaultPhysicalKey("owner-result"))            // lockKey 表示其它实例仍持有或释放失败的重建锁
	resultKey := manager.rebuildResultKey(defaultPhysicalKey("owner-result")) // resultKey 表示本轮刷新完成标记，值必须与锁 owner 一致才可信
	if err := client.Set(ctx, lockKey, "owner-1", time.Minute).Err(); err != nil {
		t.Fatalf("Set(lockKey) error = %v", err)
	}
	if err := client.Set(ctx, resultKey, "stale-owner", time.Minute).Err(); err != nil {
		t.Fatalf("Set(stale resultKey) error = %v", err)
	}
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("owner-result"))
	if !errors.Is(err, ErrWaitRebuildTimeout) {
		t.Fatalf("RefreshByKey(owner-result stale marker) error = %v, want ErrWaitRebuildTimeout", err)
	}
	if err := client.Set(ctx, resultKey, "owner-1", time.Minute).Err(); err != nil {
		t.Fatalf("Set(owner resultKey) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("owner-result")); err != nil {
		t.Fatalf("RefreshByKey(owner-result owner marker) error = %v, want nil", err)
	}
}

// TestManagerWaitRebuildIgnoresStaleKeyAndResultMarker 验证等待其它实例刷新时不会把旧业务 key 或旧完成标记误判为本轮成功。
func TestManagerWaitRebuildIgnoresStaleKeyAndResultMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var ownerReleased atomic.Bool
	manager1, err := newTestManager(store, []Target{
		{
			Index: "stale-wait",
			Title: "旧值等待缓存",
			Key:   "stale-wait:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				started <- struct{}{}
				<-release
				return nil, errors.Errorf("mock owner refresh failed")
			},
		},
	}, WithWait(5*time.Millisecond, 100), WithRebuildResultTTL(time.Minute))
	if err != nil {
		t.Fatalf("newTestManager(manager1) error = %v", err)
	}
	manager2, err := newTestManager(store, []Target{
		{
			Index: "stale-wait",
			Title: "旧值等待缓存",
			Key:   "stale-wait:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if !ownerReleased.Load() {
					return nil, errors.Errorf("waiter acquired loader before owner released")
				}
				return []Entry{{Key: params.Key, Type: TypeString, Value: "fresh"}}, nil
			},
		},
	}, WithWait(5*time.Millisecond, 100), WithRebuildResultTTL(time.Minute))
	if err != nil {
		t.Fatalf("newTestManager(manager2) error = %v", err)
	}
	if err := client.Set(ctx, defaultPhysicalKey("stale-wait:1"), "old", 0).Err(); err != nil {
		t.Fatalf("Set(stale-wait:1) error = %v", err)
	}
	if err := client.Set(ctx, manager1.rebuildResultKey(defaultPhysicalKey("stale-wait:1")), "stale-done", time.Minute).Err(); err != nil {
		t.Fatalf("Set(stale result marker) error = %v", err)
	}
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- manager1.RefreshByKey(ctx, defaultPhysicalKey("stale-wait:1"))
	}()
	<-started
	secondErr := make(chan error, 1)
	go func() {
		secondErr <- manager2.RefreshByKey(ctx, defaultPhysicalKey("stale-wait:1"))
	}()
	select {
	case err := <-secondErr:
		t.Fatalf("second RefreshByKey returned before owner released: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	ownerReleased.Store(true)
	close(release)
	if err := <-firstErr; err == nil {
		t.Fatalf("first RefreshByKey error = nil, want owner loader failure")
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("second RefreshByKey error = %v, want retry success", err)
	}
	if got := client.Get(ctx, defaultPhysicalKey("stale-wait:1")).Val(); got != "fresh" {
		t.Fatalf("stale-wait:1 value = %q, want fresh", got)
	}
}

// TestManagerFieldsWaiterDoesNotAcceptDifferentFieldsMarker 验证不同 fields 局部刷新不会互相复用完成标记。
func TestManagerFieldsWaiterDoesNotAcceptDifferentFieldsMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	manager1, err := newTestManager(store, []Target{
		{
			Index: "fields-wait",
			Title: "字段等待缓存",
			Key:   "fields-wait:",
			Type:  TypeHash,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				started <- struct{}{}
				<-release
				return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{"name": "alpha"}}}, nil
			},
		},
	}, WithWait(5*time.Millisecond, 100), WithRebuildResultTTL(time.Minute))
	if err != nil {
		t.Fatalf("newTestManager(manager1) error = %v", err)
	}
	manager2, err := newTestManager(store, []Target{
		{
			Index: "fields-wait",
			Title: "字段等待缓存",
			Key:   "fields-wait:",
			Type:  TypeHash,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{"status": "enabled"}}}, nil
			},
		},
	}, WithWait(5*time.Millisecond, 100), WithRebuildResultTTL(time.Minute))
	if err != nil {
		t.Fatalf("newTestManager(manager2) error = %v", err)
	}
	if err := client.Set(ctx, manager1.rebuildResultKey(defaultPhysicalKey("fields-wait:1")), "old-full", time.Minute).Err(); err != nil {
		t.Fatalf("Set(old full result marker) error = %v", err)
	}
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- manager1.RefreshByKey(ctx, defaultPhysicalKey("fields-wait:1"), "name")
	}()
	<-started
	secondErr := make(chan error, 1)
	go func() {
		secondErr <- manager2.RefreshByKey(ctx, defaultPhysicalKey("fields-wait:1"), "status")
	}()
	select {
	case err := <-secondErr:
		t.Fatalf("second fields RefreshByKey returned before owner released: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-firstErr; err != nil {
		t.Fatalf("first fields RefreshByKey error = %v", err)
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("second fields RefreshByKey error = %v", err)
	}
	if got := client.HGet(ctx, defaultPhysicalKey("fields-wait:1"), "status").Val(); got != "enabled" {
		t.Fatalf("fields-wait:1 status = %q, want enabled", got)
	}
}

// TestManagerHiddenEmptyMarker 验证默认隐藏空值占位不会污染业务 key，但仍能被 Get 识别。
func TestManagerHiddenEmptyMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "hidden",
			Title:            "隐藏空值缓存",
			Key:              "hidden:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, errors.Tag(ErrNotFound)
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("hidden:1")); err != nil {
		t.Fatalf("RefreshByKey(hidden:1) error = %v", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("hidden:1")).Val(); exists != 0 {
		t.Fatalf("hidden:1 exists = %d, want 0", exists)
	}
	if got := client.Get(ctx, manager.emptyKey(defaultPhysicalKey("hidden:1"))).Val(); got != DefaultEmptyMarker {
		t.Fatalf("hidden empty marker = %q, want %q", got, DefaultEmptyMarker)
	}
	var value string
	ok, err := manager.Get(ctx, "hidden:1", &value)
	if err != nil {
		t.Fatalf("Get(hidden:1) error = %v", err)
	}
	if ok {
		t.Fatalf("Get(hidden:1) ok = true, want false")
	}
}

// TestManagerGetState 验证状态化读取可以区分命中、未命中和空值占位。
func TestManagerGetState(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "state",
			Title: "状态缓存",
			Key:   "state:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if len(params.KeyParts) == 0 {
					return nil, errors.Tag(ErrNotFound)
				}
				switch params.KeyParts[0] {
				case "1":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
				case "2":
					return nil, errors.Tag(ErrNotFound)
				default:
					return nil, nil
				}
			},
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("state:1")); err != nil {
		t.Fatalf("RefreshByKey(state:1) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("state:2")); err != nil {
		t.Fatalf("RefreshByKey(state:2) error = %v", err)
	}
	var value string
	result, err := manager.GetState(ctx, "state:1", &value)
	if err != nil {
		t.Fatalf("GetState(state:1) error = %v", err)
	}
	if result.State != LookupStateHit || value != "alpha" {
		t.Fatalf("GetState(state:1) = %+v value=%q, want hit alpha", result, value)
	}
	result, err = manager.GetState(ctx, "state:2", &value)
	if err != nil {
		t.Fatalf("GetState(state:2) error = %v", err)
	}
	if result.State != LookupStateEmpty {
		t.Fatalf("GetState(state:2) = %+v, want empty", result)
	}
	result, err = manager.GetState(ctx, "state:3", &value)
	if err != nil {
		t.Fatalf("GetState(state:3) error = %v", err)
	}
	if result.State != LookupStateMiss {
		t.Fatalf("GetState(state:3) = %+v, want miss", result)
	}
}

// TestManagerGetOrRefreshHitAndEmpty 验证 GetOrRefresh 对命中和空值占位不会重复刷新。
func TestManagerGetOrRefreshHitAndEmpty(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "load",
			Title: "读取即刷新缓存",
			Key:   "load:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				if len(params.KeyParts) == 0 {
					return nil, errors.Tag(ErrNotFound)
				}
				switch params.KeyParts[0] {
				case "1":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
				case "2":
					return nil, errors.Tag(ErrNotFound)
				default:
					return nil, nil
				}
			},
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value string
	result, err := manager.GetOrRefresh(ctx, defaultPhysicalKey("load:1"), &value)
	if err != nil {
		t.Fatalf("GetOrRefresh(load:1) error = %v", err)
	}
	if result.State != LookupStateHit || !result.Refreshed || value != "alpha" {
		t.Fatalf("GetOrRefresh(load:1) = %+v value=%q, want refreshed hit alpha", result, value)
	}
	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls after load:1 = %d, want 1", got)
	}
	result, err = manager.GetOrRefresh(ctx, defaultPhysicalKey("load:1"), &value)
	if err != nil {
		t.Fatalf("GetOrRefresh(load:1) second error = %v", err)
	}
	if result.State != LookupStateHit || result.Refreshed {
		t.Fatalf("GetOrRefresh(load:1) second = %+v, want direct hit", result)
	}
	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls after second load:1 = %d, want 1", got)
	}
	result, err = manager.GetOrRefresh(ctx, defaultPhysicalKey("load:2"), &value)
	if err != nil {
		t.Fatalf("GetOrRefresh(load:2) error = %v", err)
	}
	if result.State != LookupStateEmpty || !result.Refreshed {
		t.Fatalf("GetOrRefresh(load:2) = %+v, want refreshed empty", result)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls after load:2 = %d, want 2", got)
	}
	result, err = manager.GetOrRefresh(ctx, defaultPhysicalKey("load:2"), &value)
	if err != nil {
		t.Fatalf("GetOrRefresh(load:2) second error = %v", err)
	}
	if result.State != LookupStateEmpty || result.Refreshed {
		t.Fatalf("GetOrRefresh(load:2) second = %+v, want direct empty", result)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls after second load:2 = %d, want 2", got)
	}
}

// TestManagerGetOrRefreshWithLoaderAndLoadThrough 验证显式 Loader 的读穿接口可以直接完成回源、回填与返回。
func TestManagerGetOrRefreshWithLoaderAndLoadThrough(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "through",
			Title:            "读穿缓存",
			Key:              "through:",
			Type:             TypeString,
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	loader := func(ctx context.Context, params LoadParams) ([]Entry, error) {
		loaderCalls.Add(1)
		if len(params.KeyParts) == 0 {
			return nil, errors.Tag(ErrNotFound)
		}
		switch params.KeyParts[0] {
		case "1":
			return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
		case "2":
			return nil, errors.Tag(ErrNotFound)
		default:
			return nil, nil
		}
	}
	var value string
	result, err := manager.GetOrRefreshWithLoader(ctx, defaultPhysicalKey("through:1"), &value, loader)
	if err != nil {
		t.Fatalf("GetOrRefreshWithLoader(through:1) error = %v", err)
	}
	if result.State != LookupStateHit || !result.Refreshed || value != "alpha" {
		t.Fatalf("GetOrRefreshWithLoader(through:1) = %+v value=%q, want refreshed hit alpha", result, value)
	}
	result, err = manager.LoadThrough(ctx, defaultPhysicalKey("through:2"), &value, loader)
	if err != nil {
		t.Fatalf("LoadThrough(through:2) error = %v", err)
	}
	if result.State != LookupStateEmpty || !result.Refreshed {
		t.Fatalf("LoadThrough(through:2) = %+v, want refreshed empty", result)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
}

// TestManagerLoadThroughWithOptions 验证完整读穿配置可以透传 fields 并临时覆盖 Loader。
func TestManagerLoadThroughWithOptions(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "detail",
			Title: "明细缓存",
			Key:   "detail:",
			Type:  TypeHash,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				t.Fatal("传入临时 Loader 后不应走 Target.Loader")
				return nil, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var loaderFields []string
	var value map[string]string
	result, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("detail:1"), &value, LoadThroughOptions{
		Fields: []string{" name ", "", "status"},
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			loaderFields = append(loaderFields, params.Fields...)
			return []Entry{{
				Key:  params.Key,
				Type: TypeHash,
				Value: map[string]any{
					"name":   "alpha",
					"status": "enabled",
				},
			}}, nil
		},
	})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions(detail:1) error = %v", err)
	}
	if result.State != LookupStateHit || !result.Refreshed {
		t.Fatalf("LoadThroughWithOptions(detail:1) = %+v, want refreshed hit", result)
	}
	if len(loaderFields) != 2 || loaderFields[0] != "name" || loaderFields[1] != "status" {
		t.Fatalf("loader fields = %#v, want [name status]", loaderFields)
	}
	if value["name"] != "alpha" || value["status"] != "enabled" {
		t.Fatalf("value = %#v, want name/status", value)
	}
}

// TestManagerLoadThroughWithOptionsDisableEmptyMarker 验证单次读穿可关闭空值占位写入。
func TestManagerLoadThroughWithOptionsDisableEmptyMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "empty",
			Title:            "空值缓存",
			Key:              "empty:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, errors.Tag(ErrNotFound)
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value string
	result, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("empty:1"), &value, LoadThroughOptions{
		AllowEmptyMarker: Bool(false),
	})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions(empty:1) error = %v", err)
	}
	if result.State != LookupStateMiss || !result.Refreshed {
		t.Fatalf("LoadThroughWithOptions(empty:1) = %+v, want refreshed miss", result)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("empty:1"), manager.emptyKey(defaultPhysicalKey("empty:1"))).Val(); exists != 0 {
		t.Fatalf("empty:1 related keys exists = %d, want 0", exists)
	}
}

// TestManagerLoadThroughWithOptionsLoaderTimeout 验证单次读穿可覆盖回源超时时间。
func TestManagerLoadThroughWithOptionsLoaderTimeout(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "timeout-through",
			Title: "读穿超时缓存",
			Key:   "timeout-through:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				<-ctx.Done()
				return nil, errors.Tag(ctx.Err())
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value string
	_, err = manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("timeout-through:1"), &value, LoadThroughOptions{
		LoaderTimeout: 20 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LoadThroughWithOptions(timeout-through:1) error = %v, want context deadline exceeded", err)
	}
}

// TestManagerLoadThroughWithOptionsIgnoreCancel 验证单次读穿可忽略调用方取消并完成回源与回读。
func TestManagerLoadThroughWithOptionsIgnoreCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "cancel-through",
			Title: "忽略取消读穿缓存",
			Key:   "cancel-through:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				cancel()
				select {
				case <-ctx.Done():
					return nil, errors.Tag(ctx.Err())
				case <-time.After(20 * time.Millisecond):
				}
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value string
	result, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("cancel-through:1"), &value, LoadThroughOptions{
		ContextPolicy: RebuildPolicy(RebuildContextIgnoreCancel),
	})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions(cancel-through:1) error = %v", err)
	}
	if result.State != LookupStateHit || !result.Refreshed || value != "ok" {
		t.Fatalf("LoadThroughWithOptions(cancel-through:1) = %+v value=%q, want refreshed hit ok", result, value)
	}
}

// TestManagerLoadThroughBatch 验证批量读穿会逐项返回结果且单条失败不影响其它条目。
func TestManagerLoadThroughBatch(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "batch-load",
			Title:            "批量读穿缓存",
			Key:              "batch-load:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				if len(params.KeyParts) == 0 {
					return nil, errors.Tag(ErrNotFound)
				}
				switch params.KeyParts[0] {
				case "1":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
				case "2":
					return nil, errors.Tag(ErrNotFound)
				default:
					return nil, nil
				}
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value1 string
	var value2 string
	var value3 string
	results := manager.LoadThroughBatch(ctx, []LoadThroughItem{
		{Key: defaultPhysicalKey("batch-load:1"), Dest: &value1},
		{Key: defaultPhysicalKey("batch-load:2"), Dest: &value2},
		{Key: defaultPhysicalKey("other:1"), Dest: &value3},
	})
	if len(results) != 3 {
		t.Fatalf("LoadThroughBatch() results len = %d, want 3", len(results))
	}
	if results[0].Error != nil || results[0].LookupResult.State != LookupStateHit || !results[0].LookupResult.Refreshed || value1 != "alpha" {
		t.Fatalf("results[0] = %+v value1=%q, want refreshed hit alpha", results[0], value1)
	}
	if results[1].Error != nil || results[1].LookupResult.State != LookupStateEmpty || !results[1].LookupResult.Refreshed {
		t.Fatalf("results[1] = %+v, want refreshed empty", results[1])
	}
	if !errors.Is(results[2].Error, ErrTargetNotFound) {
		t.Fatalf("results[2].Error = %v, want ErrTargetNotFound", results[2].Error)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
}

// TestManagerLoadThroughBatchWithSummary 验证批量读穿汇总信息和聚合错误符合预期。
func TestManagerLoadThroughBatchWithSummary(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "batch-summary",
			Title:            "批量汇总缓存",
			Key:              "batch-summary:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case defaultPhysicalKey("batch-summary:1"):
					return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
				case defaultPhysicalKey("batch-summary:2"):
					return nil, errors.Tag(ErrNotFound)
				default:
					return nil, nil
				}
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value1 string
	var value2 string
	results, summary, err := manager.LoadThroughBatchWithSummary(ctx, []LoadThroughItem{
		{Key: defaultPhysicalKey("batch-summary:1"), Dest: &value1},
		{Key: defaultPhysicalKey("batch-summary:2"), Dest: &value2},
		{Key: defaultPhysicalKey("other:1")},
	})
	if len(results) != 3 {
		t.Fatalf("LoadThroughBatchWithSummary() results len = %d, want 3", len(results))
	}
	batchErr := &LoadThroughBatchError{}
	if !errors.As(err, &batchErr) {
		t.Fatalf("LoadThroughBatchWithSummary() error = %v, want LoadThroughBatchError", err)
	}
	if summary.Total != 3 || summary.Success != 2 || summary.Failed != 1 {
		t.Fatalf("summary = %+v, want total=3 success=2 failed=1", summary)
	}
	if summary.Hit != 1 || summary.Empty != 1 || summary.Miss != 0 || summary.Refreshed != 2 {
		t.Fatalf("summary = %+v, want hit=1 empty=1 miss=0 refreshed=2", summary)
	}
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != defaultPhysicalKey("other:1") {
		t.Fatalf("summary failed keys = %#v, want other:1", summary.FailedKeys)
	}
}

// TestManagerRefreshByKeysWithSummary 验证批量刷新可返回逐项结果、汇总信息和聚合错误。
func TestManagerRefreshByKeysWithSummary(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "refresh-summary",
			Title: "刷新汇总缓存",
			Key:   "refresh-summary:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	}, WithMetrics(metrics), WithLogger(logger))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	results, summary, err := manager.RefreshByKeysWithSummary(ctx, []string{
		defaultPhysicalKey("refresh-summary:1"),
		defaultPhysicalKey("refresh-summary:1"),
		defaultPhysicalKey("other:1"),
	})
	if len(results) != 2 {
		t.Fatalf("results len = %d, want 2", len(results))
	}
	batchErr := &RefreshBatchError{}
	if !errors.As(err, &batchErr) {
		t.Fatalf("RefreshByKeysWithSummary() error = %v, want RefreshBatchError", err)
	}
	if summary.Total != 2 || summary.Success != 1 || summary.Failed != 1 {
		t.Fatalf("summary = %+v, want total=2 success=1 failed=1", summary)
	}
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != defaultPhysicalKey("other:1") {
		t.Fatalf("summary failed keys = %#v, want other:1", summary.FailedKeys)
	}
	if got := client.Get(ctx, defaultPhysicalKey("refresh-summary:1")).Val(); got != "ok" {
		t.Fatalf("refresh-summary:1 value = %q, want ok", got)
	}
	if got := metrics.refreshBatchCount.Load(); got != 1 {
		t.Fatalf("refreshBatchCount = %d, want 1", got)
	}
	if got := metrics.refreshBatchSuccess.Load(); got != 1 {
		t.Fatalf("refreshBatchSuccess = %d, want 1", got)
	}
	if got := metrics.refreshBatchFailed.Load(); got != 1 {
		t.Fatalf("refreshBatchFailed = %d, want 1", got)
	}
	if !logger.contains("event=\"refresh_batch_done\"") {
		t.Fatalf("refresh batch done log not found, logs=%v", logger.messages())
	}
}

// TestSummarizeLoadThroughBatchResults 验证批量读穿结果汇总函数可正确统计状态。
func TestSummarizeLoadThroughBatchResults(t *testing.T) {
	summary := SummarizeLoadThroughBatchResults([]LoadThroughBatchResult{
		{Key: "demo:1", LookupResult: LookupResult{State: LookupStateHit, Refreshed: true}},
		{Key: "demo:2", LookupResult: LookupResult{State: LookupStateMiss}},
		{Key: "demo:3", LookupResult: LookupResult{State: LookupStateEmpty, Refreshed: true}},
		{Key: "demo:4", Error: ErrTargetNotFound},
	})
	if summary.Total != 4 || summary.Success != 3 || summary.Failed != 1 {
		t.Fatalf("summary = %+v, want total=4 success=3 failed=1", summary)
	}
	if summary.Hit != 1 || summary.Miss != 1 || summary.Empty != 1 || summary.Refreshed != 2 {
		t.Fatalf("summary = %+v, want hit=1 miss=1 empty=1 refreshed=2", summary)
	}
	if !summary.HasError() {
		t.Fatalf("summary.HasError() = false, want true")
	}
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != "demo:4" {
		t.Fatalf("summary failed keys = %#v, want demo:4", summary.FailedKeys)
	}
}

// TestManagerLoadThroughBatchWithBatchOptions 验证批次级默认参数会生效，且条目级参数优先覆盖。
func TestManagerLoadThroughBatchWithBatchOptions(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "batch-options",
			Title:            "批次配置缓存",
			Key:              "batch-options:",
			Type:             TypeHash,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case defaultPhysicalKey("batch-options:1"):
					if len(params.Fields) != 1 || params.Fields[0] != "name" {
						t.Fatalf("params.Fields = %#v, want [name]", params.Fields)
					}
					return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{"name": "alpha"}}}, nil
				case defaultPhysicalKey("batch-options:2"):
					return nil, errors.Tag(ErrNotFound)
				default:
					return nil, nil
				}
			},
		},
	}, WithRefreshConcurrency(1))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value1 map[string]string
	var value2 map[string]string
	results := manager.LoadThroughBatchWithBatchOptions(ctx, []LoadThroughItem{
		{
			Key:  defaultPhysicalKey("batch-options:1"),
			Dest: &value1,
		},
		{
			Key:  defaultPhysicalKey("batch-options:2"),
			Dest: &value2,
			Options: LoadThroughOptions{
				AllowEmptyMarker: Bool(false),
			},
		},
	}, LoadThroughBatchOptions{
		Concurrency: 2,
		DefaultOptions: LoadThroughOptions{
			Fields:           []string{"name"},
			AllowEmptyMarker: Bool(true),
			ContextPolicy:    RebuildPolicy(RebuildContextIgnoreCancel),
		},
	})
	if results[0].Error != nil || results[0].LookupResult.State != LookupStateHit || value1["name"] != "alpha" {
		t.Fatalf("results[0] = %+v value1=%v, want hit alpha", results[0], value1)
	}
	if results[1].Error != nil || results[1].LookupResult.State != LookupStateMiss {
		t.Fatalf("results[1] = %+v, want miss", results[1])
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("batch-options:2"), manager.emptyKey(defaultPhysicalKey("batch-options:2"))).Val(); exists != 0 {
		t.Fatalf("batch-options:2 related keys exists = %d, want 0", exists)
	}
}

// TestManagerLoadThroughBatchRejectsNegativeOptions 验证批量读穿不会静默接受负数超时或并发度。
func TestManagerLoadThroughBatchRejectsNegativeOptions(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "batch-invalid",
		Title: "非法批次配置缓存",
		Key:   "batch-invalid:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			loaderCalls.Add(1)
			return []Entry{{Key: params.Key, Type: TypeString, Value: "unexpected"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	items := []LoadThroughItem{
		{Key: defaultPhysicalKey("batch-invalid:1"), Dest: new(string)},
		{Key: defaultPhysicalKey("batch-invalid:2"), Dest: new(string)},
	}

	t.Run("item loader timeout", func(t *testing.T) {
		itemOptions := append([]LoadThroughItem(nil), items...)
		itemOptions[0].Options.LoaderTimeout = -time.Second
		results := manager.LoadThroughBatchWithBatchOptions(ctx, itemOptions, LoadThroughBatchOptions{})
		if !errors.Is(results[0].Error, ErrInvalidConfig) {
			t.Fatalf("results[0].Error = %v, want ErrInvalidConfig", results[0].Error)
		}
	})

	t.Run("batch concurrency", func(t *testing.T) {
		results := manager.LoadThroughBatchWithBatchOptions(ctx, items, LoadThroughBatchOptions{Concurrency: -1})
		for index, result := range results {
			if !errors.Is(result.Error, ErrInvalidConfig) {
				t.Fatalf("results[%d].Error = %v, want ErrInvalidConfig", index, result.Error)
			}
		}
	})

	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1 for the valid item only", got)
	}
}

// TestManagerLookupMetrics 验证读取链路细分指标可以区分状态并统计读穿刷新触发次数。
func TestManagerLookupMetrics(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:            "metric",
			Title:            "指标缓存",
			Key:              "metric:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case defaultPhysicalKey("metric:1"), defaultPhysicalKey("metric:4"):
					return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
				case defaultPhysicalKey("metric:2"):
					return nil, errors.Tag(ErrNotFound)
				default:
					return nil, nil
				}
			},
		},
	}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("metric:1")); err != nil {
		t.Fatalf("RefreshByKey(metric:1) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("metric:2")); err != nil {
		t.Fatalf("RefreshByKey(metric:2) error = %v", err)
	}
	var value string
	if _, err := manager.GetState(ctx, "metric:1", &value); err != nil {
		t.Fatalf("GetState(metric:1) error = %v", err)
	}
	if _, err := manager.GetState(ctx, "metric:2", &value); err != nil {
		t.Fatalf("GetState(metric:2) error = %v", err)
	}
	if _, err := manager.GetState(ctx, "metric:3", &value); err != nil {
		t.Fatalf("GetState(metric:3) error = %v", err)
	}
	if _, err := manager.GetOrRefresh(ctx, defaultPhysicalKey("metric:4"), &value); err != nil {
		t.Fatalf("GetOrRefresh(metric:4) error = %v", err)
	}
	if got := metrics.lookupHit.Load(); got != 2 {
		t.Fatalf("lookup hit = %d, want 2", got)
	}
	if got := metrics.lookupMiss.Load(); got != 1 {
		t.Fatalf("lookup miss = %d, want 1", got)
	}
	if got := metrics.lookupEmpty.Load(); got != 1 {
		t.Fatalf("lookup empty = %d, want 1", got)
	}
	if got := metrics.lookupRefreshTriggered.Load(); got != 1 {
		t.Fatalf("lookup refresh triggered = %d, want 1", got)
	}
}

// TestManagerLoaderTimeout 验证 Loader 超时配置会终止长时间回源。
func TestManagerLoaderTimeout(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index:         "timeout-loader",
			Title:         "加载超时缓存",
			Key:           "timeout-loader:",
			Type:          TypeString,
			LoaderTimeout: 20 * time.Millisecond,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				<-ctx.Done()
				return nil, errors.Tag(ctx.Err())
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("timeout-loader:1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RefreshByKey(timeout-loader:1) error = %v, want context deadline exceeded", err)
	}
	if exists := client.Exists(ctx, manager.lockKey(defaultPhysicalKey("timeout-loader:1"))).Val(); exists != 0 {
		t.Fatalf("timeout-loader lock exists = %d, want 0", exists)
	}
}

// TestNewManagerRejectsOverlappingPrefixTargets 验证启动时拒绝重叠前缀目标，避免线上匹配歧义。
func TestNewManagerRejectsOverlappingPrefixTargets(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	_, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "user",
			Title: "用户缓存",
			Key:   "user:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, nil
			},
		},
		{
			Index: "profile",
			Title: "用户资料缓存",
			Key:   "user:profile:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, nil
			},
		},
	})
	if err == nil {
		t.Fatalf("newTestManager() error = nil, want overlapping prefix error")
	}
}

// TestNewManagerRejectsFixedPrefixOverlap 验证固定 key 落入前缀目标范围时会在启动期失败，避免 DeleteByPrefix 误删固定目标。
func TestNewManagerRejectsFixedPrefixOverlap(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cases := []struct {
		name    string   // name 是当前子用例名称，用于区分注册顺序。
		targets []Target // targets 是待注册目标列表，覆盖“先前缀后固定”和“先固定后前缀”两种边界。
	}{
		{
			name: "prefix then fixed",
			targets: []Target{
				{Index: "user", Key: "user:", Type: TypeString},
				{Index: "user-profile", Key: "user:profile", Type: TypeString},
			},
		},
		{
			name: "fixed then prefix",
			targets: []Target{
				{Index: "user-profile", Key: "user:profile", Type: TypeString},
				{Index: "user", Key: "user:", Type: TypeString},
			},
		},
	}
	for _, item := range cases {
		item := item
		t.Run(item.name, func(t *testing.T) {
			_, err := newTestManager(NewRedisStore(client), item.targets)
			if err == nil || !strings.Contains(err.Error(), "重叠") {
				t.Fatalf("newTestManager() error = %v, want overlapping target error", err)
			}
		})
	}
}

// TestManagerRejectsInvalidClusterHashTagKeys 验证无法安全生成同槽元信息 key 的异常花括号 key 会被拒绝。
func TestManagerRejectsInvalidClusterHashTagKeys(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	_, err := newTestManager(NewRedisStore(client), []Target{
		{Index: "bad-empty-tag", Key: "bad{}:", Type: TypeString},
	})
	if !errors.Is(err, ErrInvalidClusterHashTag) {
		t.Fatalf("newTestManager(bad{}:) error = %v, want ErrInvalidClusterHashTag", err)
	}
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "brace",
			Key:   "brace:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "bad"}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("newTestManager(brace) error = %v", err)
	}
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("brace:1}"))
	if !errors.Is(err, ErrInvalidClusterHashTag) {
		t.Fatalf("RefreshByKey(brace:1}) error = %v, want ErrInvalidClusterHashTag", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("brace:1}")).Val(); exists != 0 {
		t.Fatalf("brace:1} exists = %d, want 0", exists)
	}
}

// TestRedisStoreRefreshLock 验证锁续期只对当前持有者生效。
func TestRedisStoreRefreshLock(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	locked, _, err := store.AcquireRefreshLock(ctx, "lock:test", "owner-1", time.Second)
	if err != nil || !locked {
		t.Fatalf("AcquireRefreshLock locked=%v err=%v, want locked", locked, err)
	}
	ok, err := store.RefreshLock(ctx, "lock:test", "owner-2", 2*time.Second)
	if err != nil {
		t.Fatalf("RefreshLock wrong owner error = %v", err)
	}
	if ok {
		t.Fatalf("RefreshLock wrong owner ok = true, want false")
	}
	ok, err = store.RefreshLock(ctx, "lock:test", "owner-1", 2*time.Second)
	if err != nil {
		t.Fatalf("RefreshLock owner error = %v", err)
	}
	if !ok {
		t.Fatalf("RefreshLock owner ok = false, want true")
	}
}

// TestJitterDurationBounds 验证 TTL 抖动落在预期范围内。
func TestJitterDurationBounds(t *testing.T) {
	base := 100 * time.Millisecond
	jitter := 10 * time.Millisecond
	for i := 0; i < 100; i++ {
		got := jitterDurationWithDefault(base, jitter, 0.1)
		if got < base || got >= base+jitter {
			t.Fatalf("jitterDurationWithDefault() = %v, want [%v,%v)", got, base, base+jitter)
		}
	}
}

// TestJitterDurationWithDefaultRatio 验证未显式配置 Jitter 时可使用默认比例，也可显式关闭。
func TestJitterDurationWithDefaultRatio(t *testing.T) {
	base := 100 * time.Millisecond
	for i := 0; i < 100; i++ {
		got := jitterDurationWithDefault(base, 0, 0.2)
		if got < base || got >= base+20*time.Millisecond {
			t.Fatalf("jitterDurationWithDefault() = %v, want [%v,%v)", got, base, base+20*time.Millisecond)
		}
	}
	if got := jitterDurationWithDefault(base, 0, 0); got != base {
		t.Fatalf("jitterDurationWithDefault(default disabled) = %v, want %v", got, base)
	}
	got := jitterDurationWithDefault(base, 5*time.Millisecond, 0)
	if got < base || got >= base+5*time.Millisecond {
		t.Fatalf("jitterDurationWithDefault(explicit jitter) = %v, want [%v,%v)", got, base, base+5*time.Millisecond)
	}
}

// TestManagerDefaultWaitDelayBackoff 验证默认等待策略采用递增退避，降低热点轮询压力。
func TestManagerDefaultWaitDelayBackoff(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "wait-backoff",
			Title: "等待退避缓存",
			Key:   "wait-backoff",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, nil
			},
		},
	}, WithLockTTL(time.Second))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	first := manager.waitDelay(Target{}, 0)
	second := manager.waitDelay(Target{}, 1)
	third := manager.waitDelay(Target{}, 2)
	if !(first > 0 && second > first && third >= second) {
		t.Fatalf("wait delays = %v, %v, %v, want increasing", first, second, third)
	}
	if got := manager.waitDelay(Target{}, 16); got != defaultWaitStepMax {
		t.Fatalf("waitDelay capped = %v, want %v", got, defaultWaitStepMax)
	}
}

// TestManagerConfiguredWaitKeepsFixedStep 验证显式 WithWait 配置时继续保持固定步长语义。
func TestManagerConfiguredWaitKeepsFixedStep(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "wait-fixed",
			Title: "固定等待缓存",
			Key:   "wait-fixed",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, nil
			},
		},
	}, WithWait(12*time.Millisecond, 5))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	for attempt := 0; attempt < 4; attempt++ {
		if got := manager.waitDelay(Target{}, attempt); got != 12*time.Millisecond {
			t.Fatalf("waitDelay(attempt=%d) = %v, want 12ms", attempt, got)
		}
	}
}

// BenchmarkManagerWaitDelay 衡量默认等待退避计算开销，确保不会成为热点路径负担。
func BenchmarkManagerWaitDelay(b *testing.B) {
	server := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "bench-wait",
			Title: "等待退避基准",
			Key:   "bench-wait",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, nil
			},
		},
	})
	if err != nil {
		b.Fatalf("newTestManager() error = %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manager.waitDelay(Target{}, i%8)
	}
}

// BenchmarkRefreshReadyName 衡量 fields 签名收敛开销，给热点读穿路径提供基础性能参考。
func BenchmarkRefreshReadyName(b *testing.B) {
	server := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{
		{
			Index: "bench-ready",
			Title: "就绪标识基准",
			Key:   "bench-ready:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, nil
			},
		},
	})
	if err != nil {
		b.Fatalf("newTestManager() error = %v", err)
	}
	fields := []string{"name", "age", "status", "avatar", "department", "updated_at"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manager.refreshReadyName("bench-ready:1", fields)
	}
}

// BenchmarkRedisHotPathHelpers 衡量 TTL 抖动与带锁提交参数构造的基础开销。
func BenchmarkRedisHotPathHelpers(b *testing.B) {
	b.Run("jitter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = jitterDurationWithDefault(time.Minute, 0, 0.1)
		}
	})
	b.Run("single_guard", func(b *testing.B) {
		guards := []LockGuard{{Key: "tcm:{bench}:lock", Owner: "owner"}}
		for i := 0; i < b.N; i++ {
			_, _ = cleanLockGuards(guards)
		}
	})
	b.Run("guarded_list_args", func(b *testing.B) {
		guards := []LockGuard{{Key: "tcm:{bench}:lock", Owner: "owner"}}
		entry := preparedEntry{
			key:       "tc:{bench}:list",
			typ:       TypeList,
			ttl:       time.Minute,
			overwrite: true,
			values:    make([]any, 100),
		}
		for i := 0; i < b.N; i++ {
			_, _ = guardedMutationArgs(guards, &entry)
		}
	})
}

// countingStore 统计刷新锁请求次数，用于验证 singleflight 是否拦截本机并发。
type countingStore struct {
	Store        // 复用底层 RedisStore 的完整 Store 实现
	acquireCalls atomic.Int64
}

// AcquireRefreshLock 统计刷新锁请求次数后再透传到底层 Store。
func (s *countingStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	s.acquireCalls.Add(1)
	return s.Store.AcquireRefreshLock(ctx, key, value, ttl)
}

// lockLostStore 模拟锁续期失败场景。
type lockLostStore struct {
	Store
	refreshLockCalls atomic.Int64
}

// RefreshLock 首次续期直接返回 owner mismatch，模拟实例在刷新过程中失锁。
func (s *lockLostStore) RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	s.refreshLockCalls.Add(1)
	return false, nil
}

// concurrentDeletePatternStore 模拟慢速 pattern 删除存储，用于验证 DeleteByPrefix 的多 pattern 有界并发。
type concurrentDeletePatternStore struct {
	Store                  // Store 是未使用的嵌入接口，仅用于满足测试中除 DeletePattern 外的 Store 方法集
	active    atomic.Int64 // active 表示当前正在执行 DeletePattern 的调用数
	maxActive atomic.Int64 // maxActive 表示测试期间观测到的最大并发 DeletePattern 调用数
	calls     atomic.Int64 // calls 表示 DeletePattern 总调用次数，应覆盖业务 key 与三类内部元信息 pattern
}

// AcquireRefreshLock 模拟原子获取删除互斥锁成功。
func (s *concurrentDeletePatternStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	return true, "", nil
}

// RefreshLock 模拟当前测试持有删除互斥锁。
func (s *concurrentDeletePatternStore) RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	return true, nil
}

// ReleaseLock 模拟释放删除互斥锁。
func (s *concurrentDeletePatternStore) ReleaseLock(ctx context.Context, key string, value string) (bool, error) {
	return true, nil
}

// ApplyMutation 接收带 guard 的删除代际写入，避免测试替身依赖真实 Redis。
func (s *concurrentDeletePatternStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	return nil
}

// DeletePattern 记录并发度并模拟一次较慢的 Redis SCAN 删除任务。
func (s *concurrentDeletePatternStore) DeletePattern(ctx context.Context, pattern string, count int64) (int64, error) {
	current := s.active.Add(1) // current 是进入当前 pattern 删除后的实时并发数，用于更新峰值
	for {
		maxActive := s.maxActive.Load() // maxActive 是已记录的并发峰值，CAS 更新可避免并发写竞争
		if current <= maxActive || s.maxActive.CompareAndSwap(maxActive, current) {
			break
		}
	}
	s.calls.Add(1)
	defer s.active.Add(-1)
	select {
	case <-ctx.Done():
		return 0, errors.Tag(ctx.Err())
	case <-time.After(30 * time.Millisecond):
		// 慢速分支用于给其它 pattern goroutine 留出并发进入窗口，避免测试依赖调度偶然性。
		return 1, nil
	}
}

// DeletePatternGuarded 复用慢速删除实现，测试只关注 Manager 的有界并发调度。
func (s *concurrentDeletePatternStore) DeletePatternGuarded(ctx context.Context, pattern string, count int64, guards []LockGuard) (int64, error) {
	return s.DeletePattern(ctx, pattern, count)
}

// deletePatternCountingStore 包装 RedisStore 并统计 DeletePattern 调用次数，用于验证索引快路径是否绕过全库 SCAN。
type deletePatternCountingStore struct {
	*RedisStore                     // RedisStore 提供真实 Redis 读写、锁与前缀索引能力
	deletePatternCalls atomic.Int64 // deletePatternCalls 表示测试期间 DeletePattern 被调用的次数
}

// DeletePatternGuarded 统计 Manager 实际使用的带锁 SCAN 删除调用。
func (s *deletePatternCountingStore) DeletePatternGuarded(ctx context.Context, pattern string, count int64, guards []LockGuard) (int64, error) {
	s.deletePatternCalls.Add(1)
	return s.RedisStore.DeletePatternGuarded(ctx, pattern, count, guards)
}

// mutationCountingStore 包装 RedisStore 并统计合并变更调用次数。
type mutationCountingStore struct {
	*RedisStore                // RedisStore 提供真实 Redis 写入、索引维护和锁能力
	mutationCalls atomic.Int64 // mutationCalls 表示 ApplyMutation 快路径调用次数
}

// ApplyMutation 统计安全合并变更调用次数后再透传到底层 RedisStore。
func (s *mutationCountingStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	s.mutationCalls.Add(1)
	return s.RedisStore.ApplyMutation(ctx, mutation)
}

// recordingLockStore 记录实际使用的锁 key，便于验证不同刷新入口的锁域是否一致。
type recordingLockStore struct {
	Store
	mu   sync.Mutex
	keys []string
}

// AcquireRefreshLock 记录所有刷新锁 key，再透传到底层 Store。
func (s *recordingLockStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.Store.AcquireRefreshLock(ctx, key, value, ttl)
}

// ReplacePrefix 透传安全前缀替换能力，避免锁记录器改变被测Store契约。
func (s *recordingLockStore) ReplacePrefix(ctx context.Context, mutation PrefixReplaceMutation) (int64, error) {
	store, ok := s.Store.(PrefixReplaceStore)
	if !ok {
		return 0, errors.Tag(ErrPrefixReplaceUnsupported)
	}
	return store.ReplacePrefix(ctx, mutation)
}

// lockKeys 返回当前测试记录到的锁 key 列表副本。
func (s *recordingLockStore) lockKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, len(s.keys))
	copy(keys, s.keys)
	return keys
}

// pipelineCommandRecorder 记录 go-redis pipeline 命令名，用于验证写入命令形态。
type pipelineCommandRecorder struct {
	mu    sync.Mutex
	names []string
}

func (r *pipelineCommandRecorder) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (r *pipelineCommandRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (r *pipelineCommandRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		r.mu.Lock()
		for _, cmd := range cmds {
			name := strings.ToLower(cmd.Name())
			if name == "client" {
				continue
			}
			r.names = append(r.names, name)
		}
		r.mu.Unlock()
		return next(ctx, cmds)
	}
}

func (r *pipelineCommandRecorder) pipelineNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, len(r.names))
	copy(names, r.names)
	return names
}

// recordingMetrics 记录测试中的缓存指标调用次数。
type recordingMetrics struct {
	refresh                atomic.Int64
	hit                    atomic.Int64
	miss                   atomic.Int64
	lock                   atomic.Int64
	prefixWait             atomic.Int64
	prefixRetry            atomic.Int64
	refreshBatchCount      atomic.Int64
	refreshBatchSuccess    atomic.Int64
	refreshBatchFailed     atomic.Int64
	scanFallback           atomic.Int64
	lookupHit              atomic.Int64
	lookupMiss             atomic.Int64
	lookupEmpty            atomic.Int64
	lookupRefreshTriggered atomic.Int64
	refreshEntryCountCalls atomic.Int64
}

// RecordRefresh 记录刷新次数。
func (m *recordingMetrics) RecordRefresh(ctx context.Context, index string, result string, duration time.Duration) {
	m.refresh.Add(1)
}

// RecordCacheHit 记录命中次数。
func (m *recordingMetrics) RecordCacheHit(ctx context.Context, index string) {
	m.hit.Add(1)
}

// RecordCacheMiss 记录未命中次数。
func (m *recordingMetrics) RecordCacheMiss(ctx context.Context, index string) {
	m.miss.Add(1)
}

// RecordLockFailed 记录锁竞争失败次数。
func (m *recordingMetrics) RecordLockFailed(ctx context.Context, index string) {
	m.lock.Add(1)
}

// RecordLoaderError 记录加载器错误次数；测试场景无需额外断言时保留空实现。
func (m *recordingMetrics) RecordLoaderError(ctx context.Context, index string, err error) {
}

// RecordEmptyMarkerWrite 记录空值占位写入次数；测试场景无需额外断言时保留空实现。
func (m *recordingMetrics) RecordEmptyMarkerWrite(ctx context.Context, index string) {
}

// RecordWaitTimeout 记录等待超时次数；测试场景无需额外断言时保留空实现。
func (m *recordingMetrics) RecordWaitTimeout(ctx context.Context, index string) {
}

// RecordPrefixWait 记录前缀等待次数。
func (m *recordingMetrics) RecordPrefixWait(ctx context.Context, index string) {
	m.prefixWait.Add(1)
}

// RecordPrefixRetry 记录前缀重试次数。
func (m *recordingMetrics) RecordPrefixRetry(ctx context.Context, index string) {
	m.prefixRetry.Add(1)
}

// RecordRefreshBatch 记录批量刷新与全量刷新任务的汇总信息。
func (m *recordingMetrics) RecordRefreshBatch(ctx context.Context, mode string, result string, total int, success int, failed int) {
	m.refreshBatchCount.Add(1)
	m.refreshBatchSuccess.Add(int64(success))
	m.refreshBatchFailed.Add(int64(failed))
}

// RecordPrefixDelete 记录前缀删除次数；测试场景无需额外断言时保留空实现。
func (m *recordingMetrics) RecordPrefixDelete(ctx context.Context, index string, prefix string, count int64) {
}

// RecordScanFallback 记录前缀删除降级扫描次数。
func (m *recordingMetrics) RecordScanFallback(ctx context.Context, index string, prefix string) {
	m.scanFallback.Add(1)
}

// RecordRefreshEntryCount 记录刷新条数指标调用次数。
func (m *recordingMetrics) RecordRefreshEntryCount(ctx context.Context, index string, count int) {
	m.refreshEntryCountCalls.Add(1)
}

// RecordLookupState 记录读取链路命中状态细分指标。
func (m *recordingMetrics) RecordLookupState(ctx context.Context, index string, state LookupState) {
	switch state {
	case LookupStateHit:
		m.lookupHit.Add(1)
	case LookupStateMiss:
		m.lookupMiss.Add(1)
	case LookupStateEmpty:
		m.lookupEmpty.Add(1)
	}
}

// RecordLookupRefreshTriggered 记录读取 miss 后触发回源刷新的次数。
func (m *recordingMetrics) RecordLookupRefreshTriggered(ctx context.Context, index string) {
	m.lookupRefreshTriggered.Add(1)
}

// TestManagerWithGoUtilsLoggerOption 验证显式 go-utils 日志接入入口可正常工作。
func TestManagerWithGoUtilsLoggerOption(t *testing.T) {
	logger := &recordingUtilsLogger{}
	manager, err := newTestManager(NewRedisStore(redis.NewClient(&redis.Options{Addr: runStandaloneRedis(t).Addr()})), nil, WithGoUtilsLogger(logger))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	manager.logInfoEvent("go_utils_logger_option", "user", "user:2", "result", "success")
	if !logger.contains("go_utils_logger_option") {
		t.Fatalf("utils logger option message not found, logs=%v", logger.messages())
	}
}

// testPrefixIndexContains 检查分片 ZSet 是否包含指定业务 key。
func testPrefixIndexContains(ctx context.Context, client redis.UniversalClient, indexKey string, key string) bool {
	shardKey := prefixIndexShardKey(indexKey, prefixIndexMemberShard(key))
	return client.ZScore(ctx, shardKey, key).Err() == nil
}

// recordingLogger 记录测试中的结构化日志内容。
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

// recordingUtilsLogger 记录 go-utils.Logger 风格日志内容。
type recordingUtilsLogger struct {
	mu    sync.Mutex
	lines []string
}

// Infof 记录信息日志。
func (l *recordingLogger) Infof(format string, args ...any) {
	l.append(format, args...)
}

// Warnf 记录警告日志。
func (l *recordingLogger) Warnf(format string, args ...any) {
	l.append(format, args...)
}

// contains 判断当前日志是否包含指定片段。
func (l *recordingLogger) contains(keyword string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, keyword) {
			return true
		}
	}
	return false
}

// messages 返回当前测试收集到的日志副本。
func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	lines := make([]string, len(l.lines))
	copy(lines, l.lines)
	return lines
}

// append 统一写入一条格式化后的日志。
func (l *recordingLogger) append(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

// Debug 记录调试日志。
func (l *recordingUtilsLogger) Debug(msg string, args ...any) {
	l.append(msg, args...)
}

// Info 记录信息日志。
func (l *recordingUtilsLogger) Info(msg string, args ...any) {
	l.append(msg, args...)
}

// Warn 记录警告日志。
func (l *recordingUtilsLogger) Warn(msg string, args ...any) {
	l.append(msg, args...)
}

// Error 记录错误日志。
func (l *recordingUtilsLogger) Error(msg string, args ...any) {
	l.append(msg, args...)
}

// With 返回附带上下文字段的子 logger；测试场景直接复用同一记录器。
func (l *recordingUtilsLogger) With(args ...any) utils.Logger {
	return l
}

// Enabled 表示所有日志级别都可输出。
func (l *recordingUtilsLogger) Enabled(ctx context.Context, level utils.LogLevel) bool {
	return true
}

// contains 判断当前日志是否包含指定片段。
func (l *recordingUtilsLogger) contains(keyword string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, keyword) {
			return true
		}
	}
	return false
}

// messages 返回当前测试收集到的日志副本。
func (l *recordingUtilsLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	lines := make([]string, len(l.lines))
	copy(lines, l.lines)
	return lines
}

// append 统一写入一条日志。
func (l *recordingUtilsLogger) append(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(args) == 0 {
		l.lines = append(l.lines, msg)
		return
	}
	l.lines = append(l.lines, fmt.Sprintf("%s %v", msg, args))
}
