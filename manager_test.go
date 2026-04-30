package tablecache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestManagerRefreshByKey 验证指定 key 刷新、TTL 抖动和空值占位能力。
func TestManagerRefreshByKey(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "demo",
			Title:            "演示缓存",
			Key:              "demo:",
			KeyTitle:         "demo:{id}",
			Type:             TypeHash,
			TTL:              10 * time.Minute,
			AllowEmptyMarker: true,
			VisibleEmptyMark: true,
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
		t.Fatalf("NewManager() error = %v", err)
	}

	if err := manager.RefreshByKey(ctx, "demo:1"); err != nil {
		t.Fatalf("RefreshByKey(demo:1) error = %v", err)
	}
	if got := client.HGet(ctx, "demo:1", "name").Val(); got != "alpha" {
		t.Fatalf("demo:1 name = %q, want alpha", got)
	}
	if ttl := client.TTL(ctx, "demo:1").Val(); ttl <= 0 {
		t.Fatalf("demo:1 ttl = %v, want positive", ttl)
	}

	if err := manager.RefreshByKey(ctx, "demo:404"); err != nil {
		t.Fatalf("RefreshByKey(demo:404) error = %v", err)
	}
	if got := client.HGet(ctx, "demo:404", "value").Val(); got != DefaultEmptyMarker {
		t.Fatalf("demo:404 empty marker = %q, want %q", got, DefaultEmptyMarker)
	}
}

// TestManagerRefreshAll 验证批量刷新只处理允许全量刷新的固定缓存目标。
func TestManagerRefreshAll(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	called := 0
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshAll(ctx); err != nil {
		t.Fatalf("RefreshAll() error = %v", err)
	}
	if called != 1 {
		t.Fatalf("loader called = %d, want 1", called)
	}
	if got := client.Get(ctx, "fixed").Val(); got != "ok" {
		t.Fatalf("fixed value = %q, want ok", got)
	}
	if exists := client.Exists(ctx, "detail:1").Val(); exists != 0 {
		t.Fatalf("detail:1 exists = %d, want 0", exists)
	}
}

// TestManagerRefreshAllWithSummary 验证全量刷新可返回逐项结果、汇总信息和聚合错误。
func TestManagerRefreshAllWithSummary(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var fixedCalls atomic.Int64
	var failCalls atomic.Int64
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
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
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != "fixed-fail" {
		t.Fatalf("summary failed keys = %#v, want fixed-fail", summary.FailedKeys)
	}
	if got := fixedCalls.Load(); got != 1 {
		t.Fatalf("fixedCalls = %d, want 1", got)
	}
	if got := failCalls.Load(); got != 1 {
		t.Fatalf("failCalls = %d, want 1", got)
	}
	if got := client.Get(ctx, "fixed-ok").Val(); got != "ok" {
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

// TestManagerZSetEmptyMarker 验证 ZSet 空值占位保持 Redis 类型一致，避免业务读取 WRONGTYPE。
func TestManagerZSetEmptyMarker(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "rank",
			Title:            "排行榜缓存",
			Key:              "rank:",
			Type:             TypeZSet,
			AllowEmptyMarker: true,
			VisibleEmptyMark: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "rank:404"); err != nil {
		t.Fatalf("RefreshByKey(rank:404) error = %v", err)
	}
	if typ := client.Type(ctx, "rank:404").Val(); typ != string(TypeZSet) {
		t.Fatalf("rank:404 type = %q, want zset", typ)
	}
	if score := client.ZScore(ctx, "rank:404", DefaultEmptyMarker).Val(); score != 0 {
		t.Fatalf("rank:404 empty score = %v, want 0", score)
	}
}

// TestManagerWaitRebuildTimeout 验证等待其它实例重建超时时返回明确错误。
func TestManagerWaitRebuildTimeout(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := client.Set(ctx, manager.lockKey("timeout"), "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock error = %v", err)
	}
	err = manager.RefreshByKey(ctx, "timeout")
	if !errors.Is(err, ErrWaitRebuildTimeout) {
		t.Fatalf("RefreshByKey(timeout) error = %v, want ErrWaitRebuildTimeout", err)
	}
}

// TestManagerReleaseLockSafely 验证锁过期后不会误删其它实例新抢到的锁。
func TestManagerReleaseLockSafely(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var manager *Manager
	var err error
	manager, err = NewManager(NewRedisStore(client), []Target{
		{
			Index: "safe",
			Title: "安全锁缓存",
			Key:   "safe:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				// 模拟当前持锁实例加载耗时超过锁 TTL，随后其它实例重新持有同一把锁。
				server.FastForward(2 * time.Second)
				if err := client.Set(ctx, manager.lockKey(params.Key), "other-owner", time.Minute).Err(); err != nil {
					return nil, err
				}
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	}, WithLockTTL(time.Second))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "safe:1"); err != nil {
		t.Fatalf("RefreshByKey(safe:1) error = %v", err)
	}
	if got := client.Get(ctx, manager.lockKey("safe:1")).Val(); got != "other-owner" {
		t.Fatalf("lock owner = %q, want other-owner", got)
	}
}

// TestManagerRefreshStopsAfterLockLost 验证锁续期失败后会立即中止刷新并禁止后续写回。
func TestManagerRefreshStopsAfterLockLost(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &lockLostStore{Store: NewRedisStore(client)}
	logger := &recordingLogger{}
	manager, err := NewManager(store, []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	err = manager.RefreshByKey(ctx, "lock-lost")
	if !errors.Is(err, ErrRefreshLockLost) {
		t.Fatalf("RefreshByKey(lock-lost) error = %v, want ErrRefreshLockLost", err)
	}
	if exists := client.Exists(ctx, "lock-lost").Val(); exists != 0 {
		t.Fatalf("lock-lost exists = %d, want 0", exists)
	}
	if got := store.writeBatchCalls.Load(); got != 0 {
		t.Fatalf("WriteBatch calls = %d, want 0", got)
	}
	if !logger.contains("event=\"lock_lost\"") {
		t.Fatalf("lock lost log not found, logs=%v", logger.messages())
	}
}

// TestManagerPrefixRefreshUsesLayeredLock 验证前缀全量刷新与单 key 刷新采用分层锁域。
func TestManagerPrefixRefreshUsesLayeredLock(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &recordingLockStore{Store: NewRedisStore(client)}
	manager, err := NewManager(store, []Target{
		{
			Index: "prefix-lock",
			Title: "前缀锁缓存",
			Key:   "prefix-lock:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if params.Key == "prefix-lock:1" {
					return []Entry{{Key: params.Key, Type: TypeString, Value: "one"}}, nil
				}
				return nil, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "prefix-lock:1"); err != nil {
		t.Fatalf("RefreshByKey(prefix-lock:1) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "prefix-lock:2"); err != nil {
		t.Fatalf("RefreshByKey(prefix-lock:2) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "prefix-lock:"); err != nil {
		t.Fatalf("RefreshByKey(prefix-lock:) error = %v", err)
	}
	lockKeys := store.lockKeys()
	if len(lockKeys) != 3 {
		t.Fatalf("SetNX lock keys len = %d, want 3", len(lockKeys))
	}
	if lockKeys[0] != manager.lockKey("prefix-lock:1") {
		t.Fatalf("lock key[0] = %q, want %q", lockKeys[0], manager.lockKey("prefix-lock:1"))
	}
	if lockKeys[1] != manager.lockKey("prefix-lock:2") {
		t.Fatalf("lock key[1] = %q, want %q", lockKeys[1], manager.lockKey("prefix-lock:2"))
	}
	if lockKeys[2] != manager.lockKey("prefix-lock:") {
		t.Fatalf("lock key[2] = %q, want %q", lockKeys[2], manager.lockKey("prefix-lock:"))
	}
}

// TestManagerPrefixSingleKeyWaitsFullRefresh 验证单 key 刷新在同前缀全量刷新期间会等待锁释放后再执行。
func TestManagerPrefixSingleKeyWaitsFullRefresh(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := client.Set(ctx, manager.lockKey("prefix-wait:"), "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set prefix lock error = %v", err)
	}
	go func() {
		time.Sleep(60 * time.Millisecond)
		_ = client.Del(ctx, manager.lockKey("prefix-wait:")).Err()
	}()
	startedAt := time.Now()
	if err := manager.RefreshByKey(ctx, "prefix-wait:1"); err != nil {
		t.Fatalf("RefreshByKey(prefix-wait:1) error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < 60*time.Millisecond {
		t.Fatalf("RefreshByKey(prefix-wait:1) elapsed = %v, want >= 60ms", elapsed)
	}
	if got := client.Get(ctx, "prefix-wait:1").Val(); got != "ok" {
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	firstStarted := make(chan struct{}, 1)
	firstReleased := make(chan struct{})
	var loaderCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- manager.RefreshByKey(ctx, "prefix-retry:1")
	}()
	<-firstStarted
	if err := client.Set(ctx, manager.lockKey("prefix-retry:"), "full-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set prefix lock error = %v", err)
	}
	close(firstReleased)
	time.Sleep(60 * time.Millisecond)
	if err := client.Del(ctx, manager.lockKey("prefix-retry:")).Err(); err != nil {
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	firstStarted := make(chan struct{}, 1)
	firstReleased := make(chan struct{})
	var singleCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "prefix-epoch",
			Title: "前缀代际缓存",
			Key:   "prefix-epoch:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case "prefix-epoch:1":
					call := singleCalls.Add(1)
					if call == 1 {
						firstStarted <- struct{}{}
						<-firstReleased
						return []Entry{{Key: params.Key, Type: TypeString, Value: "stale"}}, nil
					}
					return []Entry{{Key: params.Key, Type: TypeString, Value: "fresh"}}, nil
				case "prefix-epoch:":
					return []Entry{{Key: "prefix-epoch:1", Type: TypeString, Value: "full-refresh"}}, nil
				default:
					return nil, nil
				}
			},
		},
	}, WithWait(20*time.Millisecond, 20), WithMetrics(metrics), WithLogger(logger))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- manager.RefreshByKey(ctx, "prefix-epoch:1")
	}()
	<-firstStarted
	if err := manager.RefreshByKey(ctx, "prefix-epoch:"); err != nil {
		t.Fatalf("RefreshByKey(prefix-epoch:) error = %v", err)
	}
	close(firstReleased)
	if err := <-done; err != nil {
		t.Fatalf("RefreshByKey(prefix-epoch:1) error = %v", err)
	}
	if got := singleCalls.Load(); got != 2 {
		t.Fatalf("single loader calls = %d, want 2", got)
	}
	if got := client.Get(ctx, "prefix-epoch:1").Val(); got != "fresh" {
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := client.Set(ctx, manager.lockKey("wait-adaptive"), "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock error = %v", err)
	}
	go func() {
		time.Sleep(260 * time.Millisecond)
		_ = client.Set(ctx, manager.rebuildResultKey("wait-adaptive"), "done", time.Minute).Err()
	}()
	if err := manager.RefreshByKey(ctx, "wait-adaptive"); err != nil {
		t.Fatalf("RefreshByKey(wait-adaptive) error = %v, want nil", err)
	}
}

// TestManagerSingleflight 验证同进程内同一个 key 的并发刷新只打到 Redis 一次。
func TestManagerSingleflight(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &countingStore{Store: NewRedisStore(client)}
	var loaderCalls atomic.Int64
	manager, err := NewManager(store, []Target{
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
		t.Fatalf("NewManager() error = %v", err)
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
			errs <- manager.RefreshByKey(ctx, "sf:1")
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
	if got := store.setNXCalls.Load(); got != 1 {
		t.Fatalf("SetNX calls = %d, want 1", got)
	}
}

// TestRedisStoreWriteBatch 验证批量写入可以一次提交多种缓存结构。
func TestRedisStoreWriteBatch(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	err := store.WriteBatch(ctx, []Entry{
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "profile:1"); err != nil {
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "missing",
			Title:            "不存在缓存",
			Key:              "missing:",
			Type:             TypeHash,
			AllowEmptyMarker: true,
			VisibleEmptyMark: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, ErrNotFound
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "missing:1"); err != nil {
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
	if got := client.HGet(ctx, "missing:1", "value").Val(); got != DefaultEmptyMarker {
		t.Fatalf("empty marker = %q, want %q", got, DefaultEmptyMarker)
	}
}

// TestManagerDeleteByKeyAndPrefix 验证精确删除和前缀删除语义分离。
func TestManagerDeleteByKeyAndPrefix(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "demo",
			Title: "演示缓存",
			Key:   "demo:",
			Type:  TypeString,
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := client.MSet(ctx,
		"demo:", "root",
		"demo:1", "one",
		"demo:2", "two",
		manager.emptyKey("demo:1"), DefaultEmptyMarker,
		manager.rebuildResultKey("demo:1"), "done",
		manager.emptyKey("demo:2"), DefaultEmptyMarker,
		manager.rebuildResultKey("demo:2"), "done",
	).Err(); err != nil {
		t.Fatalf("MSet error = %v", err)
	}
	if err := manager.DeleteByKey(ctx, "demo:1"); err != nil {
		t.Fatalf("DeleteByKey(demo:1) error = %v", err)
	}
	if exists := client.Exists(ctx, "demo:1", manager.emptyKey("demo:1"), manager.rebuildResultKey("demo:1")).Val(); exists != 0 {
		t.Fatalf("demo:1 related keys exists = %d, want 0", exists)
	}
	if exists := client.Exists(ctx, "demo:", "demo:2", manager.emptyKey("demo:2"), manager.rebuildResultKey("demo:2")).Val(); exists != 4 {
		t.Fatalf("demo root/demo:2 related keys exists = %d, want 4", exists)
	}
	if err := manager.DeleteByPrefix(ctx, "demo:"); err != nil {
		t.Fatalf("DeleteByPrefix(demo:) error = %v", err)
	}
	if exists := client.Exists(ctx, "demo:", "demo:2", manager.emptyKey("demo:2"), manager.rebuildResultKey("demo:2")).Val(); exists != 0 {
		t.Fatalf("demo prefix related keys exists = %d, want 0", exists)
	}
}

// TestManagerDeleteRejectsUnregisteredKey 验证删除能力只允许操作已注册缓存目标。
func TestManagerDeleteRejectsUnregisteredKey(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "demo",
			Title: "演示缓存",
			Key:   "demo:",
			Type:  TypeString,
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	err = manager.DeleteByKey(ctx, "other:1")
	if !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("DeleteByKey(other:1) error = %v, want ErrTargetNotFound", err)
	}
	err = manager.DeleteByPrefix(ctx, "other:")
	if !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("DeleteByPrefix(other:) error = %v, want ErrTargetNotFound", err)
	}
}

// TestRedisStoreOverwriteFlag 验证 Overwrite=false 时保留旧数据并执行增量写入。
func TestRedisStoreOverwriteFlag(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	if err := store.Write(ctx, Entry{Key: "merge:hash", Type: TypeHash, Value: map[string]any{"a": "1"}}); err != nil {
		t.Fatalf("Write initial hash error = %v", err)
	}
	if err := store.Write(ctx, Entry{Key: "merge:hash", Type: TypeHash, Value: map[string]any{"b": "2"}, Overwrite: Bool(false)}); err != nil {
		t.Fatalf("Write merge hash error = %v", err)
	}
	if got := client.HGetAll(ctx, "merge:hash").Val(); len(got) != 2 || got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("merged hash = %#v, want a+b", got)
	}
	if err := store.Write(ctx, Entry{Key: "merge:hash", Type: TypeHash, Value: map[string]any{"c": "3"}}); err != nil {
		t.Fatalf("Write overwrite hash error = %v", err)
	}
	if got := client.HGetAll(ctx, "merge:hash").Val(); len(got) != 1 || got["c"] != "3" {
		t.Fatalf("overwritten hash = %#v, want only c", got)
	}
}

// TestManagerRefreshByKeys 验证批量刷新会去重并刷新每个有效 key。
func TestManagerRefreshByKeys(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKeys(ctx, []string{"multi:1", "multi:1", "", "multi:2"}); err != nil {
		t.Fatalf("RefreshByKeys() error = %v", err)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
	if got := client.Get(ctx, "multi:2").Val(); got != "multi:2" {
		t.Fatalf("multi:2 = %q, want multi:2", got)
	}
}

// TestManagerWaitRebuiltByResultMarker 验证空结果刷新也能通过重建结果元信息通知等待方完成。
func TestManagerWaitRebuiltByResultMarker(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := client.Set(ctx, manager.lockKey("result"), "other-owner", time.Minute).Err(); err != nil {
		t.Fatalf("set lock error = %v", err)
	}
	go func() {
		time.Sleep(15 * time.Millisecond)
		_ = client.Set(ctx, manager.rebuildResultKey("result"), "done", time.Minute).Err()
	}()
	if err := manager.RefreshByKey(ctx, "result"); err != nil {
		t.Fatalf("RefreshByKey(result) error = %v, want nil", err)
	}
}

// TestManagerHiddenEmptyMarker 验证默认隐藏空值占位不会污染业务 key，但仍能被 Get 识别。
func TestManagerHiddenEmptyMarker(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "hidden",
			Title:            "隐藏空值缓存",
			Key:              "hidden:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, ErrNotFound
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "hidden:1"); err != nil {
		t.Fatalf("RefreshByKey(hidden:1) error = %v", err)
	}
	if exists := client.Exists(ctx, "hidden:1").Val(); exists != 0 {
		t.Fatalf("hidden:1 exists = %d, want 0", exists)
	}
	if got := client.Get(ctx, manager.emptyKey("hidden:1")).Val(); got != DefaultEmptyMarker {
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "state",
			Title: "状态缓存",
			Key:   "state:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if len(params.KeyParts) == 0 {
					return nil, ErrNotFound
				}
				switch params.KeyParts[0] {
				case "1":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
				case "2":
					return nil, ErrNotFound
				default:
					return nil, nil
				}
			},
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "state:1"); err != nil {
		t.Fatalf("RefreshByKey(state:1) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "state:2"); err != nil {
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "load",
			Title: "读取即刷新缓存",
			Key:   "load:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				if len(params.KeyParts) == 0 {
					return nil, ErrNotFound
				}
				switch params.KeyParts[0] {
				case "1":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
				case "2":
					return nil, ErrNotFound
				default:
					return nil, nil
				}
			},
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value string
	result, err := manager.GetOrRefresh(ctx, "load:1", &value)
	if err != nil {
		t.Fatalf("GetOrRefresh(load:1) error = %v", err)
	}
	if result.State != LookupStateHit || !result.Refreshed || value != "alpha" {
		t.Fatalf("GetOrRefresh(load:1) = %+v value=%q, want refreshed hit alpha", result, value)
	}
	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls after load:1 = %d, want 1", got)
	}
	result, err = manager.GetOrRefresh(ctx, "load:1", &value)
	if err != nil {
		t.Fatalf("GetOrRefresh(load:1) second error = %v", err)
	}
	if result.State != LookupStateHit || result.Refreshed {
		t.Fatalf("GetOrRefresh(load:1) second = %+v, want direct hit", result)
	}
	if got := loaderCalls.Load(); got != 1 {
		t.Fatalf("loader calls after second load:1 = %d, want 1", got)
	}
	result, err = manager.GetOrRefresh(ctx, "load:2", &value)
	if err != nil {
		t.Fatalf("GetOrRefresh(load:2) error = %v", err)
	}
	if result.State != LookupStateEmpty || !result.Refreshed {
		t.Fatalf("GetOrRefresh(load:2) = %+v, want refreshed empty", result)
	}
	if got := loaderCalls.Load(); got != 2 {
		t.Fatalf("loader calls after load:2 = %d, want 2", got)
	}
	result, err = manager.GetOrRefresh(ctx, "load:2", &value)
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "through",
			Title:            "读穿缓存",
			Key:              "through:",
			Type:             TypeString,
			AllowEmptyMarker: true,
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	loader := func(ctx context.Context, params LoadParams) ([]Entry, error) {
		loaderCalls.Add(1)
		if len(params.KeyParts) == 0 {
			return nil, ErrNotFound
		}
		switch params.KeyParts[0] {
		case "1":
			return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
		case "2":
			return nil, ErrNotFound
		default:
			return nil, nil
		}
	}
	var value string
	result, err := manager.GetOrRefreshWithLoader(ctx, "through:1", &value, loader)
	if err != nil {
		t.Fatalf("GetOrRefreshWithLoader(through:1) error = %v", err)
	}
	if result.State != LookupStateHit || !result.Refreshed || value != "alpha" {
		t.Fatalf("GetOrRefreshWithLoader(through:1) = %+v value=%q, want refreshed hit alpha", result, value)
	}
	result, err = manager.LoadThrough(ctx, "through:2", &value, loader)
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	var loaderFields []string
	var value map[string]string
	result, err := manager.LoadThroughWithOptions(ctx, "detail:1", &value, LoadThroughOptions{
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "empty",
			Title:            "空值缓存",
			Key:              "empty:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				return nil, ErrNotFound
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value string
	result, err := manager.LoadThroughWithOptions(ctx, "empty:1", &value, LoadThroughOptions{
		AllowEmptyMarker: Bool(false),
	})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions(empty:1) error = %v", err)
	}
	if result.State != LookupStateMiss || !result.Refreshed {
		t.Fatalf("LoadThroughWithOptions(empty:1) = %+v, want refreshed miss", result)
	}
	if exists := client.Exists(ctx, "empty:1", manager.emptyKey("empty:1")).Val(); exists != 0 {
		t.Fatalf("empty:1 related keys exists = %d, want 0", exists)
	}
}

// TestManagerLoadThroughWithOptionsLoaderTimeout 验证单次读穿可覆盖回源超时时间。
func TestManagerLoadThroughWithOptionsLoaderTimeout(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "timeout-through",
			Title: "读穿超时缓存",
			Key:   "timeout-through:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value string
	_, err = manager.LoadThroughWithOptions(ctx, "timeout-through:1", &value, LoadThroughOptions{
		LoaderTimeout: 20 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LoadThroughWithOptions(timeout-through:1) error = %v, want context deadline exceeded", err)
	}
}

// TestManagerLoadThroughWithOptionsIgnoreCancel 验证单次读穿可忽略调用方取消并完成回源与回读。
func TestManagerLoadThroughWithOptionsIgnoreCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index: "cancel-through",
			Title: "忽略取消读穿缓存",
			Key:   "cancel-through:",
			Type:  TypeString,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				cancel()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value string
	result, err := manager.LoadThroughWithOptions(ctx, "cancel-through:1", &value, LoadThroughOptions{
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "batch-load",
			Title:            "批量读穿缓存",
			Key:              "batch-load:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				loaderCalls.Add(1)
				if len(params.KeyParts) == 0 {
					return nil, ErrNotFound
				}
				switch params.KeyParts[0] {
				case "1":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
				case "2":
					return nil, ErrNotFound
				default:
					return nil, nil
				}
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value1 string
	var value2 string
	var value3 string
	results := manager.LoadThroughBatch(ctx, []LoadThroughItem{
		{Key: "batch-load:1", Dest: &value1},
		{Key: "batch-load:2", Dest: &value2},
		{Key: "other:1", Dest: &value3},
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "batch-summary",
			Title:            "批量汇总缓存",
			Key:              "batch-summary:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case "batch-summary:1":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
				case "batch-summary:2":
					return nil, ErrNotFound
				default:
					return nil, nil
				}
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value1 string
	var value2 string
	results, summary, err := manager.LoadThroughBatchWithSummary(ctx, []LoadThroughItem{
		{Key: "batch-summary:1", Dest: &value1},
		{Key: "batch-summary:2", Dest: &value2},
		{Key: "other:1"},
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
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != "other:1" {
		t.Fatalf("summary failed keys = %#v, want other:1", summary.FailedKeys)
	}
}

// TestManagerRefreshByKeysWithSummary 验证批量刷新可返回逐项结果、汇总信息和聚合错误。
func TestManagerRefreshByKeysWithSummary(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	logger := &recordingLogger{}
	manager, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = %v", err)
	}
	results, summary, err := manager.RefreshByKeysWithSummary(ctx, []string{
		"refresh-summary:1",
		"refresh-summary:1",
		"other:1",
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
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != "other:1" {
		t.Fatalf("summary failed keys = %#v, want other:1", summary.FailedKeys)
	}
	if got := client.Get(ctx, "refresh-summary:1").Val(); got != "ok" {
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "batch-options",
			Title:            "批次配置缓存",
			Key:              "batch-options:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case "batch-options:1":
					if len(params.Fields) != 1 || params.Fields[0] != "name" {
						t.Fatalf("params.Fields = %#v, want [name]", params.Fields)
					}
					return []Entry{{Key: params.Key, Type: TypeString, Value: "alpha"}}, nil
				case "batch-options:2":
					return nil, ErrNotFound
				default:
					return nil, nil
				}
			},
		},
	}, WithRefreshConcurrency(1))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	var value1 string
	var value2 string
	results := manager.LoadThroughBatchWithBatchOptions(ctx, []LoadThroughItem{
		{
			Key:  "batch-options:1",
			Dest: &value1,
		},
		{
			Key:  "batch-options:2",
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
	if results[0].Error != nil || results[0].LookupResult.State != LookupStateHit || value1 != "alpha" {
		t.Fatalf("results[0] = %+v value1=%q, want hit alpha", results[0], value1)
	}
	if results[1].Error != nil || results[1].LookupResult.State != LookupStateMiss {
		t.Fatalf("results[1] = %+v, want miss", results[1])
	}
	if exists := client.Exists(ctx, "batch-options:2", manager.emptyKey("batch-options:2")).Val(); exists != 0 {
		t.Fatalf("batch-options:2 related keys exists = %d, want 0", exists)
	}
}

// TestManagerLookupMetrics 验证读取链路细分指标可以区分状态并统计读穿刷新触发次数。
func TestManagerLookupMetrics(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:            "metric",
			Title:            "指标缓存",
			Key:              "metric:",
			Type:             TypeString,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				switch params.Key {
				case "metric:1", "metric:4":
					return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
				case "metric:2":
					return nil, ErrNotFound
				default:
					return nil, nil
				}
			},
		},
	}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "metric:1"); err != nil {
		t.Fatalf("RefreshByKey(metric:1) error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, "metric:2"); err != nil {
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
	if _, err := manager.GetOrRefresh(ctx, "metric:4", &value); err != nil {
		t.Fatalf("GetOrRefresh(metric:4) error = %v", err)
	}
	if got := metrics.lookupHit.Load(); got != 2 {
		t.Fatalf("lookup hit = %d, want 2", got)
	}
	if got := metrics.lookupMiss.Load(); got != 2 {
		t.Fatalf("lookup miss = %d, want 2", got)
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
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := NewManager(NewRedisStore(client), []Target{
		{
			Index:         "timeout-loader",
			Title:         "加载超时缓存",
			Key:           "timeout-loader:",
			Type:          TypeString,
			LoaderTimeout: 20 * time.Millisecond,
			Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	err = manager.RefreshByKey(ctx, "timeout-loader:1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RefreshByKey(timeout-loader:1) error = %v, want context deadline exceeded", err)
	}
}

// TestNewManagerRejectsOverlappingPrefixTargets 验证启动时拒绝重叠前缀目标，避免线上匹配歧义。
func TestNewManagerRejectsOverlappingPrefixTargets(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	_, err := NewManager(NewRedisStore(client), []Target{
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
		t.Fatalf("NewManager() error = nil, want overlapping prefix error")
	}
}

// TestRedisStoreRefreshLock 验证锁续期只对当前持有者生效。
func TestRedisStoreRefreshLock(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	locked, err := store.SetNX(ctx, "lock:test", "owner-1", time.Second)
	if err != nil || !locked {
		t.Fatalf("SetNX locked=%v err=%v, want locked", locked, err)
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
		got := jitterDuration(base, jitter)
		if got < base || got >= base+jitter {
			t.Fatalf("jitterDuration() = %v, want [%v,%v)", got, base, base+jitter)
		}
	}
}

// countingStore 统计 SetNX 调用次数，用于验证 singleflight 是否拦截本机并发。
type countingStore struct {
	Store      // 复用底层 RedisStore 的完整 Store 实现
	setNXCalls atomic.Int64
}

// SetNX 统计分布式锁请求次数后再透传到底层 Store。
func (s *countingStore) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	s.setNXCalls.Add(1)
	return s.Store.SetNX(ctx, key, value, ttl)
}

// lockLostStore 模拟锁续期失败场景，并统计是否发生后续写回。
type lockLostStore struct {
	Store
	refreshLockCalls atomic.Int64
	writeBatchCalls  atomic.Int64
}

// RefreshLock 首次续期直接返回 owner mismatch，模拟实例在刷新过程中失锁。
func (s *lockLostStore) RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	s.refreshLockCalls.Add(1)
	return false, nil
}

// WriteBatch 统计批量写回次数，便于断言失锁后不会再写缓存。
func (s *lockLostStore) WriteBatch(ctx context.Context, entries []Entry) error {
	s.writeBatchCalls.Add(1)
	return s.Store.WriteBatch(ctx, entries)
}

// recordingLockStore 记录实际使用的锁 key，便于验证不同刷新入口的锁域是否一致。
type recordingLockStore struct {
	Store
	mu   sync.Mutex
	keys []string
}

// SetNX 记录所有加锁 key，再透传到底层 Store。
func (s *recordingLockStore) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.Store.SetNX(ctx, key, value, ttl)
}

// lockKeys 返回当前测试记录到的锁 key 列表副本。
func (s *recordingLockStore) lockKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, len(s.keys))
	copy(keys, s.keys)
	return keys
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
	lookupHit              atomic.Int64
	lookupMiss             atomic.Int64
	lookupEmpty            atomic.Int64
	lookupRefreshTriggered atomic.Int64
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

// RecordRefreshEntryCount 记录单次刷新写入条数；测试场景无需额外断言时保留空实现。
func (m *recordingMetrics) RecordRefreshEntryCount(ctx context.Context, index string, count int) {
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

// TestManagerWithGoUtilsLogger 验证缓存管理器可直接接入 go-utils.Logger。
func TestManagerWithGoUtilsLogger(t *testing.T) {
	logger := &recordingUtilsLogger{}
	manager, err := NewManager(NewRedisStore(redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})), nil, WithLogger(logger))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	manager.logInfoEvent("go_utils_logger", "user", "user:1", "result", "success")
	if !logger.contains("tablecache") {
		t.Fatalf("utils logger message not found, logs=%v", logger.messages())
	}
}

// TestManagerWithGoUtilsLoggerOption 验证显式 go-utils 日志接入入口可正常工作。
func TestManagerWithGoUtilsLoggerOption(t *testing.T) {
	logger := &recordingUtilsLogger{}
	manager, err := NewManager(NewRedisStore(redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})), nil, WithGoUtilsLogger(logger))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	manager.logInfoEvent("go_utils_logger_option", "user", "user:2", "result", "success")
	if !logger.contains("go_utils_logger_option") {
		t.Fatalf("utils logger option message not found, logs=%v", logger.messages())
	}
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

// Debugf 记录调试日志。
func (l *recordingLogger) Debugf(format string, args ...any) {
	l.append(format, args...)
}

// Infof 记录信息日志。
func (l *recordingLogger) Infof(format string, args ...any) {
	l.append(format, args...)
}

// Warnf 记录警告日志。
func (l *recordingLogger) Warnf(format string, args ...any) {
	l.append(format, args...)
}

// Errorf 记录错误日志。
func (l *recordingLogger) Errorf(format string, args ...any) {
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
