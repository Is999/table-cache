package tablecache

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
)

// TestManagerPrefixCommitAllowsConcurrentLoaders 验证全量 Loader 不阻塞单 key Loader，提交仍按代际串行。
func TestManagerPrefixCommitAllowsConcurrentLoaders(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &blockingPrefixReplaceStore{
		RedisStore:     NewRedisStore(client),
		replaceStarted: make(chan struct{}),
		replaceRelease: make(chan struct{}),
	}
	metrics := &recordingMetrics{}
	fullStarted := make(chan struct{})
	fullRelease := make(chan struct{})
	singleStarted := make(chan struct{})
	singleRelease := make(chan struct{})
	var singleCalls atomic.Int64
	manager, err := newTestManager(store, []Target{{
		Index: "commit-race",
		Key:   "commit-race:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			if params.Key == defaultPhysicalKey("commit-race:") {
				close(fullStarted)
				<-fullRelease
				return []Entry{{Key: defaultPhysicalKey("commit-race:1"), Value: "full"}}, nil
			}
			if singleCalls.Add(1) == 1 {
				close(singleStarted)
				<-singleRelease
				return []Entry{{Key: params.Key, Value: "stale"}}, nil
			}
			return []Entry{{Key: params.Key, Value: "fresh"}}, nil
		},
	}}, WithWait(5*time.Millisecond, 200), WithMetrics(metrics))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	fullDone := make(chan error, 1)
	go func() { fullDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("commit-race:")) }()
	<-fullStarted
	singleDone := make(chan error, 1)
	go func() { singleDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("commit-race:1")) }()
	select {
	case <-singleStarted:
	case <-time.After(time.Second):
		t.Fatal("single loader did not enter while full loader was blocked")
	}
	close(fullRelease)
	<-store.replaceStarted
	close(singleRelease)
	select {
	case err := <-singleDone:
		t.Fatalf("single refresh committed during full replace: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.replaceRelease)
	if err := <-fullDone; err != nil {
		t.Fatalf("full RefreshByKey() error = %v", err)
	}
	if err := <-singleDone; err != nil {
		t.Fatalf("single RefreshByKey() error = %v", err)
	}
	if got := singleCalls.Load(); got != 2 {
		t.Fatalf("single loader calls = %d, want 2 after generation retry", got)
	}
	if got := metrics.prefixWait.Load(); got != 1 {
		t.Fatalf("prefix commit waits = %d, want 1", got)
	}
	if got := client.Get(ctx, defaultPhysicalKey("commit-race:1")).Val(); got != "fresh" {
		t.Fatalf("commit-race:1 = %q, want fresh", got)
	}
}

// TestManagerFullStartEpochWaitsForSingleCommit 验证全量起始代际不会插入已校验的单key提交。
func TestManagerFullStartEpochWaitsForSingleCommit(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	store := &blockingMutationStore{
		RedisStore: NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()})),
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	fullLoaderStarted := make(chan struct{})
	manager, err := newTestManager(store, []Target{{
		Index: "start-epoch",
		Key:   "start-epoch:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			if params.Key == defaultPhysicalKey("start-epoch:") {
				close(fullLoaderStarted)
				return []Entry{{Key: defaultPhysicalKey("start-epoch:1"), Value: "full"}}, nil
			}
			return []Entry{{Key: params.Key, Value: "single"}}, nil
		},
	}}, WithWait(5*time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	singleDone := make(chan error, 1)
	go func() { singleDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("start-epoch:1")) }()
	<-store.started
	fullDone := make(chan error, 1)
	go func() { fullDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("start-epoch:")) }()
	select {
	case <-fullLoaderStarted:
		t.Fatal("full loader entered before existing single commit released")
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	if err := <-singleDone; err != nil {
		t.Fatalf("single refresh wrote then returned error: %v", err)
	}
	select {
	case <-fullLoaderStarted:
	case <-time.After(time.Second):
		t.Fatal("full loader did not enter after single commit")
	}
	if err := <-fullDone; err != nil {
		t.Fatalf("full refresh error = %v", err)
	}
}

// TestManagerDeleteByKeyInvalidatesFullSnapshot 验证成员删除返回后，加载中的旧全量快照不能复活该 key。
func TestManagerDeleteByKeyInvalidatesFullSnapshot(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	started := make(chan struct{})
	release := make(chan struct{})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "delete-full",
		Key:   "delete-full:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			close(started)
			<-release
			return []Entry{{Key: defaultPhysicalKey("delete-full:1"), Value: "stale"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, defaultPhysicalKey("delete-full:1"), "old", 0).Err(); err != nil {
		t.Fatal(err)
	}
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("delete-full:")) }()
	<-started
	if err := manager.DeleteByKey(ctx, defaultPhysicalKey("delete-full:1")); err != nil {
		t.Fatalf("DeleteByKey() error = %v", err)
	}
	close(release)
	if err := <-refreshDone; !errors.Is(err, ErrRefreshInvalidated) {
		t.Fatalf("full refresh error = %v, want ErrRefreshInvalidated", err)
	}
	if client.Exists(ctx, defaultPhysicalKey("delete-full:1")).Val() != 0 {
		t.Fatal("deleted key was resurrected by old full snapshot")
	}
}

// TestManagerDeleteByPrefixInvalidatesSingleCommit 验证前缀删除可在线程加载期间完成并阻断旧提交。
func TestManagerDeleteByPrefixInvalidatesSingleCommit(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	started := make(chan struct{})
	release := make(chan struct{})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "delete-prefix",
		Key:   "delete-prefix:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			close(started)
			<-release
			return []Entry{{Key: params.Key, Value: "stale"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- manager.RefreshByKey(ctx, defaultPhysicalKey("delete-prefix:1")) }()
	<-started
	if err := manager.DeleteByPrefix(ctx, defaultPhysicalKey("delete-prefix:")); err != nil {
		t.Fatalf("DeleteByPrefix() error = %v", err)
	}
	close(release)
	if err := <-refreshDone; !errors.Is(err, ErrRefreshInvalidated) {
		t.Fatalf("single refresh error = %v, want ErrRefreshInvalidated", err)
	}
	if client.Exists(ctx, defaultPhysicalKey("delete-prefix:1")).Val() != 0 {
		t.Fatal("prefix delete was followed by stale single-key write")
	}
}

// TestManagerSkipsRedundantEpochIO 验证只在存在合法跨 key 前缀操作时读取必要代际。
func TestManagerSkipsRedundantEpochIO(t *testing.T) {
	t.Run("fixed target", func(t *testing.T) {
		ctx := context.Background()
		server := runStandaloneRedis(t)
		store := &epochCountingStore{RedisStore: NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))}
		manager, err := newTestManager(store, []Target{{
			Index: "epoch-fixed",
			Key:   "epoch-fixed",
			Type:  TypeString,
			Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		}})
		if err != nil {
			t.Fatalf("newTestManager() error = %v", err)
		}
		key := defaultPhysicalKey("epoch-fixed")
		if err := manager.RefreshByKey(ctx, key); err != nil {
			t.Fatalf("RefreshByKey() error = %v", err)
		}
		if reads := store.reads.Load(); reads != 0 {
			t.Fatalf("Store.Read calls = %d, want 0 epoch reads", reads)
		}
		if err := manager.DeleteByKey(ctx, key); err != nil {
			t.Fatalf("DeleteByKey() error = %v", err)
		}
		if writes := store.keyDeleteWrites.Load(); writes != 0 {
			t.Fatalf("key delete epoch writes = %d, want 0", writes)
		}
	})

	t.Run("distributed untagged prefix", func(t *testing.T) {
		server := runStandaloneRedis(t)
		ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"one": server.Addr()}})
		t.Cleanup(func() { _ = ring.Close() })
		store := &epochCountingStore{RedisStore: NewRedisStore(ring)}
		_, err := newTestManager(store, []Target{{
			Index: "epoch-ring",
			Key:   "epoch-ring:",
			Type:  TypeString,
			Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		}})
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("newTestManager() error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("single redis prefix", func(t *testing.T) {
		ctx := context.Background()
		server := runStandaloneRedis(t)
		store := &epochCountingStore{RedisStore: NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))}
		manager, err := newTestManager(store, []Target{{
			Index: "epoch-prefix",
			Key:   "epoch-prefix:",
			Type:  TypeString,
			Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		}})
		if err != nil {
			t.Fatalf("newTestManager() error = %v", err)
		}
		if err := manager.RefreshByKey(ctx, defaultPhysicalKey("epoch-prefix:1")); err != nil {
			t.Fatalf("RefreshByKey() error = %v", err)
		}
		if reads := store.reads.Load(); reads != 4 {
			t.Fatalf("Store.Read calls = %d, want 4 retained prefix epoch reads", reads)
		}
	})
}

// TestManagerPrefixReplaceValidatesBeforeMutation 验证非法全量结果不会触碰旧业务缓存。
func TestManagerPrefixReplaceValidatesBeforeMutation(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	if err := client.Set(ctx, defaultPhysicalKey("snapshot:old"), "keep", 0).Err(); err != nil {
		t.Fatal(err)
	}
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "snapshot",
		Key:   "snapshot:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: defaultPhysicalKey("outside:1"), Value: "bad"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("snapshot:")); !errors.Is(err, ErrEntryKeyOutOfScope) {
		t.Fatalf("RefreshByKey() error = %v, want ErrEntryKeyOutOfScope", err)
	}
	if got := client.Get(ctx, defaultPhysicalKey("snapshot:old")).Val(); got != "keep" {
		t.Fatalf("snapshot:old = %q, want keep", got)
	}
}

// TestManagerPrefixReplaceRejectsTypeMismatchAndDuplicateKeys 验证整批本地校验失败时零写入。
func TestManagerPrefixReplaceRejectsTypeMismatchAndDuplicateKeys(t *testing.T) {
	tests := []struct {
		name    string
		entries []Entry
	}{
		{
			name: "type",
			entries: []Entry{{
				Key:   defaultPhysicalKey("entry-check:1"),
				Type:  TypeHash,
				Value: map[string]any{"a": "1"},
			}},
		},
		{
			name: "duplicate",
			entries: []Entry{
				{Key: defaultPhysicalKey("entry-check:1"), Value: "a"},
				{Key: " " + defaultPhysicalKey("entry-check:1") + " ", Value: "b"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := runStandaloneRedis(t)
			store := &recordingPrefixReplaceStore{RedisStore: NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))}
			manager, err := newTestManager(store, []Target{{
				Index: "entry-check",
				Key:   "entry-check:",
				Type:  TypeString,
				Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
					return tt.entries, nil
				},
			}})
			if err != nil {
				t.Fatalf("newTestManager() error = %v", err)
			}
			if err := manager.RefreshByKey(context.Background(), defaultPhysicalKey("entry-check:")); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("RefreshByKey() error = %v, want ErrInvalidConfig", err)
			}
			if store.calls.Load() != 0 {
				t.Fatalf("ReplacePrefix calls = %d, want 0", store.calls.Load())
			}
		})
	}
}

// TestManagerPrefixReplaceFailurePreservesOldData 验证安全替换失败不会先清空旧快照。
func TestManagerPrefixReplaceFailurePreservesOldData(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &failingPrefixReplaceStore{RedisStore: NewRedisStore(client)}
	if err := client.Set(ctx, defaultPhysicalKey("replace-fail:old"), "keep", 0).Err(); err != nil {
		t.Fatal(err)
	}
	manager, err := newTestManager(store, []Target{{
		Index: "replace-fail",
		Key:   "replace-fail:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: defaultPhysicalKey("replace-fail:new"), Value: "new"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("replace-fail:")); err == nil {
		t.Fatal("RefreshByKey() error = nil, want replace failure")
	}
	if got := client.Get(ctx, defaultPhysicalKey("replace-fail:old")).Val(); got != "keep" {
		t.Fatalf("replace-fail:old = %q, want keep", got)
	}
}

// TestManagerEmptyFullRefreshMarksCompletion 验证空全量快照仍可让跨实例waiter复用完成状态。
func TestManagerEmptyFullRefreshMarksCompletion(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	redisStore := NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	store := &prefixAttemptStore{RedisStore: redisStore, lockKey: tablecacheMetaKey("rebuild:lock", defaultPhysicalKey("empty-full:")), second: make(chan struct{})}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	loader := func(ctx context.Context, params LoadParams) ([]Entry, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil, nil
	}
	manager1, err := newTestManager(store, []Target{{Index: "empty-full", Key: "empty-full:", Type: TypeString, Loader: loader}}, WithWait(5*time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager(1) error = %v", err)
	}
	manager2, err := newTestManager(store, []Target{{Index: "empty-full", Key: "empty-full:", Type: TypeString, Loader: loader}}, WithWait(5*time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager(2) error = %v", err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- manager1.RefreshByKey(ctx, defaultPhysicalKey("empty-full:")) }()
	<-started
	secondDone := make(chan error, 1)
	go func() { secondDone <- manager2.RefreshByKey(ctx, defaultPhysicalKey("empty-full:")) }()
	<-store.second
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first full refresh error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second full refresh error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("full loader calls = %d, want 1", calls.Load())
	}
}

// TestManagerEmptyFullRefreshSkipsMarker 验证空全量快照不会生成前缀根伪业务 key。
func TestManagerEmptyFullRefreshSkipsMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	metrics := &recordingMetrics{}
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index:            "empty-full-marker",
		Key:              "empty-full-marker:",
		Type:             TypeString,
		AllowEmptyMarker: true,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return nil, errors.Tag(ErrNotFound)
		},
	}}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.Set(ctx, defaultPhysicalKey("empty-full-marker:old"), "old", 0).Err(); err != nil {
		t.Fatal(err)
	}
	target := manager.prefixTargets[0]
	if err := manager.RefreshByKey(ctx, target.Key); err != nil {
		t.Fatalf("RefreshByKey() error = %v", err)
	}
	if exists := client.Exists(ctx, defaultPhysicalKey("empty-full-marker:old"), target.Key, manager.emptyKey(target.Key)).Val(); exists != 0 {
		t.Fatalf("empty full snapshot left business or marker keys = %d", exists)
	}
	if calls := metrics.refreshEntryCountCalls.Load(); calls != 1 {
		t.Fatalf("refresh entry count metric calls = %d, want 1", calls)
	}
}

// TestManagerPrefixReplaceRequiresSafeStore 验证不支持安全替换时在调用 Loader 前失败。
func TestManagerPrefixReplaceRequiresSafeStore(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var calls atomic.Int64
	manager, err := newTestManager(&basicStore{Store: NewRedisStore(client)}, []Target{{
		Index: "unsupported",
		Key:   "unsupported:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			calls.Add(1)
			return nil, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(context.Background(), defaultPhysicalKey("unsupported:")); !errors.Is(err, ErrPrefixReplaceUnsupported) {
		t.Fatalf("RefreshByKey() error = %v, want ErrPrefixReplaceUnsupported", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("loader calls = %d, want 0", calls.Load())
	}
}

// TestManagerReportsPrefixDeleteCapabilityAccurately 验证自定义 Store 缺少带锁 SCAN 时返回专用错误。
func TestManagerReportsPrefixDeleteCapabilityAccurately(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(&basicStore{Store: NewRedisStore(client)}, []Target{{
		Index: "unsupported-delete",
		Key:   "unsupported-delete:",
		Type:  TypeString,
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	err = manager.DeleteByPrefix(context.Background(), defaultPhysicalKey("unsupported-delete:"))
	if !errors.Is(err, ErrPrefixDeleteUnsupported) {
		t.Fatalf("DeleteByPrefix() error = %v, want ErrPrefixDeleteUnsupported", err)
	}
}

// TestManagerSingleflightWaiterRetriesAfterLeaderCancel 验证 leader ctx 取消不会污染仍存活 waiter。
func TestManagerSingleflightWaiterRetriesAfterLeaderCancel(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	started := make(chan struct{})
	var calls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "flight-cancel",
		Key:   "flight-cancel",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				return nil, errors.Tag(ctx.Err())
			}
			return []Entry{{Key: params.Key, Value: "ok"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	leaderCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- manager.RefreshByKey(leaderCtx, defaultPhysicalKey("flight-cancel")) }()
	<-started
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- manager.RefreshByKey(context.Background(), defaultPhysicalKey("flight-cancel")) }()
	if err := <-leaderDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("leader error = %v, want deadline exceeded", err)
	}
	if err := <-waiterDone; err != nil {
		t.Fatalf("waiter error = %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("loader calls = %d, want 2", calls.Load())
	}
}

// TestManagerSingleflightLeaderCancelRetriesStayCoalesced 验证多个 waiter 重试时仍只创建一个新 flight。
func TestManagerSingleflightLeaderCancelRetriesStayCoalesced(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	firstStarted := make(chan struct{})
	retryStarted := make(chan struct{})
	retryRelease := make(chan struct{})
	var calls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "flight-cancel-many",
		Key:   "flight-cancel-many",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			call := calls.Add(1)
			if call == 1 {
				close(firstStarted)
				<-ctx.Done()
				return nil, errors.Tag(ctx.Err())
			}
			if call == 2 {
				close(retryStarted)
			}
			<-retryRelease
			return []Entry{{Key: params.Key, Value: "ok"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- manager.RefreshByKey(leaderCtx, defaultPhysicalKey("flight-cancel-many")) }()
	<-firstStarted

	const waiters = 24
	start := make(chan struct{})
	waiterDone := make(chan error, waiters)
	for range waiters {
		go func() {
			<-start
			waiterDone <- manager.RefreshByKey(context.Background(), defaultPhysicalKey("flight-cancel-many"))
		}()
	}
	close(start)
	// 给全部 waiter 足够时间加入首轮 flight，再取消 leader。
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	<-retryStarted
	time.Sleep(30 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		close(retryRelease)
		t.Fatalf("loader calls before retry release = %d, want 2", got)
	}
	close(retryRelease)
	for range waiters {
		if err := <-waiterDone; err != nil {
			t.Fatalf("waiter error = %v", err)
		}
	}
}

// TestManagerLoadThroughOverridesDoNotShareFlight 验证不同临时 Loader 不复用同一 flight 结果。
func TestManagerLoadThroughOverridesDoNotShareFlight(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{{Index: "override", Key: "override", Type: TypeString}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	var firstCalls atomic.Int64
	var secondCalls atomic.Int64
	firstDone := make(chan error, 1)
	go func() {
		var value string
		_, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("override"), &value, LoadThroughOptions{Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			firstCalls.Add(1)
			close(firstStarted)
			<-firstRelease
			return []Entry{{Key: params.Key, Value: "first"}}, nil
		}})
		firstDone <- err
	}()
	<-firstStarted
	secondDone := make(chan error, 1)
	go func() {
		var value string
		_, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("override"), &value, LoadThroughOptions{Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			secondCalls.Add(1)
			return []Entry{{Key: params.Key, Value: "second"}}, nil
		}})
		secondDone <- err
	}()
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first LoadThrough() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second LoadThrough() error = %v", err)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 0 {
		t.Fatalf("loader calls = %d/%d, want 1/0 after shared Redis completion", firstCalls.Load(), secondCalls.Load())
	}
}

// TestManagerLoadThroughWithoutReadableStateRetriesLoader 验证空结果未落缓存时不写完成标记。
func TestManagerLoadThroughWithoutReadableStateRetriesLoader(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{{Index: "no-state", Key: "no-state", Type: TypeString}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	var firstCalls atomic.Int64
	var secondCalls atomic.Int64
	firstDone := make(chan error, 1)
	go func() {
		var value string
		_, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("no-state"), &value, LoadThroughOptions{Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			firstCalls.Add(1)
			close(firstStarted)
			<-firstRelease
			return nil, nil
		}})
		firstDone <- err
	}()
	<-firstStarted
	secondDone := make(chan error, 1)
	go func() {
		var value string
		_, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("no-state"), &value, LoadThroughOptions{Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			secondCalls.Add(1)
			return nil, nil
		}})
		secondDone <- err
	}()
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first LoadThrough() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second LoadThrough() error = %v", err)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("loader calls = %d/%d, want 1/1 without readable state", firstCalls.Load(), secondCalls.Load())
	}
}

// TestManagerInternalLoaderTimeoutDoesNotRetryFlight 验证内部Loader超时不会被误判为leader caller取消。
func TestManagerInternalLoaderTimeoutDoesNotRetryFlight(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var calls atomic.Int64
	started := make(chan struct{})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index:         "internal-timeout",
		Key:           "internal-timeout",
		Type:          TypeString,
		LoaderTimeout: 500 * time.Millisecond,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-ctx.Done()
			return nil, errors.Tag(ctx.Err())
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- manager.RefreshByKey(context.Background(), defaultPhysicalKey("internal-timeout")) }()
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("loader did not start")
	}
	for range 2 {
		if err := <-results; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("RefreshByKey() error = %v, want deadline exceeded", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}
}

// TestManagerPartialHashRefreshPreservesFields 验证缺少字段触发局部回源且不会覆盖已有 Hash。
func TestManagerPartialHashRefreshPreservesFields(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	if err := client.HSet(ctx, defaultPhysicalKey("partial:1"), "name", "alpha").Err(); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "partial",
		Key:   "partial:",
		Type:  TypeHash,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			calls.Add(1)
			return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{"status": "enabled"}}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value map[string]string
	result, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("partial:1"), &value, LoadThroughOptions{Fields: []string{"status"}})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions() error = %v", err)
	}
	if result.State != LookupStateHit || value["status"] != "enabled" || calls.Load() != 1 {
		t.Fatalf("result=%+v value=%v calls=%d", result, value, calls.Load())
	}
	all := client.HGetAll(ctx, defaultPhysicalKey("partial:1")).Val()
	if all["name"] != "alpha" || all["status"] != "enabled" {
		t.Fatalf("hash = %v, want preserved name and refreshed status", all)
	}
}

// TestManagerPartialHashWithoutEmptyMarkerNeedsPageAndDeleteStore 验证关闭字段空值后不强制自定义Store实现registry能力。
func TestManagerPartialHashWithoutEmptyMarkerNeedsPageAndDeleteStore(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	redisStore := NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	store := &pageOnlyStore{Store: redisStore, page: redisStore, fields: redisStore}
	manager, err := newTestManager(store, []Target{{
		Index: "page-only",
		Key:   "page-only",
		Type:  TypeHash,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{"status": "ok"}}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value map[string]string
	result, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("page-only"), &value, LoadThroughOptions{Fields: []string{"status"}})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions() error = %v", err)
	}
	if result.State != LookupStateHit || value["status"] != "ok" {
		t.Fatalf("result=%+v value=%v, want hit/status=ok", result, value)
	}
}

// TestManagerPartialHashWithoutEmptyMarkerDeletesStaleFields 验证 fields 空回源会删除请求字段而保留其它 Hash 内容。
func TestManagerPartialHashWithoutEmptyMarkerDeletesStaleFields(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	key := defaultPhysicalKey("fields-empty-delete")
	if err := client.HSet(ctx, key, "name", "alpha", "status", "stale").Err(); err != nil {
		t.Fatalf("HSet() error = %v", err)
	}
	if err := client.Expire(ctx, key, time.Hour).Err(); err != nil {
		t.Fatalf("Expire() error = %v", err)
	}
	manager, err := newTestManager(store, []Target{{
		Index: "fields-empty-delete",
		Key:   "fields-empty-delete",
		Type:  TypeHash,
		Loader: func(context.Context, LoadParams) ([]Entry, error) {
			return nil, errors.Tag(ErrNotFound)
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, key, "status"); err != nil {
		t.Fatalf("RefreshByKey(fields) error = %v", err)
	}
	values := client.HGetAll(ctx, key).Val()
	if len(values) != 1 || values["name"] != "alpha" {
		t.Fatalf("hash = %v, want only name=alpha", values)
	}
	if ttl := client.TTL(ctx, key).Val(); ttl <= 0 {
		t.Fatalf("hash TTL = %v, want preserved positive TTL", ttl)
	}
	if exists := client.Exists(ctx, manager.fieldsEmptyKey(key)).Val(); exists != 0 {
		t.Fatalf("fields empty registry exists = %d, want 0", exists)
	}
}

// TestManagerPartialHashWithoutEmptyMarkerClearsRegistry 验证正向字段写回清除整个历史负缓存 registry。
func TestManagerPartialHashWithoutEmptyMarkerClearsRegistry(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	redisStore := NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	manager, err := newTestManager(redisStore, []Target{{
		Index: "fields-disabled",
		Key:   "fields-disabled",
		Type:  TypeHash,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{"status": "ok"}}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := defaultPhysicalKey("fields-disabled")
	registryKey := manager.fieldsEmptyKey(key)
	otherFieldsID := fieldsID([]string{"name"})
	if err := redisStore.ReplaceFieldsWithEmpty(ctx, acquireTestLockGuard(t, redisStore, key), key, registryKey, otherFieldsID, []string{"name"}, time.Minute); err != nil {
		t.Fatalf("ReplaceFieldsWithEmpty() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("fields-disabled"), "status"); err != nil {
		t.Fatalf("RefreshByKey(fields) error = %v", err)
	}
	marked, err := redisStore.HasFieldsEmpty(ctx, registryKey, otherFieldsID)
	if err != nil {
		t.Fatalf("HasFieldsEmpty() error = %v", err)
	}
	if marked {
		t.Fatal("partial refresh kept a stale fields signature")
	}
}

// TestManagerPartialHashEmptyMarkerRemovesStaleFields 验证字段级空值会删除请求字段并保留其它业务字段。
func TestManagerPartialHashEmptyMarkerRemovesStaleFields(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	if err := client.HSet(ctx, defaultPhysicalKey("partial-empty:1"), "name", "alpha", "status", "stale").Err(); err != nil {
		t.Fatal(err)
	}
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index:            "partial-empty",
		Key:              "partial-empty:",
		Type:             TypeHash,
		AllowEmptyMarker: true,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return nil, errors.Tag(ErrNotFound)
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("partial-empty:1"), "status"); err != nil {
		t.Fatalf("RefreshByKey(fields) error = %v", err)
	}
	if status := client.HGet(ctx, defaultPhysicalKey("partial-empty:1"), "status").Val(); status != "" {
		t.Fatalf("stale status = %q, want deleted", status)
	}
	var value map[string]string
	result, err := manager.LoadThroughWithOptions(ctx, defaultPhysicalKey("partial-empty:1"), &value, LoadThroughOptions{Fields: []string{"status"}})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions() error = %v", err)
	}
	if result.State != LookupStateEmpty || client.HGet(ctx, defaultPhysicalKey("partial-empty:1"), "name").Val() != "alpha" {
		t.Fatalf("result=%+v hash=%v", result, client.HGetAll(ctx, defaultPhysicalKey("partial-empty:1")).Val())
	}
	fieldsStore := any(manager.store).(FieldsEmptyStore)
	marked, err := fieldsStore.HasFieldsEmpty(ctx, manager.fieldsEmptyKey(defaultPhysicalKey("partial-empty:1")), fieldsID([]string{"status"}))
	if err != nil || !marked {
		t.Fatal("fields empty marker not written")
	}
}

// TestManagerPartialHashMarkersAreIndependent 验证字段 marker 独立到期，正向写回统一撤销历史负缓存。
func TestManagerPartialHashMarkersAreIndependent(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index:            "field-marker",
		Key:              "field-marker:",
		Type:             TypeHash,
		EmptyTTL:         100 * time.Millisecond,
		AllowEmptyMarker: true,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]any{params.Fields[0]: "ok"}}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	target := manager.prefixTargets[0]
	left := []string{"name"}
	right := []string{"status"}
	key := defaultPhysicalKey("field-marker:1")
	if err := manager.writeFieldsEmptyMarker(ctx, target, key, left, acquireTestLockGuard(t, manager.store, key)); err != nil {
		t.Fatal(err)
	}
	target.EmptyTTL = 200 * time.Millisecond
	if err := manager.writeFieldsEmptyMarker(ctx, target, key, right, acquireTestLockGuard(t, manager.store, defaultPhysicalKey("field-marker:1:right"))); err != nil {
		t.Fatal(err)
	}
	registryKey := manager.fieldsEmptyKey(key)
	leftExpiry := client.ZScore(ctx, registryKey, fieldsID(left)).Val()
	rightExpiry := client.ZScore(ctx, registryKey, fieldsID(right)).Val()
	if rightExpiry-leftExpiry < 90 {
		t.Fatalf("field marker expiries = %.0f/%.0f, want independent TTL scores", leftExpiry, rightExpiry)
	}
	// 任一字段成功写回都清除整个 registry，避免重叠组合留下假阴性。
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("field-marker:1"), "name"); err != nil {
		t.Fatalf("RefreshByKey(fields) error = %v", err)
	}
	leftMarked, _ := any(manager.store).(FieldsEmptyStore).HasFieldsEmpty(ctx, registryKey, fieldsID(left))
	rightMarked, _ := any(manager.store).(FieldsEmptyStore).HasFieldsEmpty(ctx, registryKey, fieldsID(right))
	if leftMarked || rightMarked {
		t.Fatal("successful field refresh kept stale fields markers")
	}
}

// TestFieldsIDSeparatesDelimiterContent 验证字段内容中的分隔符不会造成签名碰撞。
func TestFieldsIDSeparatesDelimiterContent(t *testing.T) {
	left := fieldsID(normalizeFields([]string{"a,b", "c"}))
	right := fieldsID(normalizeFields([]string{"a", "b,c"}))
	if left == right {
		t.Fatalf("fields signatures collide: %s", left)
	}
}

// TestManagerLoaderBulkheadBoundsConcurrency 验证全局回源上限和等待槽位取消。
func TestManagerLoaderBulkheadBoundsConcurrency(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	var startedOnce sync.Once
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "bulkhead",
		Key:   "bulkhead:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			calls.Add(1)
			startedOnce.Do(func() { close(started) })
			<-release
			return []Entry{{Key: params.Key, Value: "ok"}}, nil
		},
	}}, WithLoaderConcurrency(1))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.RefreshByKey(context.Background(), defaultPhysicalKey("bulkhead:1")) }()
	<-started
	secondCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := manager.RefreshByKey(secondCtx, defaultPhysicalKey("bulkhead:2")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second RefreshByKey() error = %v, want deadline exceeded", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first RefreshByKey() error = %v", err)
	}
}

// TestManagerHiddenEmptyFailureOrdering 验证索引和安全状态切换按失败保守顺序执行。
func TestManagerHiddenEmptyFailureOrdering(t *testing.T) {
	tests := []struct {
		name        string
		failAdd     bool
		failReplace bool
		wantOld     bool
		wantMarker  bool
		wantCalls   int64
		wantError   bool
	}{
		{name: "add", failAdd: true, wantOld: true, wantError: true},
		{name: "replace", failReplace: true, wantOld: true, wantCalls: 1, wantError: true},
		{name: "superset", wantMarker: true, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			server := runStandaloneRedis(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			store := &hiddenFaultStore{RedisStore: NewRedisStore(client), failAdd: tt.failAdd, failReplace: tt.failReplace}
			manager, err := newTestManager(store, []Target{{
				Index:            "hidden-fault",
				Key:              "hidden-fault:",
				Type:             TypeString,
				AllowEmptyMarker: true,
				Loader:           func(ctx context.Context, params LoadParams) ([]Entry, error) { return nil, errors.Tag(ErrNotFound) },
			}})
			if err != nil {
				t.Fatalf("newTestManager() error = %v", err)
			}
			key := defaultPhysicalKey("hidden-fault:1")
			if err := client.Set(ctx, key, "old", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := manager.RefreshByKey(ctx, defaultPhysicalKey("hidden-fault:1")); (err != nil) != tt.wantError {
				t.Fatalf("RefreshByKey() error = %v, wantError %v", err, tt.wantError)
			}
			if got := client.Exists(ctx, key).Val() == 1; got != tt.wantOld {
				t.Fatalf("old exists = %v, want %v", got, tt.wantOld)
			}
			if got := client.Exists(ctx, manager.emptyKey(key)).Val() == 1; got != tt.wantMarker {
				t.Fatalf("marker exists = %v, want %v", got, tt.wantMarker)
			}
			if store.replaceCalls.Load() != tt.wantCalls {
				t.Fatalf("replace calls = %d, want %d", store.replaceCalls.Load(), tt.wantCalls)
			}
		})
	}
}

// TestManagerEmptyCollectionMarkerKeepsIndexSuperset 验证空集合提交后在线索引保留安全超集。
func TestManagerEmptyCollectionMarkerKeepsIndexSuperset(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &hiddenFaultStore{RedisStore: NewRedisStore(client)}
	manager, err := newTestManager(store, []Target{{
		Index: "empty-collection-fault",
		Key:   "empty-collection-fault:",
		Type:  TypeList,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Type: TypeList, Value: []string{}}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := defaultPhysicalKey("empty-collection-fault:1")
	if err := client.RPush(ctx, key, "old").Err(); err != nil {
		t.Fatal(err)
	}
	if err := manager.RefreshByKey(ctx, key); err != nil {
		t.Fatalf("RefreshByKey() error = %v", err)
	}
	if exists := client.Exists(ctx, key).Val(); exists != 0 {
		t.Fatalf("old list exists = %d, want 0", exists)
	}
	var value []string
	result, err := manager.GetState(ctx, key, &value)
	if err != nil {
		t.Fatalf("GetState() error = %v", err)
	}
	if result.State != LookupStateHit || len(value) != 0 {
		t.Fatalf("GetState() result=%+v value=%v, want empty hit", result, value)
	}
}

// TestManagerHiddenEmptyFailureKeepsFieldsRegistry 验证隐藏空值切换失败时保留既有字段空值状态。
func TestManagerHiddenEmptyFailureKeepsFieldsRegistry(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &hiddenFaultStore{RedisStore: NewRedisStore(client), failReplace: true}
	manager, err := newTestManager(store, []Target{{
		Index:            "hidden-fields-fault",
		Key:              "hidden-fields-fault:",
		Type:             TypeHash,
		AllowEmptyMarker: true,
		Loader:           func(ctx context.Context, params LoadParams) ([]Entry, error) { return nil, errors.Tag(ErrNotFound) },
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := defaultPhysicalKey("hidden-fields-fault:1")
	registryKey := manager.fieldsEmptyKey(key)
	fields := []string{"name"}
	if err := store.ReplaceFieldsWithEmpty(ctx, acquireTestLockGuard(t, store, key), key, registryKey, fieldsID(fields), fields, time.Minute); err != nil {
		t.Fatalf("ReplaceFieldsWithEmpty() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, key); err == nil {
		t.Fatal("RefreshByKey() error = nil, want injected failure")
	}
	marked, err := store.HasFieldsEmpty(ctx, registryKey, fieldsID(fields))
	if err != nil {
		t.Fatalf("HasFieldsEmpty() error = %v", err)
	}
	if !marked {
		t.Fatal("fields empty registry was removed before hidden marker commit")
	}
}

// TestManagerConfigValidation 验证非法类型、TTL、锁和上下文策略在启动期失败。
func TestManagerConfigValidation(t *testing.T) {
	server := runStandaloneRedis(t)
	store := NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	tests := []struct {
		name    string
		targets []Target
		opts    []Option
	}{
		{name: "cache type", targets: []Target{{Index: "bad", Key: "bad", Type: CacheType("stream")}}},
		{name: "negative ttl", targets: []Target{{Index: "bad", Key: "bad", TTL: -time.Second}}},
		{name: "permanent jitter", targets: []Target{{Index: "bad", Key: "bad", Jitter: time.Second}}},
		{name: "refresh all loader", targets: []Target{{Index: "bad", Key: "bad", RefreshAll: true}}},
		{name: "lock renew", opts: []Option{WithLockTTL(time.Second), WithLockRenewInterval(time.Second)}},
		{name: "context policy", opts: []Option{WithRebuildContextPolicy(RebuildContextPolicy(99))}},
		{name: "scan count", opts: []Option{WithScanCount(-1)}},
		{name: "loader concurrency", opts: []Option{WithLoaderConcurrency(0)}},
		{name: "short index ttl", opts: []Option{WithPrefixKeyIndexTTL(time.Nanosecond)}},
		{name: "short epoch window", targets: []Target{{Index: "bad", Key: "bad:"}}, opts: []Option{WithPrefixEpochTTL(100 * time.Millisecond)}},
		{name: "target loader window", targets: []Target{{Index: "bad", Key: "bad:", LoaderTimeout: time.Hour}}, opts: []Option{WithPrefixEpochTTL(time.Hour)}},
		{name: "ttl overflow", targets: []Target{{Index: "bad", Key: "bad", TTL: time.Duration(math.MaxInt64), Jitter: time.Nanosecond}}},
		{name: "lock max", opts: []Option{WithLockTTL(time.Duration(math.MaxInt64))}},
		{name: "loader max", opts: []Option{WithLoaderTimeout(time.Duration(math.MaxInt64))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestManager(store, tt.targets, tt.opts...)
			if err == nil {
				t.Fatal("NewManager() error = nil, want invalid config")
			}
		})
	}
}

// TestManagerScopesLoaderEpochWindowToPrefixMutations 验证只有真实存在跨 key 前缀操作的目标受代际窗口约束。
func TestManagerScopesLoaderEpochWindowToPrefixMutations(t *testing.T) {
	t.Run("fixed target", func(t *testing.T) {
		server := runStandaloneRedis(t)
		manager, err := newTestManager(NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()})), []Target{{
			Index: "epoch-window-fixed",
			Key:   "epoch-window-fixed",
			Type:  TypeString,
			Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		}}, WithPrefixEpochTTL(2*time.Second), WithLoaderTimeout(1500*time.Millisecond))
		if err != nil {
			t.Fatalf("newTestManager() error = %v", err)
		}
		var value string
		result, err := manager.LoadThroughWithOptions(context.Background(), defaultPhysicalKey("epoch-window-fixed"), &value, LoadThroughOptions{
			LoaderTimeout: 1800 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("LoadThroughWithOptions() error = %v", err)
		}
		if result.State != LookupStateHit || value != "ok" {
			t.Fatalf("LoadThroughWithOptions() = %#v/%q, want hit/ok", result, value)
		}
	})

	t.Run("distributed untagged prefix", func(t *testing.T) {
		server := runStandaloneRedis(t)
		ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"one": server.Addr()}})
		t.Cleanup(func() { _ = ring.Close() })
		_, err := newTestManager(NewRedisStore(ring), []Target{{
			Index: "epoch-window-ring",
			Key:   "epoch-window-ring:",
			Type:  TypeString,
			Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
				return []Entry{{Key: params.Key, Type: TypeString, Value: "ok"}}, nil
			},
		}}, WithPrefixEpochTTL(2*time.Second), WithLoaderTimeout(1500*time.Millisecond))
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("newTestManager() error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("key prefix introduces distributed hash tag", func(t *testing.T) {
		server := runStandaloneRedis(t)
		ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"one": server.Addr()}})
		t.Cleanup(func() { _ = ring.Close() })
		_, err := newTestManager(NewRedisStore(ring), []Target{{
			Index: "epoch-window-tagged",
			Key:   "epoch-window-tagged:",
			Type:  TypeString,
		}}, WithKeyPrefix("tc:{all}:"), WithPrefixEpochTTL(2*time.Second), WithLoaderTimeout(1500*time.Millisecond))
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("newTestManager() error = %v, want ErrInvalidConfig", err)
		}
	})
}

// TestManagerIgnoresUnrelatedValidateMethod 验证自定义 Store 的通用 Validate 方法不会被构造器误调用。
func TestManagerIgnoresUnrelatedValidateMethod(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := &unrelatedValidateStore{Store: NewRedisStore(client)}
	manager, err := newTestManager(store, []Target{{
		Index: "custom-validate",
		Key:   "custom-validate",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Value: "ok"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := manager.RefreshByKey(ctx, defaultPhysicalKey("custom-validate")); err != nil {
		t.Fatalf("RefreshByKey() error = %v", err)
	}
	if store.validateCalls.Load() != 0 {
		t.Fatalf("unrelated Validate calls = %d, want 0", store.validateCalls.Load())
	}
	if value := client.Get(ctx, defaultPhysicalKey("custom-validate")).Val(); value != "ok" {
		t.Fatalf("cached value = %q, want ok", value)
	}
}

// TestManagerFullRefreshRejectsIncrementalEntry 验证权威快照不会混入旧集合成员。
func TestManagerFullRefreshRejectsIncrementalEntry(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	incremental := true
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "full-overwrite",
		Key:   "full-overwrite:",
		Type:  TypeHash,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			entry := Entry{Key: params.Key + "1", Type: TypeHash, Value: map[string]string{"new": "value"}}
			if incremental {
				entry.Overwrite = Bool(false)
			}
			return []Entry{entry}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.HSet(ctx, defaultPhysicalKey("full-overwrite:1"), "old", "value").Err(); err != nil {
		t.Fatal(err)
	}
	target := manager.prefixTargets[0]
	if err := manager.RefreshByKey(ctx, target.Key); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("incremental full RefreshByKey() error = %v, want ErrInvalidConfig", err)
	}
	if value := client.HGet(ctx, defaultPhysicalKey("full-overwrite:1"), "old").Val(); value != "value" {
		t.Fatalf("old field after rejected refresh = %q, want value", value)
	}
	incremental = false
	if err := manager.RefreshByKey(ctx, target.Key); err != nil {
		t.Fatalf("overwrite full RefreshByKey() error = %v", err)
	}
	values := client.HGetAll(ctx, defaultPhysicalKey("full-overwrite:1")).Val()
	if len(values) != 1 || values["new"] != "value" {
		t.Fatalf("full snapshot fields = %#v, want only new field", values)
	}
}

// TestManagerPrefixRootRejectsFields 验证前缀全量入口不会被 fields 误降级成局部刷新。
func TestManagerPrefixRootRejectsFields(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "prefix-fields",
		Key:   "prefix-fields:",
		Type:  TypeHash,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			loaderCalls.Add(1)
			return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]string{"name": "new"}}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	if err := client.HSet(ctx, "prefix-fields:1", "name", "one").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, "prefix-fields:2", "name", "two").Err(); err != nil {
		t.Fatal(err)
	}
	target := manager.prefixTargets[0]
	if err := manager.RefreshByKey(ctx, target.Key, "name"); !errors.Is(err, ErrPartialHashUnsupported) {
		t.Fatalf("RefreshByKey(prefix, fields) error = %v, want ErrPartialHashUnsupported", err)
	}
	var value map[string]string
	_, err = manager.GetOrRefreshWithOptions(ctx, target.Key, &value, LoadThroughOptions{Fields: []string{"name"}})
	if !errors.Is(err, ErrPartialHashUnsupported) {
		t.Fatalf("GetOrRefreshWithOptions(prefix, fields) error = %v, want ErrPartialHashUnsupported", err)
	}
	if loaderCalls.Load() != 0 {
		t.Fatalf("loader calls = %d, want 0", loaderCalls.Load())
	}
	if first := client.HGet(ctx, "prefix-fields:1", "name").Val(); first != "one" {
		t.Fatalf("first sibling = %q, want one", first)
	}
	if second := client.HGet(ctx, "prefix-fields:2", "name").Val(); second != "two" {
		t.Fatalf("second sibling = %q, want two", second)
	}
}

// TestManagerWaitPolicyHandlesMaxDuration 验证退避次数计算不会按超大Duration循环。
func TestManagerWaitPolicyHandlesMaxDuration(t *testing.T) {
	manager := &Manager{lockTTL: time.Duration(math.MaxInt64), loaderTTL: time.Duration(math.MaxInt64)}
	step, times := manager.waitPolicy(Target{})
	if step <= 0 || times <= 0 {
		t.Fatalf("waitPolicy() = %s/%d, want positive", step, times)
	}
}

// TestManagerRejectsUnsafePerCallLoaderWindow 验证前缀操作可达时单次超时不能越过代际安全窗口。
func TestManagerRejectsUnsafePerCallLoaderWindow(t *testing.T) {
	server := runStandaloneRedis(t)
	var calls atomic.Int64
	manager, err := newTestManager(NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()})), []Target{{
		Index: "call-window",
		Key:   "call-window:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			calls.Add(1)
			return nil, nil
		},
	}}, WithPrefixEpochTTL(2*time.Second), WithLoaderTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value string
	_, err = manager.LoadThroughWithOptions(context.Background(), defaultPhysicalKey("call-window:1"), &value, LoadThroughOptions{LoaderTimeout: 1500 * time.Millisecond})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("LoadThroughWithOptions() error = %v, want ErrInvalidConfig", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("loader calls = %d, want 0", calls.Load())
	}
}

// TestRefreshWriteEntriesDoesNotDuplicateEmptyCollection 验证一个空集合只生成一个 marker。
func TestRefreshWriteEntriesDoesNotDuplicateEmptyCollection(t *testing.T) {
	manager := &Manager{}
	entries := manager.refreshWriteEntries([]Entry{{Key: "empty-list", Type: TypeList, Value: []string{}}})
	if len(entries) != 1 || entries[0].Key != manager.emptyCollectionKey("empty-list") {
		t.Fatalf("write entries = %#v, want one empty collection marker", entries)
	}
}

// TestManagerIndexDisableInvalidatesReady 验证关闭索引期间的写入不会让后续实例复用旧ready。
func TestManagerIndexDisableInvalidatesReady(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	store := NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	on, err := newTestManager(store, []Target{{
		Index: "toggle-index",
		Key:   "toggle-index:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: defaultPhysicalKey("toggle-index:1"), Value: "one"}}, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager(on) error = %v", err)
	}
	if err := on.RefreshByKey(ctx, defaultPhysicalKey("toggle-index:")); err != nil {
		t.Fatalf("full refresh error = %v", err)
	}
	readyKey := on.prefixIndexReadyKey(on.prefixTargets[0])
	if store.client.Exists(ctx, readyKey).Val() != 1 {
		t.Fatal("prefix index ready missing after full refresh")
	}
	off, err := newTestManager(store, []Target{{
		Index: "toggle-index",
		Key:   "toggle-index:",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Value: "two"}}, nil
		},
	}}, WithPrefixKeyIndex(false))
	if err != nil {
		t.Fatalf("newTestManager(off) error = %v", err)
	}
	if err := off.RefreshByKey(ctx, defaultPhysicalKey("toggle-index:2")); err != nil {
		t.Fatalf("off index refresh error = %v", err)
	}
	if store.client.Exists(ctx, readyKey).Val() != 0 {
		t.Fatal("index-off write preserved stale ready")
	}
	onAgain, err := newTestManager(store, []Target{{Index: "toggle-index", Key: "toggle-index:", Type: TypeString}})
	if err != nil {
		t.Fatalf("newTestManager(on again) error = %v", err)
	}
	if err := onAgain.DeleteByPrefix(ctx, defaultPhysicalKey("toggle-index:")); err != nil {
		t.Fatalf("DeleteByPrefix() error = %v", err)
	}
	if store.client.Exists(ctx, defaultPhysicalKey("toggle-index:1"), defaultPhysicalKey("toggle-index:2")).Val() != 0 {
		t.Fatal("SCAN fallback missed index-off write")
	}
}

// TestManagerWaitRebuiltRejectsStaleMarker 验证锁消失时不会接受未观察owner的旧完成标记。
func TestManagerWaitRebuiltRejectsStaleMarker(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	manager, err := newTestManager(NewRedisStore(client), nil, WithWait(5*time.Millisecond, 20))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := "stale-owner"
	if err := client.Set(ctx, manager.lockKey(key), "new-owner", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, manager.rebuildResultKey(key), "old-owner", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = client.Del(ctx, manager.lockKey(key)).Err()
	}()
	outcome, err := manager.waitRebuilt(ctx, Target{Index: "stale"}, key, key, key, "")
	if err != nil {
		t.Fatalf("waitRebuilt() error = %v", err)
	}
	if outcome != waitRebuildRetry {
		t.Fatalf("waitRebuilt() outcome = %v, want retry", outcome)
	}
}

// TestManagerFastOwnerCompletionDoesNotReload 验证竞争方原子观察owner后可接受其已完成结果。
func TestManagerFastOwnerCompletionDoesNotReload(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	redisStore := NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	observed := make(chan struct{})
	resume := make(chan struct{})
	waiterStore := &fastOwnerBarrierStore{RedisStore: redisStore, observed: observed, resume: resume}
	loaderStarted := make(chan struct{})
	loaderRelease := make(chan struct{})
	var calls atomic.Int64
	target := Target{
		Index: "fast-owner",
		Key:   "fast-owner",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			if calls.Add(1) == 1 {
				close(loaderStarted)
				<-loaderRelease
			}
			return []Entry{{Key: params.Key, Value: "ok"}}, nil
		},
	}
	leader, err := newTestManager(redisStore, []Target{target}, WithWait(5*time.Millisecond, 20))
	if err != nil {
		t.Fatalf("newTestManager(leader) error = %v", err)
	}
	waiter, err := newTestManager(waiterStore, []Target{target}, WithWait(5*time.Millisecond, 20))
	if err != nil {
		t.Fatalf("newTestManager(waiter) error = %v", err)
	}
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- leader.RefreshByKey(ctx, defaultPhysicalKey("fast-owner")) }()
	<-loaderStarted
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- waiter.RefreshByKey(ctx, defaultPhysicalKey("fast-owner")) }()
	<-observed
	close(loaderRelease)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader RefreshByKey() error = %v", err)
	}
	close(resume)
	if err := <-waiterDone; err != nil {
		t.Fatalf("waiter RefreshByKey() error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader calls = %d, want 1", got)
	}
}

// TestManagerLookupMetricRecordsFinalStateOnce 验证读穿只记录一次最终状态。
func TestManagerLookupMetricRecordsFinalStateOnce(t *testing.T) {
	server := runStandaloneRedis(t)
	metrics := &recordingMetrics{}
	manager, err := newTestManager(NewRedisStore(redis.NewClient(&redis.Options{Addr: server.Addr()})), []Target{{
		Index: "metric-once",
		Key:   "metric-once",
		Type:  TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			return []Entry{{Key: params.Key, Value: "ok"}}, nil
		},
	}}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value string
	if _, err := manager.GetOrRefresh(context.Background(), defaultPhysicalKey("metric-once"), &value); err != nil {
		t.Fatalf("GetOrRefresh() error = %v", err)
	}
	if metrics.lookupHit.Load() != 1 || metrics.lookupMiss.Load() != 0 {
		t.Fatalf("lookup metrics hit/miss = %d/%d, want 1/0", metrics.lookupHit.Load(), metrics.lookupMiss.Load())
	}
}

// blockingPrefixReplaceStore 在全量快照提交阶段提供确定性阻塞点。
type blockingPrefixReplaceStore struct {
	*RedisStore
	replaceStarted chan struct{}
	replaceRelease chan struct{}
}

// prefixAttemptStore 记录同一前缀leadership锁的第二次竞争。
type prefixAttemptStore struct {
	*RedisStore
	lockKey string
	calls   atomic.Int64
	second  chan struct{}
}

// AcquireRefreshLock 在原子抢锁路径保留相同的第二次请求屏障。
func (s *prefixAttemptStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	if key == s.lockKey && s.calls.Add(1) == 2 {
		close(s.second)
	}
	return s.RedisStore.AcquireRefreshLock(ctx, key, value, ttl)
}

// blockingMutationStore 在单key业务写入时阻塞，验证full起始代际锁序。
type blockingMutationStore struct {
	*RedisStore
	started chan struct{}
	release chan struct{}
	once    atomic.Bool
}

// ApplyMutation 在安全合并写入路径中阻塞第一次业务写入。
func (s *blockingMutationStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	if len(mutation.WriteEntries) > 0 && s.once.CompareAndSwap(false, true) {
		close(s.started)
		<-s.release
	}
	return s.RedisStore.ApplyMutation(ctx, mutation)
}

// ReplacePrefix 阻塞安全替换，便于验证短提交锁。
func (s *blockingPrefixReplaceStore) ReplacePrefix(ctx context.Context, mutation PrefixReplaceMutation) (int64, error) {
	close(s.replaceStarted)
	<-s.replaceRelease
	return s.RedisStore.ReplacePrefix(ctx, mutation)
}

// failingPrefixReplaceStore 模拟安全前缀替换在写入前失败。
type failingPrefixReplaceStore struct{ *RedisStore }

// ReplacePrefix 返回故障，旧业务缓存应保持可读。
func (s *failingPrefixReplaceStore) ReplacePrefix(ctx context.Context, mutation PrefixReplaceMutation) (int64, error) {
	return 0, errors.Errorf("mock replace failure")
}

// recordingPrefixReplaceStore 统计安全替换调用，验证本地校验发生在Store之前。
type recordingPrefixReplaceStore struct {
	*RedisStore
	calls atomic.Int64
}

// epochCountingStore 统计刷新路径的 Store.Read 与已废弃单 key 删除代际写入。
type epochCountingStore struct {
	*RedisStore
	reads           atomic.Int64
	keyDeleteWrites atomic.Int64
}

// Read 记录 Manager 经 Store 发起的读取；目标用例中这些读取只可能来自 epoch 校验。
func (s *epochCountingStore) Read(ctx context.Context, key string, typ CacheType) (any, error) {
	s.reads.Add(1)
	return s.RedisStore.Read(ctx, key, typ)
}

// ApplyMutation 记录单 key 删除代际写入，同时透传真实 Redis 原子变更。
func (s *epochCountingStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	for _, entry := range mutation.WriteEntries {
		if strings.Contains(entry.Key, ":delete:epoch:") {
			s.keyDeleteWrites.Add(1)
		}
	}
	return s.RedisStore.ApplyMutation(ctx, mutation)
}

// ReplacePrefix 记录调用后透传真实实现。
func (s *recordingPrefixReplaceStore) ReplacePrefix(ctx context.Context, mutation PrefixReplaceMutation) (int64, error) {
	s.calls.Add(1)
	return s.RedisStore.ReplacePrefix(ctx, mutation)
}

// basicStore 只暴露基础 Store，验证 Manager 不做破坏性前缀降级。
type basicStore struct{ Store }

// pageOnlyStore 仅在基础Store之外提供Hash分页读取与字段删除能力。
type pageOnlyStore struct {
	Store
	page   CollectionPageStore
	fields HashFieldsStore
}

// ReadPage 转发集合分页读取，不额外暴露字段空值registry能力。
func (s *pageOnlyStore) ReadPage(ctx context.Context, key string, typ CacheType, options ReadPageOptions) (ReadPageResult, error) {
	return s.page.ReadPage(ctx, key, typ, options)
}

// DeleteHashFields 转发带锁字段删除，不额外暴露字段空值registry能力。
func (s *pageOnlyStore) DeleteHashFields(ctx context.Context, guards []LockGuard, key string, registryKey string, fields []string) error {
	return s.fields.DeleteHashFields(ctx, guards, key, registryKey, fields)
}

// fastOwnerBarrierStore 在竞争失败返回Manager前等待leader完成，用于稳定复现快owner窗口。
type fastOwnerBarrierStore struct {
	*RedisStore
	observed chan struct{}
	resume   chan struct{}
}

// AcquireRefreshLock 原子观察当前owner，并在首次竞争失败后提供测试屏障。
func (s *fastOwnerBarrierStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	locked, owner, err := s.RedisStore.AcquireRefreshLock(ctx, key, value, ttl)
	if err == nil && !locked {
		close(s.observed)
		select {
		case <-ctx.Done():
			return false, owner, errors.Tag(ctx.Err())
		case <-s.resume:
		}
	}
	return locked, owner, errors.Tag(err)
}

// hiddenFaultStore 注入隐藏空值索引和状态切换故障。
type hiddenFaultStore struct {
	*RedisStore
	failAdd      bool
	failReplace  bool
	replaceCalls atomic.Int64
}

// unrelatedValidateStore 带有业务自用 Validate 方法，但没有实现 table-cache 专用校验接口。
type unrelatedValidateStore struct {
	Store
	validateCalls atomic.Int64
}

// Validate 模拟第三方 Store 自有校验语义，不应被 NewManager 隐式调用。
func (s *unrelatedValidateStore) Validate() error {
	s.validateCalls.Add(1)
	return errors.Errorf("unrelated validate should not be called")
}

// ApplyMutation 在安全合并变更的索引添加阶段注入故障。
func (s *hiddenFaultStore) ApplyMutation(ctx context.Context, mutation StoreMutation) error {
	if s.failAdd && len(mutation.AddIndex) > 0 {
		return errors.Errorf("mock add index failure")
	}
	return s.RedisStore.ApplyMutation(ctx, mutation)
}

// ReplaceWithMarker 在专用安全状态切换阶段注入故障。
func (s *hiddenFaultStore) ReplaceWithMarker(ctx context.Context, guards []LockGuard, marker Entry, deleteKeys ...string) error {
	s.replaceCalls.Add(1)
	if s.failReplace {
		return errors.Errorf("mock hidden replace failure")
	}
	return s.RedisStore.ReplaceWithMarker(ctx, guards, marker, deleteKeys...)
}
