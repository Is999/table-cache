package tablecache

import (
	"context"
	stdErrors "errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// delayedAcquireStore 把第一次锁申请暂停到测试显式放行，用于复现 miss 后的快速 owner 窗口。
type delayedAcquireStore struct {
	*RedisStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

// AcquireRefreshLock 暂停第一次锁申请，其余调用复用真实 Redis 锁。
func (s *delayedAcquireStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	s.once.Do(func() {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	})
	return s.RedisStore.AcquireRefreshLock(ctx, key, value, ttl)
}

// notifyAcquireStore 在第一次锁申请时通知测试，不改变真实锁行为。
type notifyAcquireStore struct {
	*RedisStore
	started chan struct{}
	once    sync.Once
}

// AcquireRefreshLock 通知第一次锁申请后复用真实 Redis 锁。
func (s *notifyAcquireStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	s.once.Do(func() { close(s.started) })
	return s.RedisStore.AcquireRefreshLock(ctx, key, value, ttl)
}

// neverOwnedStore 模拟 owner 持续轮换且方法忽略 ctx 的自定义 Store。
type neverOwnedStore struct{}

// AcquireRefreshLock 始终返回竞争失败。
func (*neverOwnedStore) AcquireRefreshLock(context.Context, string, string, time.Duration) (bool, string, error) {
	return false, "other-owner", nil
}

// ApplyMutation 实现 Store 变更契约。
func (*neverOwnedStore) ApplyMutation(context.Context, StoreMutation) error { return nil }

// Exists 实现 Store 存在性契约。
func (*neverOwnedStore) Exists(context.Context, string) (bool, error) { return false, nil }

// ExistsMulti 实现 Store 批量存在性契约。
func (*neverOwnedStore) ExistsMulti(context.Context, ...string) (map[string]bool, error) {
	return map[string]bool{}, nil
}

// Read 始终返回 miss，模拟竞争 owner 已释放但未发布结果。
func (*neverOwnedStore) Read(context.Context, string, CacheType) (any, error) {
	return nil, ErrCacheMiss
}

// RefreshLock 实现 Store 续期契约。
func (*neverOwnedStore) RefreshLock(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}

// ReleaseLock 实现 Store 释放契约。
func (*neverOwnedStore) ReleaseLock(context.Context, string, string) (bool, error) {
	return false, nil
}

// deniedTopologyStore 声明强前缀能力不可用，并记录是否错误进入 Loader 后写删。
type deniedTopologyStore struct {
	Store
	replaceCalls atomic.Int64
	deleteCalls  atomic.Int64
}

// readOverrideStore 模拟嵌入 RedisStore 但重写公开 Read 语义的外部装饰 Store。
type readOverrideStore struct {
	*RedisStore
	key       string
	readCalls atomic.Int64
}

// Read 为指定业务 key 返回装饰层值，其它内部元信息仍委托 RedisStore。
func (s *readOverrideStore) Read(ctx context.Context, key string, typ CacheType) (any, error) {
	if key == s.key {
		s.readCalls.Add(1)
		return "wrapped", nil
	}
	return s.RedisStore.Read(ctx, key, typ)
}

// roundTripCountingHook 统计 go-redis 发出的单命令或 Pipeline 网络批次。
type roundTripCountingHook struct {
	roundTrips  atomic.Int64
	maxCommands atomic.Int64
}

// DialHook 不改变 Redis 建连行为。
func (*roundTripCountingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

// ProcessHook 统计一次普通命令批次。
func (h *roundTripCountingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.roundTrips.Add(1)
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook 统计一次 Pipeline 网络批次。
func (h *roundTripCountingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.roundTrips.Add(1)
		count := int64(len(cmds))
		for current := h.maxCommands.Load(); count > current && !h.maxCommands.CompareAndSwap(current, count); current = h.maxCommands.Load() {
		}
		return next(ctx, cmds)
	}
}

// AllowsPrefixMutation 明确拒绝强前缀能力。
func (*deniedTopologyStore) AllowsPrefixMutation(string) bool { return false }

// ReplacePrefix 记录不应到达的前缀替换。
func (s *deniedTopologyStore) ReplacePrefix(context.Context, PrefixReplaceMutation) (int64, error) {
	s.replaceCalls.Add(1)
	return 0, nil
}

// DeletePatternGuarded 记录不应到达的前缀扫描删除。
func (s *deniedTopologyStore) DeletePatternGuarded(context.Context, string, int64, []LockGuard) (int64, error) {
	s.deleteCalls.Add(1)
	return 0, nil
}

// TestGetOrRefreshHitUsesOneRedisRoundTrip 验证默认无空 marker 的稳态 String 命中不会串行读取 generation。
func TestGetOrRefreshHitUsesOneRedisRoundTrip(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hook := &roundTripCountingHook{}
	client.AddHook(hook)
	manager, err := newTestManager(NewRedisStore(client), []Target{{Index: "snapshot-hit", Key: "snapshot-hit", Type: TypeString}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := defaultPhysicalKey("snapshot-hit")
	if err := client.Set(context.Background(), key, "value", time.Minute).Err(); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	hook.roundTrips.Store(0)
	var value string
	result, err := manager.GetOrRefresh(context.Background(), key, &value)
	if err != nil {
		t.Fatalf("GetOrRefresh() error = %v", err)
	}
	if result.State != LookupStateHit || value != "value" {
		t.Fatalf("result=%+v value=%q, want hit/value", result, value)
	}
	if calls := hook.roundTrips.Load(); calls != 1 {
		t.Fatalf("Redis round trips = %d, want 1", calls)
	}
}

// TestRefreshSnapshotRejectsOversizedFieldsBeforeRedis 验证快照优化不会绕过 Hash fields 单页硬上限。
func TestRefreshSnapshotRejectsOversizedFieldsBeforeRedis(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hook := &roundTripCountingHook{}
	client.AddHook(hook)
	manager, err := newTestManager(NewRedisStore(client), []Target{{Index: "snapshot-fields", Key: "snapshot-fields", Type: TypeHash}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	fields := make([]string, defaultMaxCollectionCount+1)
	for index := range fields {
		fields[index] = fmt.Sprintf("field-%d", index)
	}
	hook.roundTrips.Store(0)
	var value map[string]string
	_, err = manager.GetOrRefreshWithOptions(context.Background(), defaultPhysicalKey("snapshot-fields"), &value, LoadThroughOptions{Fields: fields})
	if !stdErrors.Is(err, ErrCollectionTooLarge) {
		t.Fatalf("GetOrRefreshWithOptions(oversized fields) error = %v, want ErrCollectionTooLarge", err)
	}
	if calls := hook.roundTrips.Load(); calls != 0 {
		t.Fatalf("Redis round trips = %d, want 0 after local rejection", calls)
	}
}

// TestStringRefreshSnapshotFailsClosedOnWrongType 验证 String 类型污染不会被快照路径静默当成 miss 覆盖。
func TestStringRefreshSnapshotFailsClosedOnWrongType(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	var loaderCalls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "snapshot-wrong-type", Key: "snapshot-wrong-type", Type: TypeString,
		Loader: func(context.Context, LoadParams) ([]Entry, error) {
			loaderCalls.Add(1)
			return nil, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := defaultPhysicalKey("snapshot-wrong-type")
	if err := client.HSet(context.Background(), key, "field", "value").Err(); err != nil {
		t.Fatalf("HSet() error = %v", err)
	}
	var value string
	if _, err := manager.GetOrRefresh(context.Background(), key, &value); err == nil {
		t.Fatal("GetOrRefresh(wrong type) error = nil")
	}
	if calls := loaderCalls.Load(); calls != 0 {
		t.Fatalf("loader calls = %d, want 0", calls)
	}
}

// TestRedisStoreExistsMultiUsesBoundedPipelines 验证公开批量存在性读取不会构造无界 Pipeline。
func TestRedisStoreExistsMultiUsesBoundedPipelines(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hook := &roundTripCountingHook{}
	client.AddHook(hook)
	store := NewRedisStore(client)
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	hook.roundTrips.Store(0)
	hook.maxCommands.Store(0)
	keys := make([]string, defaultWriteBatchChunkSize*2+1)
	for index := range keys {
		keys[index] = fmt.Sprintf("exists-%d", index)
	}
	values, err := store.ExistsMulti(context.Background(), keys...)
	if err != nil {
		t.Fatalf("ExistsMulti() error = %v", err)
	}
	if len(values) != len(keys) {
		t.Fatalf("ExistsMulti() result size = %d, want %d", len(values), len(keys))
	}
	if calls := hook.roundTrips.Load(); calls != 3 {
		t.Fatalf("Redis round trips = %d, want 3 bounded pipelines", calls)
	}
	if maxCommands := hook.maxCommands.Load(); maxCommands != defaultWriteBatchChunkSize {
		t.Fatalf("max pipeline commands = %d, want %d", maxCommands, defaultWriteBatchChunkSize)
	}
}

// TestRefreshSnapshotDoesNotBypassDecoratedRead 验证内部快照优化不会绕过外部 Store 的公开 Read 实现。
func TestRefreshSnapshotDoesNotBypassDecoratedRead(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	key := defaultPhysicalKey("snapshot-wrapper")
	store := &readOverrideStore{RedisStore: NewRedisStore(client), key: key}
	manager, err := newTestManager(store, []Target{{Index: "snapshot-wrapper", Key: "snapshot-wrapper", Type: TypeString}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	var value string
	result, err := manager.GetOrRefresh(context.Background(), key, &value)
	if err != nil {
		t.Fatalf("GetOrRefresh() error = %v", err)
	}
	if result.State != LookupStateHit || value != "wrapped" || store.readCalls.Load() != 1 {
		t.Fatalf("result=%+v value=%q readCalls=%d, want decorated hit", result, value, store.readCalls.Load())
	}
}

// TestManagerLoadThroughClosesFastOwnerWindow 验证数据命中与不落 marker 的空结果都只回源一次。
func TestManagerLoadThroughClosesFastOwnerWindow(t *testing.T) {
	for _, test := range []struct {
		name      string
		withValue bool
		wantState LookupState
	}{
		{name: "value", withValue: true, wantState: LookupStateHit},
		{name: "empty without marker", wantState: LookupStateMiss},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := runStandaloneRedis(t)
			clientA := redis.NewClient(&redis.Options{Addr: server.Addr()})
			clientB := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() {
				_ = clientA.Close()
				_ = clientB.Close()
			})
			loaderStarted := make(chan struct{})
			releaseLoader := make(chan struct{})
			var loaderCalls atomic.Int64
			loader := func(ctx context.Context, params LoadParams) ([]Entry, error) {
				if loaderCalls.Add(1) == 1 {
					close(loaderStarted)
					select {
					case <-releaseLoader:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				if test.withValue {
					return []Entry{{Key: params.Key, Type: TypeString, Value: "value"}}, nil
				}
				return nil, nil
			}
			target := Target{Index: "fast-owner", Key: "fast-owner", Type: TypeString, Loader: loader}
			managerA, err := newTestManager(NewRedisStore(clientA), []Target{target}, WithWait(time.Millisecond, 200))
			if err != nil {
				t.Fatalf("newTestManager(A) error = %v", err)
			}
			storeB := &delayedAcquireStore{
				RedisStore: NewRedisStore(clientB),
				started:    make(chan struct{}),
				release:    make(chan struct{}),
			}
			managerB, err := newTestManager(storeB, []Target{target}, WithWait(time.Millisecond, 200))
			if err != nil {
				t.Fatalf("newTestManager(B) error = %v", err)
			}
			key := defaultPhysicalKey("fast-owner")
			type lookupResponse struct {
				result LookupResult
				err    error
			}
			responseA := make(chan lookupResponse, 1)
			go func() {
				var value string
				result, err := managerA.GetOrRefresh(context.Background(), key, &value)
				responseA <- lookupResponse{result: result, err: err}
			}()
			<-loaderStarted
			responseB := make(chan lookupResponse, 1)
			go func() {
				var value string
				result, err := managerB.GetOrRefresh(context.Background(), key, &value)
				responseB <- lookupResponse{result: result, err: err}
			}()
			<-storeB.started
			close(releaseLoader)
			first := <-responseA
			if first.err != nil {
				t.Fatalf("GetOrRefresh(A) error = %v", first.err)
			}
			resultKey := managerA.refreshResultKey(key, nil, refreshOptions{requested: true, missTriggered: true})
			ownerA := clientA.Get(context.Background(), resultKey).Val()
			if ownerA == "" {
				t.Fatal("first refresh result owner is empty")
			}
			close(storeB.release)
			second := <-responseB
			if second.err != nil {
				t.Fatalf("GetOrRefresh(B) error = %v", second.err)
			}
			if second.result.State != test.wantState || !second.result.Refreshed {
				t.Fatalf("GetOrRefresh(B) result = %+v, want state=%s refreshed", second.result, test.wantState)
			}
			if calls := loaderCalls.Load(); calls != 1 {
				t.Fatalf("loader calls = %d, want 1", calls)
			}
			ownerB := clientB.Get(context.Background(), resultKey).Val()
			if ownerB == "" || ownerB == ownerA {
				t.Fatalf("second owner = %q, want non-empty value different from %q", ownerB, ownerA)
			}
		})
	}
}

// TestDistributedScheduledAndLoadThroughScopesStayIsolated 验证跨实例 scheduled 空结果不会吞掉请求型空值语义。
func TestDistributedScheduledAndLoadThroughScopesStayIsolated(t *testing.T) {
	server := runStandaloneRedis(t)
	clientA := redis.NewClient(&redis.Options{Addr: server.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	var loaderCalls atomic.Int64
	loader := func(ctx context.Context, _ LoadParams) ([]Entry, error) {
		if loaderCalls.Add(1) == 1 {
			close(loaderStarted)
			select {
			case <-releaseLoader:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, nil
	}
	target := Target{
		Index: "distributed-scope", Key: "distributed-scope", Type: TypeString,
		RefreshAll: true, AllowEmptyMarker: true, Loader: loader,
	}
	managerA, err := newTestManager(NewRedisStore(clientA), []Target{target}, WithWait(time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager(A) error = %v", err)
	}
	storeB := &notifyAcquireStore{RedisStore: NewRedisStore(clientB), started: make(chan struct{})}
	managerB, err := newTestManager(storeB, []Target{target}, WithWait(time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager(B) error = %v", err)
	}
	scheduledDone := make(chan error, 1)
	go func() { scheduledDone <- managerA.RefreshAll(context.Background()) }()
	<-loaderStarted
	type lookupResponse struct {
		result LookupResult
		err    error
	}
	requestedDone := make(chan lookupResponse, 1)
	go func() {
		var value string
		result, err := managerB.GetOrRefresh(context.Background(), defaultPhysicalKey("distributed-scope"), &value)
		requestedDone <- lookupResponse{result: result, err: err}
	}()
	<-storeB.started
	close(releaseLoader)
	if err := <-scheduledDone; err != nil {
		t.Fatalf("RefreshAll() error = %v", err)
	}
	requested := <-requestedDone
	if requested.err != nil {
		t.Fatalf("GetOrRefresh() error = %v", requested.err)
	}
	if requested.result.State != LookupStateEmpty || !requested.result.Refreshed {
		t.Fatalf("GetOrRefresh() result = %+v, want refreshed empty", requested.result)
	}
	if calls := loaderCalls.Load(); calls != 2 {
		t.Fatalf("loader calls = %d, want 2", calls)
	}
}

// TestRefreshOperationHasTotalBudget 验证 IgnoreCancel 下 owner 持续轮换也会受内部总预算终止。
func TestRefreshOperationHasTotalBudget(t *testing.T) {
	manager, err := NewManager(&neverOwnedStore{}, []Target{{
		Index: "total-budget", Key: "total-budget", Type: TypeString,
		Loader: func(context.Context, LoadParams) ([]Entry, error) {
			t.Fatal("loader must not run without the lock")
			return nil, nil
		},
	}}, WithLockTTL(minLockTTL), WithLoaderTimeout(time.Millisecond), WithWait(minWaitStep, 1), WithRebuildContextPolicy(RebuildContextIgnoreCancel))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	startedAt := time.Now()
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("total-budget"))
	if !stdErrors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RefreshByKey() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("RefreshByKey() elapsed = %s, want bounded completion", elapsed)
	}
}

// TestDeniedTopologyFailsBeforeStrongPrefixWork 验证 topology=false 不能被空 validator 接口绕过。
func TestDeniedTopologyFailsBeforeStrongPrefixWork(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := &deniedTopologyStore{Store: NewRedisStore(client)}
	var loaderCalls atomic.Int64
	manager, err := newTestManager(store, []Target{{
		Index: "denied-prefix", Key: "denied-prefix:", Type: TypeString,
		Loader: func(context.Context, LoadParams) ([]Entry, error) {
			loaderCalls.Add(1)
			return nil, nil
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	prefix := defaultPhysicalKey("denied-prefix:")
	if err := manager.RefreshByKey(context.Background(), prefix); !stdErrors.Is(err, ErrRedisTopologyUnsupported) {
		t.Fatalf("RefreshByKey() error = %v, want ErrRedisTopologyUnsupported", err)
	}
	if err := manager.DeleteByPrefix(context.Background(), prefix); !stdErrors.Is(err, ErrRedisTopologyUnsupported) {
		t.Fatalf("DeleteByPrefix() error = %v, want ErrRedisTopologyUnsupported", err)
	}
	if loaderCalls.Load() != 0 || store.replaceCalls.Load() != 0 || store.deleteCalls.Load() != 0 {
		t.Fatalf("strong prefix work escaped fail-close: loader=%d replace=%d delete=%d", loaderCalls.Load(), store.replaceCalls.Load(), store.deleteCalls.Load())
	}
}

// TestRuntimeResourceLimitsFailAtConstruction 验证危险资源参数不会延迟到流量期才失控。
func TestRuntimeResourceLimitsFailAtConstruction(t *testing.T) {
	managerOptions := []Option{
		WithLoaderConcurrency(maxWorkerConcurrency + 1),
		WithRefreshConcurrency(maxWorkerConcurrency + 1),
		WithPrefixDeleteConcurrency(maxWorkerConcurrency + 1),
		WithScanCount(maxScanCount + 1),
		WithLockTTL(minLockTTL - time.Nanosecond),
		WithLockRenewInterval(minLockRenewInterval - time.Nanosecond),
		WithWait(minWaitStep-time.Nanosecond, 1),
		WithWait(minWaitStep, maxWaitAttempts+1),
		WithRebuildResultTTL(maxRedisBaseTTL + 1),
		WithPrefixEpochTTL(maxRedisBaseTTL + 1),
		WithPrefixKeyIndexTTL(maxRedisBaseTTL + 1),
		WithEmptyMarker("empty", maxRedisBaseTTL+1),
		nil,
	}
	if _, err := NewManager(&typedNilStore{}, []Target{{Index: "ttl-limit", Key: "ttl-limit", TTL: maxRedisBaseTTL + 1}}); !stdErrors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewManager(target TTL) error = %v, want ErrInvalidConfig", err)
	}
	for index, option := range managerOptions {
		if _, err := NewManager(&typedNilStore{}, nil, option); !stdErrors.Is(err, ErrInvalidConfig) {
			t.Fatalf("manager option[%d] error = %v, want ErrInvalidConfig", index, err)
		}
	}

	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	redisOptions := []RedisStoreOption{
		WithUnlinkChunkSize(maxUnlinkChunkSize + 1),
		WithPipelineRetries(maxPipelineRetries + 1),
		WithDefaultJitterRatio(maxDefaultJitterRatio + 0.01),
		nil,
	}
	for index, option := range redisOptions {
		if err := NewRedisStore(client, option).ValidateTablecacheStore(); err == nil {
			t.Fatalf("redis option[%d] error = nil", index)
		}
	}
	if got := normalizeScanCount(math.MaxInt64); got != maxScanCount {
		t.Fatalf("normalizeScanCount(MaxInt64) = %d, want %d", got, maxScanCount)
	}
	var retryCalls atomic.Int64
	retryErr := stdErrors.New("retry")
	if err := execPipelineWithRetry(context.Background(), math.MaxInt, func() error {
		retryCalls.Add(1)
		return retryErr
	}); !stdErrors.Is(err, retryErr) {
		t.Fatalf("execPipelineWithRetry() error = %v, want retry error", err)
	}
	if calls := retryCalls.Load(); calls != maxPipelineRetries+1 {
		t.Fatalf("retry calls = %d, want %d", calls, maxPipelineRetries+1)
	}
}

// TestClusterReplicaReadRoutingIsRejected 验证缓存一致性依赖的 Cluster 读取不会被路由到副本。
func TestClusterReplicaReadRoutingIsRejected(t *testing.T) {
	for name, options := range map[string]*redis.ClusterOptions{
		"readonly":         {Addrs: []string{"127.0.0.1:1"}, ReadOnly: true},
		"route by latency": {Addrs: []string{"127.0.0.1:1"}, RouteByLatency: true},
		"route randomly":   {Addrs: []string{"127.0.0.1:1"}, RouteRandomly: true},
	} {
		t.Run(name, func(t *testing.T) {
			client := redis.NewClusterClient(options)
			t.Cleanup(func() { _ = client.Close() })
			if err := NewRedisStore(client).ValidateTablecacheStore(); err == nil {
				t.Fatal("ValidateTablecacheStore() error = nil")
			}
		})
	}
}

// TestSafeDefaultsAndTypedNilOptions 验证高风险默认值与可选接口都在构造期收口。
func TestSafeDefaultsAndTypedNilOptions(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(client)
	if store.maxCollectionReadCount != defaultMaxCollectionCount || store.maxCollectionWriteCount != defaultMaxCollectionCount {
		t.Fatalf("default collection limits = %d/%d, want %d", store.maxCollectionReadCount, store.maxCollectionWriteCount, defaultMaxCollectionCount)
	}
	var logger *recordingLogger
	var metrics *recordingMetrics
	manager, err := NewManager(&typedNilStore{}, nil, WithLogger(logger), WithMetrics(metrics))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	if manager.logger != nil || manager.metrics != nil || manager.allowScanFallback || !manager.redactLogKeys {
		t.Fatalf("safe defaults/logger/metrics not closed: logger=%v metrics=%v scan=%v redact=%v", manager.logger, manager.metrics, manager.allowScanFallback, manager.redactLogKeys)
	}
	var registerer *prometheus.Registry
	if _, err := NewPrometheusMetrics(WithPrometheusNamespace("typed_nil_registerer"), WithPrometheusRegisterer(registerer)); err != nil {
		t.Fatalf("NewPrometheusMetrics(typed nil) error = %v", err)
	}
	var option PrometheusMetricsOption
	if _, err := NewPrometheusMetrics(option); !stdErrors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewPrometheusMetrics(nil option) error = %v, want ErrInvalidConfig", err)
	}
}

// TestPrefixDeleteEpochEvictionInvalidatesStaleRefresh 验证删除代际丢失时旧单 key Loader 不能复活数据。
func TestPrefixDeleteEpochEvictionInvalidatesStaleRefresh(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "epoch-evict", Key: "epoch-evict:", Type: TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			close(loaderStarted)
			select {
			case <-releaseLoader:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return []Entry{{Key: params.Key, Type: TypeString, Value: "stale"}}, nil
		},
	}}, WithWait(time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := defaultPhysicalKey("epoch-evict:1")
	prefix := defaultPhysicalKey("epoch-evict:")
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- manager.RefreshByKey(context.Background(), key) }()
	<-loaderStarted
	target := manager.prefixTargetsM[prefix]
	deleteEpochKey := manager.prefixDeleteEpochKey(target.Key)
	if value := client.Get(context.Background(), deleteEpochKey).Val(); value == "" {
		t.Fatal("refresh did not establish a non-empty delete epoch")
	}
	if err := manager.DeleteByPrefix(context.Background(), prefix); err != nil {
		t.Fatalf("DeleteByPrefix() error = %v", err)
	}
	if err := client.Del(context.Background(), deleteEpochKey).Err(); err != nil {
		t.Fatalf("Del(delete epoch) error = %v", err)
	}
	close(releaseLoader)
	if err := <-refreshDone; !stdErrors.Is(err, ErrRefreshInvalidated) {
		t.Fatalf("RefreshByKey() error = %v, want ErrRefreshInvalidated", err)
	}
	if exists := client.Exists(context.Background(), key).Val(); exists != 0 {
		t.Fatalf("stale key exists = %d, want 0", exists)
	}
}

// TestPrefixMemberEpochEvictionInvalidatesFullSnapshot 验证成员删除代际丢失时旧全量快照不能复活成员。
func TestPrefixMemberEpochEvictionInvalidatesFullSnapshot(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index: "member-epoch-evict", Key: "member-epoch-evict:", Type: TypeString,
		Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
			close(loaderStarted)
			select {
			case <-releaseLoader:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return []Entry{{Key: params.Key + "1", Type: TypeString, Value: "stale"}}, nil
		},
	}}, WithWait(time.Millisecond, 200))
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	prefix := defaultPhysicalKey("member-epoch-evict:")
	key := prefix + "1"
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- manager.RefreshByKey(context.Background(), prefix) }()
	<-loaderStarted
	target := manager.prefixTargetsM[prefix]
	memberEpochKey := manager.prefixMemberDeleteEpochKey(target.Key)
	if value := client.Get(context.Background(), memberEpochKey).Val(); value == "" {
		t.Fatal("full refresh did not establish a non-empty member epoch")
	}
	if err := manager.DeleteByKey(context.Background(), key); err != nil {
		t.Fatalf("DeleteByKey() error = %v", err)
	}
	if err := client.Del(context.Background(), memberEpochKey).Err(); err != nil {
		t.Fatalf("Del(member epoch) error = %v", err)
	}
	close(releaseLoader)
	if err := <-refreshDone; !stdErrors.Is(err, ErrRefreshInvalidated) {
		t.Fatalf("RefreshByKey(full) error = %v, want ErrRefreshInvalidated", err)
	}
	if exists := client.Exists(context.Background(), key).Val(); exists != 0 {
		t.Fatalf("stale member exists = %d, want 0", exists)
	}
}

// TestEmptyRefreshClearsHistoricalFieldsRegistry 验证 full/fields 空刷新关闭旧字段空值 registry。
func TestEmptyRefreshClearsHistoricalFieldsRegistry(t *testing.T) {
	t.Run("full refresh", func(t *testing.T) {
		server := runStandaloneRedis(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		store := NewRedisStore(client)
		key := defaultPhysicalKey("clear-fields-full")
		registryKey := tablecacheMetaKey("empty_fields", key)
		markerManager, err := newTestManager(store, []Target{{
			Index: "clear-fields-full", Key: "clear-fields-full", Type: TypeHash, AllowEmptyMarker: true,
			Loader: func(context.Context, LoadParams) ([]Entry, error) { return nil, ErrNotFound },
		}})
		if err != nil {
			t.Fatalf("newTestManager(marker) error = %v", err)
		}
		if err := markerManager.RefreshByKey(context.Background(), key, "name"); err != nil {
			t.Fatalf("RefreshByKey(fields marker) error = %v", err)
		}
		if exists := client.Exists(context.Background(), registryKey).Val(); exists != 1 {
			t.Fatalf("fields registry exists = %d, want 1", exists)
		}
		clearManager, err := newTestManager(store, []Target{{
			Index: "clear-fields-full", Key: "clear-fields-full", Type: TypeHash,
			Loader: func(context.Context, LoadParams) ([]Entry, error) { return nil, nil },
		}})
		if err != nil {
			t.Fatalf("newTestManager(clear) error = %v", err)
		}
		if err := clearManager.RefreshByKey(context.Background(), key); err != nil {
			t.Fatalf("RefreshByKey(full empty) error = %v", err)
		}
		if exists := client.Exists(context.Background(), registryKey).Val(); exists != 0 {
			t.Fatalf("fields registry exists after full empty = %d, want 0", exists)
		}
	})

	t.Run("fields refresh", func(t *testing.T) {
		server := runStandaloneRedis(t)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		store := NewRedisStore(client)
		key := defaultPhysicalKey("clear-fields-partial")
		markerManager, err := newTestManager(store, []Target{{
			Index: "clear-fields-partial", Key: "clear-fields-partial", Type: TypeHash, AllowEmptyMarker: true,
			Loader: func(context.Context, LoadParams) ([]Entry, error) { return nil, ErrNotFound },
		}})
		if err != nil {
			t.Fatalf("newTestManager(marker) error = %v", err)
		}
		if err := markerManager.RefreshByKey(context.Background(), key, "name"); err != nil {
			t.Fatalf("RefreshByKey(fields marker) error = %v", err)
		}
		clearManager, err := newTestManager(store, []Target{{
			Index: "clear-fields-partial", Key: "clear-fields-partial", Type: TypeHash,
			Loader: func(context.Context, LoadParams) ([]Entry, error) { return nil, nil },
		}})
		if err != nil {
			t.Fatalf("newTestManager(clear) error = %v", err)
		}
		if err := clearManager.RefreshByKey(context.Background(), key, "name"); err != nil {
			t.Fatalf("RefreshByKey(fields empty) error = %v", err)
		}
		registryKey := tablecacheMetaKey("empty_fields", key)
		empty, err := store.HasFieldsEmpty(context.Background(), registryKey, fieldsID([]string{"name"}))
		if err != nil {
			t.Fatalf("HasFieldsEmpty() error = %v", err)
		}
		if empty {
			t.Fatal("fields empty registry remained after marker-disabled refresh")
		}
	})
}

// TestPartialHashPositiveRefreshClearsOverlappingNegativeMarker 验证字段正向写回不会保留重叠组合的旧负缓存。
func TestPartialHashPositiveRefreshClearsOverlappingNegativeMarker(t *testing.T) {
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	var calls atomic.Int64
	manager, err := newTestManager(NewRedisStore(client), []Target{{
		Index:            "fields-overlap",
		Key:              "fields-overlap",
		Type:             TypeHash,
		AllowEmptyMarker: true,
		Loader: func(_ context.Context, params LoadParams) ([]Entry, error) {
			switch calls.Add(1) {
			case 1:
				return nil, ErrNotFound
			case 2:
				return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]string{"a": "A"}}}, nil
			default:
				return []Entry{{Key: params.Key, Type: TypeHash, Value: map[string]string{"a": "A", "b": "B"}}}, nil
			}
		},
	}})
	if err != nil {
		t.Fatalf("newTestManager() error = %v", err)
	}
	key := defaultPhysicalKey("fields-overlap")
	if err := manager.RefreshByKey(context.Background(), key, "a", "b"); err != nil {
		t.Fatalf("RefreshByKey(a,b empty) error = %v", err)
	}
	registryKey := manager.fieldsEmptyKey(key)
	if exists := client.Exists(context.Background(), registryKey).Val(); exists != 1 {
		t.Fatalf("fields registry exists = %d, want 1", exists)
	}
	if err := manager.RefreshByKey(context.Background(), key, "a"); err != nil {
		t.Fatalf("RefreshByKey(a hit) error = %v", err)
	}
	if exists := client.Exists(context.Background(), registryKey).Val(); exists != 0 {
		t.Fatalf("fields registry exists after positive refresh = %d, want 0", exists)
	}
	var value map[string]string
	result, err := manager.LoadThroughWithOptions(context.Background(), key, &value, LoadThroughOptions{Fields: []string{"a", "b"}})
	if err != nil {
		t.Fatalf("LoadThroughWithOptions(a,b) error = %v", err)
	}
	if result.State != LookupStateHit || value["a"] != "A" || value["b"] != "B" || calls.Load() != 3 {
		t.Fatalf("result=%+v value=%v calls=%d, want refreshed a/b hit", result, value, calls.Load())
	}
}
