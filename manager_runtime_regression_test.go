package tablecache

import (
	"context"
	stdErrors "errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// typedNilStore 用于验证 Store 接口中的类型化 nil 会在构造期被拒绝。
type typedNilStore struct{}

// AcquireRefreshLock 实现 Store 锁获取契约。
func (*typedNilStore) AcquireRefreshLock(context.Context, string, string, time.Duration) (bool, string, error) {
	panic("typed nil Store must not be called")
}

// ApplyMutation 实现 Store 变更契约。
func (*typedNilStore) ApplyMutation(context.Context, StoreMutation) error {
	panic("typed nil Store must not be called")
}

// Exists 实现 Store 单 key 存在性契约。
func (*typedNilStore) Exists(context.Context, string) (bool, error) {
	panic("typed nil Store must not be called")
}

// ExistsMulti 实现 Store 批量存在性契约。
func (*typedNilStore) ExistsMulti(context.Context, ...string) (map[string]bool, error) {
	panic("typed nil Store must not be called")
}

// Read 实现 Store 读取契约。
func (*typedNilStore) Read(context.Context, string, CacheType) (any, error) {
	panic("typed nil Store must not be called")
}

// RefreshLock 实现 Store 锁续期契约。
func (*typedNilStore) RefreshLock(context.Context, string, string, time.Duration) (bool, error) {
	panic("typed nil Store must not be called")
}

// ReleaseLock 实现 Store 锁释放契约。
func (*typedNilStore) ReleaseLock(context.Context, string, string) (bool, error) {
	panic("typed nil Store must not be called")
}

// acquireSignalStore 记录刷新锁申请次数，用于证明不同语义的本地 singleflight 不会合并。
type acquireSignalStore struct {
	*RedisStore
	attempts      atomic.Int64  // attempts 是刷新锁申请总次数
	secondAttempt chan struct{} // secondAttempt 在第二个刷新请求进入 Store 时关闭
	secondOnce    sync.Once     // secondOnce 保证通知通道只关闭一次
}

// AcquireRefreshLock 记录锁申请后复用 RedisStore 的真实分布式锁实现。
func (s *acquireSignalStore) AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	if s.attempts.Add(1) == 2 {
		s.secondOnce.Do(func() { close(s.secondAttempt) })
	}
	return s.RedisStore.AcquireRefreshLock(ctx, key, value, ttl)
}

// TestNewManagerRejectsTypedNilStore 验证类型化 nil Store 不会绕过启动校验。
func TestNewManagerRejectsTypedNilStore(t *testing.T) {
	var store *typedNilStore
	if _, err := NewManager(store, nil); err == nil {
		t.Fatal("NewManager(typed nil Store) error = nil")
	}
}

// TestRedisStoreRejectsTypedNilClients 验证三类 go-redis 客户端的类型化 nil 都会被拒绝。
func TestRedisStoreRejectsTypedNilClients(t *testing.T) {
	var single *redis.Client
	var cluster *redis.ClusterClient
	var ring *redis.Ring
	for name, client := range map[string]redis.UniversalClient{
		"single":  single,
		"cluster": cluster,
		"ring":    ring,
	} {
		t.Run(name, func(t *testing.T) {
			if err := NewRedisStore(client).ValidateTablecacheStore(); err == nil {
				t.Fatal("ValidateTablecacheStore(typed nil client) error = nil")
			}
		})
	}
}

// TestIgnoreCancelDoesNotRetryCanceledLoader 验证忽略调用方取消时不会把 Loader 自身取消误判成可重试 leader。
func TestIgnoreCancelDoesNotRetryCanceledLoader(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	var loaderCalls atomic.Int64
	secondErr := stdErrors.New("unexpected second loader call")
	manager, err := NewManager(NewRedisStore(client), []Target{{
		Index: "ignore-cancel",
		Key:   "ignore-cancel",
		Type:  TypeString,
		Loader: func(context.Context, LoadParams) ([]Entry, error) {
			if loaderCalls.Add(1) == 1 {
				return nil, context.Canceled
			}
			return nil, secondErr
		},
	}}, WithRebuildContextPolicy(RebuildContextIgnoreCancel))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = manager.RefreshByKey(ctx, defaultPhysicalKey("ignore-cancel"))
	if !stdErrors.Is(err, context.Canceled) {
		t.Fatalf("RefreshByKey() error = %v, want context.Canceled", err)
	}
	if calls := loaderCalls.Load(); calls != 1 {
		t.Fatalf("loader calls = %d, want 1", calls)
	}
}

// TestScheduledAndRequestedRefreshUseDifferentFlights 验证定时刷新与请求读穿不会复用不同空值语义的本地 flight。
func TestScheduledAndRequestedRefreshUseDifferentFlights(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := &acquireSignalStore{
		RedisStore:    NewRedisStore(client),
		secondAttempt: make(chan struct{}),
	}
	started := make(chan struct{})
	releaseScheduled := make(chan struct{})
	var loaderCalls atomic.Int64
	manager, err := NewManager(store, []Target{{
		Index:            "flight-scope",
		Key:              "flight-scope",
		Type:             TypeString,
		RefreshAll:       true,
		AllowEmptyMarker: true,
		Loader: func(ctx context.Context, _ LoadParams) ([]Entry, error) {
			if loaderCalls.Add(1) == 1 {
				close(started)
				select {
				case <-releaseScheduled:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		},
	}}, WithWait(time.Millisecond, 200))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	scheduledDone := make(chan error, 1)
	go func() { scheduledDone <- manager.RefreshAll(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("scheduled loader did not start")
	}
	type requestedResult struct {
		result LookupResult
		err    error
	}
	requestedDone := make(chan requestedResult, 1)
	go func() {
		var value string
		result, err := manager.GetOrRefresh(context.Background(), defaultPhysicalKey("flight-scope"), &value)
		requestedDone <- requestedResult{result: result, err: err}
	}()
	select {
	case <-store.secondAttempt:
		// 第二次锁申请证明请求读穿没有加入仍在运行的 scheduled flight。
	case <-time.After(time.Second):
		close(releaseScheduled)
		<-scheduledDone
		t.Fatal("requested refresh did not enter an independent flight")
	}
	close(releaseScheduled)
	if err := <-scheduledDone; err != nil {
		t.Fatalf("RefreshAll() error = %v", err)
	}
	select {
	case response := <-requestedDone:
		if response.err != nil {
			t.Fatalf("GetOrRefresh() error = %v", response.err)
		}
		if response.result.State != LookupStateEmpty || !response.result.Refreshed {
			t.Fatalf("GetOrRefresh() result = %+v, want refreshed empty", response.result)
		}
	case <-time.After(time.Second):
		t.Fatal("requested refresh did not finish")
	}
	if calls := loaderCalls.Load(); calls != 2 {
		t.Fatalf("loader calls = %d, want 2", calls)
	}
}
