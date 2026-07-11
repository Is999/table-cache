package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Is999/go-utils/errors"
	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

// runStandaloneRedis 创建明确模拟 standalone 拓扑的 miniredis，避免示例测试依赖不完整的 CLUSTER INFO 实现。
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

// failingUserModel 返回带敏感文本的错误，用于验证 HTTP 输出不会泄漏下游细节。
type failingUserModel struct{}

// FindOne 模拟数据库查询失败。
func (failingUserModel) FindOne(ctx context.Context, id int64) (*User, error) {
	return nil, errors.Errorf("database password=secret user_id=%d", id)
}

// FindAll 模拟全量数据库查询失败。
func (failingUserModel) FindAll(ctx context.Context) ([]User, error) {
	return nil, errors.Errorf("database password=secret full scan")
}

// newTestUserCache 创建使用独立内存 Redis 的示例缓存。
func newTestUserCache(t *testing.T, model userModel) (*UserCache, *redis.Client) {
	t.Helper()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("redis client close error = %v", err)
		}
	})
	cache, err := NewUserCache(client, model)
	if err != nil {
		t.Fatalf("NewUserCache() error = %v", err)
	}
	return cache, client
}

// TestUserCacheFlow 验证示例默认前缀下的读穿、空值保护、刷新和批量流程都可运行。
func TestUserCacheFlow(t *testing.T) {
	ctx := context.Background()
	cache, client := newTestUserCache(t, NewMemoryUserModel())

	user, err := cache.GetForBiz(ctx, 1)
	if err != nil {
		t.Fatalf("GetForBiz(1) error = %v", err)
	}
	if user == nil || user.ID != 1 || user.Name != "alpha" {
		t.Fatalf("GetForBiz(1) = %+v, want alpha user", user)
	}
	if exists := client.Exists(ctx, exampleKeyPrefix+"user_profile:1").Val(); exists != 1 {
		t.Fatalf("prefixed cache key exists = %d, want 1", exists)
	}
	for _, invalidKey := range []string{
		exampleKeyPrefix + "user_profile::",
		exampleKeyPrefix + "user_profile::1::",
		exampleKeyPrefix + "user_profile:1:",
		exampleKeyPrefix + "user_profile:1:extra",
	} {
		if err := cache.Manager().RefreshByKey(ctx, invalidKey); err == nil {
			t.Fatalf("RefreshByKey(%s) error = nil", invalidKey)
		}
		if exists := client.Exists(ctx, invalidKey).Val(); exists != 0 {
			t.Fatalf("invalid alias key %s exists = %d, want 0", invalidKey, exists)
		}
	}
	if err := cache.Manager().RefreshAll(ctx); err != nil {
		t.Fatalf("RefreshAll() error = %v", err)
	}
	if exists := client.Exists(ctx, exampleKeyPrefix+"user_profile:1", exampleKeyPrefix+"user_profile:2", exampleKeyPrefix+"user_profile:3").Val(); exists != 3 {
		t.Fatalf("full refresh cache keys = %d, want 3", exists)
	}

	missing, err := cache.GetForBiz(ctx, 404)
	if err != nil {
		t.Fatalf("GetForBiz(404) error = %v", err)
	}
	if missing != nil {
		t.Fatalf("GetForBiz(404) = %+v, want nil", missing)
	}

	if err := cache.RefreshFromAdmin(ctx, 2); err != nil {
		t.Fatalf("RefreshFromAdmin(2) error = %v", err)
	}
	response, err := cache.WarmupByTaskWithSummary(ctx, []int64{1, 2})
	if err != nil {
		t.Fatalf("WarmupByTaskWithSummary() error = %v", err)
	}
	if !response.Success || response.Code != "ok" || response.Summary.Success != 2 {
		t.Fatalf("WarmupByTaskWithSummary() response = %+v, want two successes", response)
	}
	readResponse, err := cache.LoadThroughBatchForAdmin(ctx, []int64{1, 404})
	if err != nil {
		t.Fatalf("LoadThroughBatchForAdmin() error = %v", err)
	}
	if !readResponse.Success || readResponse.Code != "ok" || len(readResponse.Items) != 2 || readResponse.Items[1].Code != "ok" || readResponse.Items[1].State != "empty" {
		t.Fatalf("LoadThroughBatchForAdmin() response = %+v, want hit and empty", readResponse)
	}
}

// TestUserCacheClusterSkipsUntaggedRefreshAll 验证 Cluster 示例不会声明无法安全执行的普通前缀全量刷新。
func TestUserCacheClusterSkipsUntaggedRefreshAll(t *testing.T) {
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("cluster client close error = %v", err)
		}
	})
	cache, err := NewUserCache(client, failingUserModel{})
	if err != nil {
		t.Fatalf("NewUserCache() error = %v", err)
	}
	results, summary, err := cache.Manager().RefreshAllWithSummary(context.Background())
	if err != nil {
		t.Fatalf("RefreshAllWithSummary() error = %v", err)
	}
	if len(results) != 0 || summary.Total != 0 {
		t.Fatalf("RefreshAllWithSummary() results=%v summary=%+v, want empty", results, summary)
	}
}

// TestAdminRoutesValidateRequests 验证示例路由限制方法、请求体和批量大小。
func TestAdminRoutesValidateRequests(t *testing.T) {
	cache, _ := newTestUserCache(t, NewMemoryUserModel())
	mux := http.NewServeMux()
	registerRoutes(mux, &ServiceContext{Cache: cache, TableList: cache.Manager().Items()})

	tooManyIDs := make([]string, 0, maxBatchUserIDs+1)
	for index := 1; index <= maxBatchUserIDs+1; index++ {
		tooManyIDs = append(tooManyIDs, fmt.Sprintf("%d", index))
	}
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{name: "items method", method: http.MethodPost, path: "/tablecache/items", status: http.StatusMethodNotAllowed},
		{name: "unknown field", method: http.MethodPost, path: "/tablecache/user/refresh", body: `{"userId":1,"password":"secret"}`, status: http.StatusBadRequest},
		{name: "invalid id", method: http.MethodPost, path: "/tablecache/user/refresh", body: `{"userId":0}`, status: http.StatusBadRequest},
		{name: "empty batch", method: http.MethodPost, path: "/tablecache/user/read", body: `{"userIds":[]}`, status: http.StatusBadRequest},
		{name: "large batch", method: http.MethodPost, path: "/tablecache/user/warmup", body: `{"userIds":[` + strings.Join(tooManyIDs, ",") + `]}`, status: http.StatusBadRequest},
		{name: "body too large", method: http.MethodPost, path: "/tablecache/user/refresh", body: strings.Repeat(" ", int(maxRequestBodyBytes)+1), status: http.StatusBadRequest},
		{name: "multiple json", method: http.MethodPost, path: "/tablecache/user/refresh", body: `{"userId":1}{"userId":2}`, status: http.StatusBadRequest},
		{name: "invalid query id", method: http.MethodGet, path: "/biz/user?userId=invalid", status: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d body=%s, want %d", response.Code, response.Body.String(), test.status)
			}
			if strings.Contains(response.Body.String(), "secret") {
				t.Fatalf("response leaked request detail: %s", response.Body.String())
			}
		})
	}
}

// TestAdminRoutesHideDownstreamError 验证下游原始错误仅保留在 Go 返回值，不进入 HTTP JSON。
func TestAdminRoutesHideDownstreamError(t *testing.T) {
	cache, _ := newTestUserCache(t, failingUserModel{})
	mux := http.NewServeMux()
	registerRoutes(mux, &ServiceContext{Cache: cache, TableList: cache.Manager().Items()})

	request := httptest.NewRequest(http.MethodPost, "/tablecache/user/read", strings.NewReader(`{"userIds":[1]}`))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	body := response.Body.String()
	if strings.Contains(body, "password=secret") || strings.Contains(body, "database") {
		t.Fatalf("response leaked downstream error: %s", body)
	}
	if strings.Contains(body, `"error":`) {
		t.Fatalf("response contains removed error field: %s", body)
	}
	if !strings.Contains(body, `"code":"partial_failure"`) || !strings.Contains(body, `"code":"internal_error"`) {
		t.Fatalf("response = %s, want safe batch and item codes", body)
	}
}

// TestNormalizeUserIDs 验证批量参数会保持顺序去重。
func TestNormalizeUserIDs(t *testing.T) {
	userIDs, err := normalizeUserIDs([]int64{2, 1, 2})
	if err != nil {
		t.Fatalf("normalizeUserIDs() error = %v", err)
	}
	if len(userIDs) != 2 || userIDs[0] != 2 || userIDs[1] != 1 {
		t.Fatalf("normalizeUserIDs() = %#v, want [2 1]", userIDs)
	}
}

// TestLoadConfigValidation 验证 Redis 模式、数值和 TLS 配置错误会在启动前失败。
func TestLoadConfigValidation(t *testing.T) {
	envKeys := []string{
		"TABLECACHE_HTTP_ADDR",
		"TABLECACHE_REDIS_TYPE",
		"TABLECACHE_REDIS_ADDRS",
		"TABLECACHE_REDIS_ADDR_MAP",
		"TABLECACHE_REDIS_PASSWORD",
		"TABLECACHE_REDIS_DB",
		"TABLECACHE_REDIS_POOL_SIZE",
		"TABLECACHE_REDIS_TLS",
		"TABLECACHE_REDIS_TLS_INSECURE_SKIP_VERIFY",
	}
	tests := []struct {
		name    string
		values  map[string]string
		wantErr bool
	}{
		{name: "defaults"},
		{name: "valid cluster", values: map[string]string{"TABLECACHE_REDIS_TYPE": "cluster", "TABLECACHE_REDIS_ADDRS": "127.0.0.1:7001,127.0.0.1:7002"}},
		{name: "invalid type", values: map[string]string{"TABLECACHE_REDIS_TYPE": "sentinel"}, wantErr: true},
		{name: "invalid db", values: map[string]string{"TABLECACHE_REDIS_DB": "x"}, wantErr: true},
		{name: "negative db", values: map[string]string{"TABLECACHE_REDIS_DB": "-1"}, wantErr: true},
		{name: "invalid pool", values: map[string]string{"TABLECACHE_REDIS_POOL_SIZE": "0"}, wantErr: true},
		{name: "invalid tls", values: map[string]string{"TABLECACHE_REDIS_TLS": "maybe"}, wantErr: true},
		{name: "insecure without tls", values: map[string]string{"TABLECACHE_REDIS_TLS_INSECURE_SKIP_VERIFY": "true"}, wantErr: true},
		{name: "invalid addr map", values: map[string]string{"TABLECACHE_REDIS_ADDR_MAP": "redis-node"}, wantErr: true},
		{name: "duplicate addr map", values: map[string]string{"TABLECACHE_REDIS_ADDR_MAP": "node=127.0.0.1,node=127.0.0.2"}, wantErr: true},
		{name: "single multiple addresses", values: map[string]string{"TABLECACHE_REDIS_ADDRS": "127.0.0.1:6379,127.0.0.1:6380"}, wantErr: true},
		{name: "cluster db", values: map[string]string{"TABLECACHE_REDIS_TYPE": "cluster", "TABLECACHE_REDIS_DB": "1"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, key := range envKeys {
				t.Setenv(key, "")
			}
			for key, value := range test.values {
				t.Setenv(key, value)
			}
			_, err := loadConfig()
			if (err != nil) != test.wantErr {
				t.Fatalf("loadConfig() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

// TestIsClusterShardsUnsupported 验证 Redis 6.2 会选择 ClusterSlots 协议。
func TestIsClusterShardsUnsupported(t *testing.T) {
	if !isClusterShardsUnsupported(errors.New("ERR unknown subcommand 'SHARDS'. Try CLUSTER HELP.")) {
		t.Fatalf("Redis 6.2 unknown subcommand should enable ClusterSlots fallback")
	}
	if isClusterShardsUnsupported(errors.New("ERR cluster is down")) {
		t.Fatalf("unrelated cluster error should not enable fallback")
	}
}
