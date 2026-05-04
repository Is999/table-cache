package tablecache

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
)

// clusterIntegrationConfig 表示真实 Redis Cluster 集成测试配置。
type clusterIntegrationConfig struct {
	Addrs    []string          // Addrs 是 Redis Cluster 种子节点地址列表
	Password string            // Password 是 Redis 认证密码
	AddrMap  map[string]string // AddrMap 用于把集群返回的容器 hostname 改写为宿主机地址
	PoolSize int               // PoolSize 是连接池大小
}

// clusterSlotsCompatTestClient 表示测试里兼容读取旧版 ClusterSlots 的最小客户端能力。
type clusterSlotsCompatTestClient interface {
	ClusterSlots(ctx context.Context) *redis.ClusterSlotsCmd
}

// TestRedisClusterIntegration 验证真实 Redis Cluster 环境下的关键缓存能力。
func TestRedisClusterIntegration(t *testing.T) {
	config, ok := loadClusterIntegrationConfig()
	if !ok {
		t.Skip("未配置真实 Redis Cluster 集成测试环境变量，跳过集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := newClusterIntegrationClient(ctx, config)
	if err != nil {
		t.Fatalf("newClusterIntegrationClient() error = %v", err)
	}
	defer client.Close()
	store := NewRedisStore(client)
	prefix := fmt.Sprintf("itc:cluster:%d:", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = store.DeletePattern(context.Background(), prefix+"*", defaultScanCount)
		_, _ = store.DeletePattern(context.Background(), "tablecache:*"+prefix+"*", defaultScanCount)
		_ = store.Delete(context.Background(), prefix+"lock")
	})

	t.Run("ping and slots", func(t *testing.T) {
		if err := client.Ping(ctx).Err(); err != nil {
			t.Fatalf("cluster ping error = %v", err)
		}
		slots, err := loadRewrittenClusterSlots(ctx, config)
		if err != nil {
			t.Fatalf("loadRewrittenClusterSlots() error = %v", err)
		}
		if len(slots) == 0 {
			t.Fatalf("loadRewrittenClusterSlots() len = 0, want > 0")
		}
	})

	t.Run("store lock and pattern delete", func(t *testing.T) {
		locked, err := store.SetNX(ctx, prefix+"lock", "owner-1", time.Minute)
		if err != nil {
			t.Fatalf("SetNX() error = %v", err)
		}
		if !locked {
			t.Fatalf("SetNX() locked = false, want true")
		}
		ok, err := store.RefreshLock(ctx, prefix+"lock", "owner-1", 2*time.Minute)
		if err != nil {
			t.Fatalf("RefreshLock() error = %v", err)
		}
		if !ok {
			t.Fatalf("RefreshLock() ok = false, want true")
		}
		released, err := store.ReleaseLock(ctx, prefix+"lock", "owner-1")
		if err != nil {
			t.Fatalf("ReleaseLock() error = %v", err)
		}
		if !released {
			t.Fatalf("ReleaseLock() released = false, want true")
		}
		err = store.WriteBatch(ctx, []Entry{
			{Key: prefix + "string", Type: TypeString, Value: "ok"},
			{Key: prefix + "hash", Type: TypeHash, Value: map[string]any{"name": "alpha"}},
			{Key: prefix + "set", Type: TypeSet, Value: []any{"a", "b"}},
		})
		if err != nil {
			t.Fatalf("WriteBatch() error = %v", err)
		}
		deleted, err := store.DeletePattern(ctx, prefix+"*", defaultScanCount)
		if err != nil {
			t.Fatalf("DeletePattern() error = %v", err)
		}
		if deleted < 3 {
			t.Fatalf("DeletePattern() deleted = %d, want >= 3", deleted)
		}
	})

	t.Run("manager registered key guard", func(t *testing.T) {
		manager, err := NewManager(store, []Target{
			{
				Index: "cluster_itc",
				Title: "集群集成测试缓存",
				Key:   prefix,
				Type:  TypeString,
				Loader: func(ctx context.Context, params LoadParams) ([]Entry, error) {
					return []Entry{{Key: params.Key, Type: TypeString, Value: "value:" + params.Key}}, nil
				},
			},
		}, WithWait(50*time.Millisecond, 20))
		if err != nil {
			t.Fatalf("NewManager() error = %v", err)
		}
		if err := manager.RefreshByKey(ctx, prefix+"1"); err != nil {
			t.Fatalf("RefreshByKey() error = %v", err)
		}
		var value string
		result, err := manager.GetState(ctx, prefix+"1", &value)
		if err != nil {
			t.Fatalf("GetState() error = %v", err)
		}
		if result.State != LookupStateHit || value != "value:"+prefix+"1" {
			t.Fatalf("GetState() result=%+v value=%q, want hit/value", result, value)
		}
		if err := manager.RefreshByKeys(ctx, []string{prefix + "2", prefix + "3", prefix + "2"}); err != nil {
			t.Fatalf("RefreshByKeys() error = %v", err)
		}
		if err := manager.DeleteByKey(ctx, "other:1"); err == nil || !errors.Is(err, ErrTargetNotFound) {
			t.Fatalf("DeleteByKey(other:1) error = %v, want ErrTargetNotFound", err)
		}
		if err := manager.DeleteByPrefix(ctx, prefix); err != nil {
			t.Fatalf("DeleteByPrefix() error = %v", err)
		}
		for _, key := range []string{prefix + "1", prefix + "2", prefix + "3"} {
			exists, err := client.Exists(ctx, key).Result()
			if err != nil {
				t.Fatalf("Exists(%s) error = %v", key, err)
			}
			if exists != 0 {
				t.Fatalf("key %s exists = %d, want 0", key, exists)
			}
		}
	})
}

// loadClusterIntegrationConfig 读取真实 Redis Cluster 集成测试配置。
func loadClusterIntegrationConfig() (clusterIntegrationConfig, bool) {
	addrsText := strings.TrimSpace(os.Getenv("TABLECACHE_REDIS_CLUSTER_ADDRS"))
	if addrsText == "" {
		return clusterIntegrationConfig{}, false
	}
	config := clusterIntegrationConfig{
		Addrs:    splitAndTrim(addrsText),
		Password: os.Getenv("TABLECACHE_REDIS_CLUSTER_PASSWORD"),
		AddrMap:  parseAddrMap(os.Getenv("TABLECACHE_REDIS_ADDR_MAP")),
		PoolSize: 20,
	}
	return config, len(config.Addrs) > 0
}

// newClusterIntegrationClient 创建支持地址改写的真实 Redis Cluster 客户端。
func newClusterIntegrationClient(ctx context.Context, config clusterIntegrationConfig) (*redis.ClusterClient, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.Addrs,
		Password: config.Password,
		PoolSize: config.PoolSize,
		ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
			return loadRewrittenClusterSlots(ctx, config)
		},
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, errors.Tag(err)
	}
	return client, nil
}

// loadRewrittenClusterSlots 从可访问的种子节点读取槽位信息，并按 addr_map 改写容器 hostname。
// Redis 7 及以上优先使用 ClusterShards；旧版本不支持时自动回退到 ClusterSlots。
func loadRewrittenClusterSlots(ctx context.Context, config clusterIntegrationConfig) ([]redis.ClusterSlot, error) {
	var lastErr error
	for _, addr := range config.Addrs {
		seedClient := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: config.Password,
		})
		slots, err := loadClusterSlotsCompatForTest(ctx, seedClient, config.AddrMap)
		_ = seedClient.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return slots, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("没有可用的 Redis Cluster 种子节点")
	}
	return nil, lastErr
}

// loadClusterSlotsCompatForTest 优先使用 ClusterShards；若目标 Redis 版本不支持，则自动回退到 ClusterSlots。
func loadClusterSlotsCompatForTest(ctx context.Context, seedClient *redis.Client, addrMap map[string]string) ([]redis.ClusterSlot, error) {
	shards, err := seedClient.ClusterShards(ctx).Result()
	if err == nil {
		return clusterShardsToSlotsForTest(shards, addrMap), nil
	}
	if !isClusterShardsUnsupportedForTest(err) {
		return nil, errors.Tag(err)
	}
	slots, err := loadLegacyClusterSlotsForTest(ctx, seedClient)
	if err != nil {
		return nil, err
	}
	for slotIndex, slot := range slots {
		for nodeIndex, node := range slot.Nodes {
			slots[slotIndex].Nodes[nodeIndex].Addr = rewriteClusterAddr(node.Addr, addrMap)
		}
	}
	return slots, nil
}

// loadLegacyClusterSlotsForTest 通过旧版 ClusterSlots 命令读取槽位信息，作为 Redis 7 以下版本的兼容回退。
func loadLegacyClusterSlotsForTest(ctx context.Context, client clusterSlotsCompatTestClient) ([]redis.ClusterSlot, error) {
	slots, err := client.ClusterSlots(ctx).Result()
	return slots, errors.Tag(err)
}

// clusterShardsToSlotsForTest 把 Redis 7 的 ClusterShards 结果转换成 ClusterSlots 回调需要的结构。
func clusterShardsToSlotsForTest(shards []redis.ClusterShard, addrMap map[string]string) []redis.ClusterSlot {
	slots := make([]redis.ClusterSlot, 0, len(shards))
	for _, shard := range shards {
		nodes := make([]redis.ClusterNode, 0, len(shard.Nodes))
		for _, node := range shard.Nodes {
			addr := rewriteClusterAddr(clusterShardNodeAddrForTest(node), addrMap)
			if strings.TrimSpace(addr) == "" {
				continue
			}
			nodes = append(nodes, redis.ClusterNode{
				ID:   node.ID,
				Addr: addr,
			})
		}
		for _, slotRange := range shard.Slots {
			slots = append(slots, redis.ClusterSlot{
				Start: int(slotRange.Start),
				End:   int(slotRange.End),
				Nodes: nodes,
			})
		}
	}
	return slots
}

// clusterShardNodeAddrForTest 返回 ClusterShards 节点的可连接地址。
func clusterShardNodeAddrForTest(node redis.Node) string {
	endpoint := strings.TrimSpace(node.Endpoint)
	if endpoint != "" {
		return endpoint
	}
	host := strings.TrimSpace(node.Hostname)
	if host == "" {
		host = strings.TrimSpace(node.IP)
	}
	if host == "" {
		return ""
	}
	port := node.Port
	if port <= 0 {
		port = node.TLSPort
	}
	if port <= 0 {
		return host
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", port))
}

// isClusterShardsUnsupportedForTest 判断当前错误是否表示 Redis 服务端不支持 ClusterShards。
func isClusterShardsUnsupportedForTest(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unknown command") && strings.Contains(message, "cluster") && strings.Contains(message, "shards")
}

// rewriteClusterAddr 按 addr_map 把集群返回的 hostname 改写为宿主机可访问地址。
func rewriteClusterAddr(addr string, addrMap map[string]string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return addr
	}
	mappedHost, ok := addrMap[host]
	if !ok || strings.TrimSpace(mappedHost) == "" {
		return addr
	}
	return net.JoinHostPort(mappedHost, port)
}

// parseAddrMap 解析形如 host=ip,host2=ip2 的地址改写配置。
func parseAddrMap(text string) map[string]string {
	result := make(map[string]string)
	for _, item := range splitAndTrim(text) {
		host, mapped, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		host = strings.TrimSpace(host)
		mapped = strings.TrimSpace(mapped)
		if host == "" || mapped == "" {
			continue
		}
		result[host] = mapped
	}
	return result
}

// splitAndTrim 按逗号分隔并清理空白项。
func splitAndTrim(text string) []string {
	parts := strings.Split(text, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
