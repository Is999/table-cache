package tablecache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestFieldsEmptyRegistryPrunesExpiredMembersInBoundedBatch 验证字段 registry 不会在一次 Lua 中同步清理全部积压成员。
func TestFieldsEmptyRegistryPrunesExpiredMembersInBoundedBatch(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	now := time.Now().Truncate(time.Millisecond)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	key := "tcm:empty:fields:{bounded-prune}"
	members := make([]redis.Z, 0, 1001)
	for index := 0; index < 1000; index++ {
		members = append(members, redis.Z{Score: float64(now.Add(-time.Minute).UnixMilli()), Member: fmt.Sprintf("expired-%04d", index)})
	}
	members = append(members, redis.Z{Score: float64(now.Add(time.Minute).UnixMilli()), Member: "future"})
	if err := client.ZAdd(ctx, key, members...).Err(); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}
	store := NewRedisStore(client)
	ready, err := store.HasFieldsEmpty(ctx, key, "future")
	if err != nil || !ready {
		t.Fatalf("HasFieldsEmpty(future) = %v,%v, want true,nil", ready, err)
	}
	if count := client.ZCard(ctx, key).Val(); count != 745 {
		t.Fatalf("registry members after one prune = %d, want 745", count)
	}
}

// TestPrefixIndexPrunesExpiredMembersInBoundedBatch 验证前缀索引写入只渐进清理固定数量的历史成员。
func TestPrefixIndexPrunesExpiredMembersInBoundedBatch(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	const shard = 7
	indexKey := "tcm:pidx:bounded-prune"
	shardKey := prefixIndexShardKey(indexKey, shard)
	members := make([]redis.Z, 0, 1000)
	for index := 0; index < 1000; index++ {
		members = append(members, redis.Z{Score: 1, Member: fmt.Sprintf("expired-%04d", index)})
	}
	if err := client.ZAdd(ctx, shardKey, members...).Err(); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}
	store := NewRedisStore(client)
	if err := store.writePrefixIndexGroups(ctx, indexKey, time.Hour, map[int][]string{shard: nil}, nil, nil, true); err != nil {
		t.Fatalf("writePrefixIndexGroups() error = %v", err)
	}
	if count := client.ZCard(ctx, shardKey).Val(); count != 745 {
		t.Fatalf("prefix index members after one prune = %d, want 745", count)
	}
}
