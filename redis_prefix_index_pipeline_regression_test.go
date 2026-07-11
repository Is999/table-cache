package tablecache

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRedisStorePrefixIndexSplitPipelineRecoversEvictedFirstBatch 验证同分片多批写入时，首批淘汰不会形成缺成员的可信索引。
func TestRedisStorePrefixIndexSplitPipelineRecoversEvictedFirstBatch(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	evictClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		_ = evictClient.Close()
	})

	const memberCount = 300
	const shard = 7
	members := make([]string, 0, memberCount)
	for sequence := 0; len(members) < memberCount; sequence++ {
		key := fmt.Sprintf("split-pipeline:%d", sequence)
		if prefixIndexMemberShard(key) == shard {
			members = append(members, key)
		}
	}

	indexKey := "tcm:pidx:split-pipeline"
	shardKey := prefixIndexShardKey(indexKey, shard)
	hook := &splitPrefixIndexPipelineHook{
		client:   evictClient,
		shardKey: shardKey,
	}
	client.AddHook(hook)
	store := NewRedisStore(client)

	if err := store.addPrefixIndexKeys(ctx, indexKey, time.Hour, members...); err != nil {
		t.Fatalf("addPrefixIndexKeys() error = %v", err)
	}
	if !hook.fired.Load() {
		t.Fatal("split pipeline hook did not fire")
	}
	if firstCount := hook.firstBatchCount.Load(); firstCount != defaultWriteBatchChunkSize+1 {
		t.Fatalf("first batch zcard = %d, want %d members plus sentinel", firstCount, defaultWriteBatchChunkSize+1)
	}
	if !hook.sawMissingSentinel.Load() {
		t.Fatal("second batch did not reject the evicted sentinel")
	}
	if count := client.ZCard(ctx, shardKey).Val(); count != memberCount+1 {
		t.Fatalf("replayed shard zcard = %d, want %d members plus sentinel", count, memberCount+1)
	}
	for _, member := range members {
		if err := client.ZScore(ctx, shardKey, member).Err(); err != nil {
			t.Fatalf("replayed shard missing member %q: %v", member, err)
		}
	}
	if err := client.ZScore(ctx, shardKey, prefixIndexSentinel).Err(); err != nil {
		t.Fatalf("replayed shard missing sentinel: %v", err)
	}
}

// TestRedisStorePrefixIndexPipelineCommandLimit 验证索引写入按固定命令数切分，且跨 Pipeline 的后续批次仍要求哨兵。
func TestRedisStorePrefixIndexPipelineCommandLimit(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	const multiBatchShard = prefixIndexShardCount - 1
	members := make([]string, 0, defaultWriteBatchChunkSize+1)
	for sequence := 0; len(members) < cap(members); sequence++ {
		key := fmt.Sprintf("pipeline-limit:%d", sequence)
		if prefixIndexMemberShard(key) == multiBatchShard {
			members = append(members, key)
		}
	}
	groups := make(map[int][]string, prefixIndexShardCount)
	for shard := 0; shard < prefixIndexShardCount; shard++ {
		groups[shard] = nil
	}
	groups[multiBatchShard] = members

	hook := &prefixIndexPipelineLimitHook{}
	client.AddHook(hook)
	store := NewRedisStore(client)
	indexKey := "tcm:pidx:pipeline-limit"
	if err := store.writePrefixIndexGroups(ctx, indexKey, time.Hour, groups, nil, nil, false); err != nil {
		t.Fatalf("writePrefixIndexGroups() error = %v", err)
	}
	if calls := hook.calls.Load(); calls != 2 {
		t.Fatalf("prefix index pipeline calls = %d, want 2", calls)
	}
	if total := hook.totalCommands.Load(); total != prefixIndexPipelineCommandLimit+1 {
		t.Fatalf("prefix index pipeline commands = %d, want %d", total, prefixIndexPipelineCommandLimit+1)
	}
	if maxCommands := hook.maxCommands.Load(); maxCommands != prefixIndexPipelineCommandLimit {
		t.Fatalf("max pipeline commands = %d, want %d", maxCommands, prefixIndexPipelineCommandLimit)
	}
	if !hook.secondChunkRequiresSentinel.Load() {
		t.Fatal("second pipeline did not preserve the shard batch sentinel requirement")
	}
	shardKey := prefixIndexShardKey(indexKey, multiBatchShard)
	if count := client.ZCard(ctx, shardKey).Val(); count != int64(len(members)+1) {
		t.Fatalf("cross-chunk shard zcard = %d, want %d members plus sentinel", count, len(members)+1)
	}
}

// splitPrefixIndexPipelineHook 在首条索引批次执行后淘汰分片，再继续执行剩余批次。
type splitPrefixIndexPipelineHook struct {
	client             *redis.Client
	shardKey           string
	fired              atomic.Bool
	firstBatchCount    atomic.Int64
	sawMissingSentinel atomic.Bool
}

// prefixIndexPipelineLimitHook 记录索引 Pipeline 的命令上限和第二批哨兵参数。
type prefixIndexPipelineLimitHook struct {
	calls                       atomic.Int64
	totalCommands               atomic.Int64
	maxCommands                 atomic.Int64
	secondChunkRequiresSentinel atomic.Bool
}

// DialHook 不改变 Redis 建连行为。
func (h *prefixIndexPipelineLimitHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook 不改变单命令行为。
func (h *prefixIndexPipelineLimitHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

// ProcessPipelineHook 记录每个前缀索引 Pipeline 的命令数量。
func (h *prefixIndexPipelineLimitHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !prefixIndexAddPipeline(cmds) {
			return next(ctx, cmds)
		}
		count := int64(len(cmds))
		call := h.calls.Add(1)
		h.totalCommands.Add(count)
		for current := h.maxCommands.Load(); count > current && !h.maxCommands.CompareAndSwap(current, count); current = h.maxCommands.Load() {
		}
		if call == 2 && len(cmds) == 1 && prefixIndexCommandRequiresSentinel(cmds[0]) {
			h.secondChunkRequiresSentinel.Store(true)
		}
		return next(ctx, cmds)
	}
}

// DialHook 不改变 Redis 建连行为。
func (h *splitPrefixIndexPipelineHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook 不改变单命令行为。
func (h *splitPrefixIndexPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

// ProcessPipelineHook 把目标 Pipeline 拆成两段，以稳定复现批次之间的分片淘汰。
func (h *splitPrefixIndexPipelineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) < 2 || !prefixIndexAddPipeline(cmds) || !h.fired.CompareAndSwap(false, true) {
			return next(ctx, cmds)
		}
		if err := next(ctx, cmds[:1]); err != nil {
			return err
		}
		h.firstBatchCount.Store(h.client.ZCard(ctx, h.shardKey).Val())
		if err := h.client.Unlink(ctx, h.shardKey).Err(); err != nil {
			return err
		}
		err := next(ctx, cmds[1:])
		if err != nil && strings.Contains(strings.ToLower(err.Error()), "missing sentinel") {
			h.sawMissingSentinel.Store(true)
		}
		return err
	}
}

// prefixIndexAddPipeline 判断 Pipeline 是否只包含前缀索引成员脚本。
func prefixIndexAddPipeline(cmds []redis.Cmder) bool {
	if len(cmds) == 0 {
		return false
	}
	for _, cmd := range cmds {
		args := cmd.Args()
		if !strings.EqualFold(cmd.Name(), "eval") || len(args) < 2 || fmt.Sprint(args[1]) != prefixIndexAddScript {
			return false
		}
	}
	return true
}

// prefixIndexCommandRequiresSentinel 读取无 guard 索引命令中的哨兵要求参数。
func prefixIndexCommandRequiresSentinel(cmd redis.Cmder) bool {
	args := cmd.Args()
	// EVAL、脚本、key数量、shard key 后依次是 guard数量、TTL、保留期和哨兵要求。
	return len(args) > 7 && fmt.Sprint(args[4]) == "0" && fmt.Sprint(args[7]) == "1"
}
