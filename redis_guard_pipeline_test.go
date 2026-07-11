package tablecache

import (
	"context"
	stdErrors "errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// pipelineShapeHook 记录 Pipeline 命令名称，用于防止每条 full entry 重复发送 Lua 原文。
type pipelineShapeHook struct {
	mu        sync.Mutex
	pipelines [][]string
}

func (h *pipelineShapeHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *pipelineShapeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h *pipelineShapeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		names := make([]string, len(cmds))
		for index, cmd := range cmds {
			names[index] = strings.ToLower(cmd.Name())
		}
		h.mu.Lock()
		h.pipelines = append(h.pipelines, names)
		h.mu.Unlock()
		return next(ctx, cmds)
	}
}

// guardedWritePipeline 返回包含 guarded EVAL 的命令批次。
func (h *pipelineShapeHook) guardedWritePipeline() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, names := range h.pipelines {
		if len(names) > 0 && names[0] == "eval" {
			return append([]string(nil), names...)
		}
	}
	return nil
}

// TestReplacePrefixGuardedPipelineUsesSingleLuaBody 验证每批只发送一次 Lua 原文，其余条目使用 SHA。
func TestReplacePrefixGuardedPipelineUsesSingleLuaBody(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	shapes := &pipelineShapeHook{}
	client.AddHook(shapes)
	store := NewRedisStore(client)
	prefix := "pipeline-full:"
	entries := []Entry{
		{Key: prefix + "1", Type: TypeString, Value: "one"},
		{Key: prefix + "2", Type: TypeString, Value: "two"},
		{Key: prefix + "3", Type: TypeString, Value: "three"},
	}
	if _, err := store.ReplacePrefix(ctx, PrefixReplaceMutation{
		Guards: acquireTestLockGuard(t, store, prefix), Prefix: prefix,
		Entries: entries, DeletePatterns: []string{RedisPrefixPattern(prefix)}, ScanCount: 10,
	}); err != nil {
		t.Fatalf("ReplacePrefix() error = %v", err)
	}
	if names := shapes.guardedWritePipeline(); !slices.Equal(names, []string{"eval", "evalsha", "evalsha"}) {
		t.Fatalf("guarded pipeline = %v, want [eval evalsha evalsha]", names)
	}
	for _, entry := range entries {
		if value := client.Get(ctx, entry.Key).Val(); value == "" {
			t.Fatalf("entry %s was not written", entry.Key)
		}
	}
}

// TestRedisStoreReplacePrefixRejectsIncrementalEntry 验证直接 Store 调用也不能绕过全量覆盖语义。
func TestRedisStoreReplacePrefixRejectsIncrementalEntry(t *testing.T) {
	ctx := context.Background()
	server := runStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store := NewRedisStore(client)
	prefix := "prefix-incremental:"
	_, err := store.ReplacePrefix(ctx, PrefixReplaceMutation{
		Guards: acquireTestLockGuard(t, store, prefix), Prefix: prefix,
		Entries: []Entry{{
			Key: prefix + "1", Type: TypeHash, Value: map[string]any{"name": "bad"}, Overwrite: Bool(false),
		}},
		DeletePatterns: []string{RedisPrefixPattern(prefix)}, ScanCount: 10,
	})
	if !stdErrors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ReplacePrefix() error = %v, want ErrInvalidConfig", err)
	}
	if exists := client.Exists(ctx, prefix+"1").Val(); exists != 0 {
		t.Fatalf("incremental full entry exists = %d, want 0", exists)
	}
}
