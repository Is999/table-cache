package tablecache

import (
	"context"
	stdErrors "errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// redisCommandCountHook 统计测试期间发出的单命令和 Pipeline 命令数。
type redisCommandCountHook struct{ calls atomic.Int64 }

func (h *redisCommandCountHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *redisCommandCountHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.calls.Add(1)
		return next(ctx, cmd)
	}
}

func (h *redisCommandCountHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.calls.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// TestCollectionWriteLimitPreflightsBeforeEncoding 验证超限集合不会先编码分配或访问 Redis。
func TestCollectionWriteLimitPreflightsBeforeEncoding(t *testing.T) {
	tests := []struct {
		name  string
		typ   CacheType
		value any
	}{
		{name: "hash", typ: TypeHash, value: map[string]any{"a": struct{ N int }{1}, "b": struct{ N int }{2}}},
		{name: "list", typ: TypeList, value: []any{struct{ N int }{1}, struct{ N int }{2}}},
		{name: "set", typ: TypeSet, value: []any{struct{ N int }{1}, struct{ N int }{2}}},
		{name: "zset", typ: TypeZSet, value: []ZMember{{Member: struct{ N int }{1}, Score: 1}, {Member: struct{ N int }{2}, Score: 2}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := runStandaloneRedis(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			commands := &redisCommandCountHook{}
			client.AddHook(commands)
			var encodes atomic.Int64
			store := NewRedisStore(client,
				WithMaxCollectionWriteCount(1),
				WithRedisEncoder(func(any) (string, error) {
					encodes.Add(1)
					return "encoded", nil
				}),
			)
			err := store.write(context.Background(), Entry{Key: "preflight:" + test.name, Type: test.typ, Value: test.value})
			if !stdErrors.Is(err, ErrCollectionTooLarge) {
				t.Fatalf("Write() error = %v, want ErrCollectionTooLarge", err)
			}
			if got := encodes.Load(); got != 0 {
				t.Fatalf("encoder calls = %d, want 0", got)
			}
			if got := commands.calls.Load(); got != 0 {
				t.Fatalf("Redis commands = %d, want 0", got)
			}
		})
	}
}
