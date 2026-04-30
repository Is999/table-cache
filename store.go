package tablecache

import (
	"context"
	"time"
)

// Store 定义缓存写入与分布式锁能力，业务框架只要实现该接口即可复用 Manager。
type Store interface {
	Delete(ctx context.Context, keys ...string) error
	DeletePattern(ctx context.Context, pattern string, count int64) (int64, error)
	Exists(ctx context.Context, key string) (bool, error)
	Read(ctx context.Context, key string, typ CacheType) (any, error)
	RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key string, value string) (bool, error)
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Write(ctx context.Context, entry Entry) error
	WriteBatch(ctx context.Context, entries []Entry) error
}

// Metrics 定义 tablecache 运行指标记录接口，可由 Prometheus、StatsD 或自定义埋点实现。
type Metrics interface {
	RecordRefresh(ctx context.Context, index string, result string, duration time.Duration)
	RecordCacheHit(ctx context.Context, index string)
	RecordCacheMiss(ctx context.Context, index string)
	RecordLockFailed(ctx context.Context, index string)
}

// ExtendedMetrics 定义可选的细分指标接口；实现后 Manager 会自动记录更丰富的生产排障指标。
type ExtendedMetrics interface {
	Metrics
	RecordLoaderError(ctx context.Context, index string, err error)
	RecordEmptyMarkerWrite(ctx context.Context, index string)
	RecordWaitTimeout(ctx context.Context, index string)
	RecordPrefixWait(ctx context.Context, index string)
	RecordPrefixRetry(ctx context.Context, index string)
	RecordPrefixDelete(ctx context.Context, index string, prefix string, count int64)
	RecordRefreshEntryCount(ctx context.Context, index string, count int)
}

// LookupMetrics 定义读取链路细分指标接口，用于区分 hit/miss/empty 与读穿刷新触发次数。
type LookupMetrics interface {
	Metrics
	RecordLookupState(ctx context.Context, index string, state LookupState)
	RecordLookupRefreshTriggered(ctx context.Context, index string)
}

// RefreshBatchMetrics 定义批量刷新与全量刷新任务的汇总指标接口。
type RefreshBatchMetrics interface {
	Metrics
	RecordRefreshBatch(ctx context.Context, mode string, result string, total int, success int, failed int)
}

// Logger 定义可选日志接口，兼容传统 printf 风格日志实现。
// 若业务已统一使用 go-utils.Logger，也可直接通过 WithLogger 传入。
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Encoder 定义复杂缓存值的序列化函数，默认使用 encoding/json。
type Encoder func(value any) (string, error)

// Decoder 定义缓存读取后的反序列化函数，默认使用 encoding/json。
type Decoder func(data []byte, dest any) error
