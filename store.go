package tablecache

import (
	"context"
	"time"
)

// Store 定义缓存写入与分布式锁能力，业务框架只要实现该接口即可复用 Manager。
type Store interface {
	// Delete 删除一个或多个缓存 key。
	Delete(ctx context.Context, keys ...string) error
	// DeletePattern 按 pattern 增量删除匹配 key，避免 KEYS 阻塞线上 Redis。
	DeletePattern(ctx context.Context, pattern string, count int64) (int64, error)
	// Exists 判断指定 key 是否存在。
	Exists(ctx context.Context, key string) (bool, error)
	// Read 按缓存类型读取 Redis 原始值。
	Read(ctx context.Context, key string, typ CacheType) (any, error)
	// RefreshLock 仅当锁值与持有者标识一致时续期锁。
	RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	// ReleaseLock 仅当锁值与持有者标识一致时释放锁，避免误删其它实例刚抢到的锁。
	ReleaseLock(ctx context.Context, key string, value string) (bool, error)
	// SetNX 设置 Redis 分布式轻量锁。
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	// Write 写入单条缓存数据。
	Write(ctx context.Context, entry Entry) error
	// WriteBatch 批量写入缓存数据。
	WriteBatch(ctx context.Context, entries []Entry) error
}

// ExistsMultiStore 表示可选的批量 Exists 能力。
// Manager 在等待其它实例重建时会优先使用该能力，以减少多次 Exists 带来的网络往返。
type ExistsMultiStore interface {
	// ExistsMulti 批量判断 key 是否存在，返回 map[key]exists。
	ExistsMulti(ctx context.Context, keys ...string) (map[string]bool, error)
}

// Metrics 定义 tablecache 运行指标记录接口，可由 Prometheus、StatsD 或自定义埋点实现。
type Metrics interface {
	// RecordRefresh 记录一次刷新执行的结果与耗时。
	RecordRefresh(ctx context.Context, index string, result string, duration time.Duration)
	// RecordCacheHit 记录一次缓存命中次数。
	RecordCacheHit(ctx context.Context, index string)
	// RecordCacheMiss 记录一次缓存未命中次数。
	RecordCacheMiss(ctx context.Context, index string)
	// RecordLockFailed 记录一次分布式锁竞争失败次数。
	RecordLockFailed(ctx context.Context, index string)
}

// ExtendedMetrics 定义可选的细分指标接口；实现后 Manager 会自动记录更丰富的生产排障指标。
type ExtendedMetrics interface {
	Metrics
	// RecordLoaderError 记录加载器回源错误次数与错误信息。
	RecordLoaderError(ctx context.Context, index string, err error)
	// RecordEmptyMarkerWrite 记录空值占位写入次数。
	RecordEmptyMarkerWrite(ctx context.Context, index string)
	// RecordWaitTimeout 记录等待其它实例重建超时次数。
	RecordWaitTimeout(ctx context.Context, index string)
	// RecordPrefixWait 记录前缀全量刷新阻塞单 key 刷新的次数。
	RecordPrefixWait(ctx context.Context, index string)
	// RecordPrefixRetry 记录单 key 刷新因前缀全量刷新而重试的次数。
	RecordPrefixRetry(ctx context.Context, index string)
	// RecordPrefixDelete 记录前缀删除次数与删除 key 数量。
	RecordPrefixDelete(ctx context.Context, index string, prefix string, count int64)
	// RecordRefreshEntryCount 记录单次刷新写入条数。
	RecordRefreshEntryCount(ctx context.Context, index string, count int)
}

// LookupMetrics 定义读取链路细分指标接口，用于区分 hit/miss/empty 与读穿刷新触发次数。
type LookupMetrics interface {
	Metrics
	// RecordLookupState 记录读取最终状态（hit/miss/empty）。
	RecordLookupState(ctx context.Context, index string, state LookupState)
	// RecordLookupRefreshTriggered 记录 GetOrRefresh/LoadThrough 在 miss 后触发回源刷新的次数。
	RecordLookupRefreshTriggered(ctx context.Context, index string)
}

// RefreshBatchMetrics 定义批量刷新与全量刷新任务的汇总指标接口。
type RefreshBatchMetrics interface {
	Metrics
	// RecordRefreshBatch 记录批量刷新/全量刷新任务的汇总信息。
	RecordRefreshBatch(ctx context.Context, mode string, result string, total int, success int, failed int)
}

// Logger 定义可选日志接口，兼容传统 printf 风格日志实现。
// 若业务已统一使用 go-utils.Logger，也可直接通过 WithLogger 传入。
type Logger interface {
	// Debugf 输出调试日志。
	Debugf(format string, args ...any)
	// Infof 输出信息日志。
	Infof(format string, args ...any)
	// Warnf 输出警告日志。
	Warnf(format string, args ...any)
	// Errorf 输出错误日志。
	Errorf(format string, args ...any)
}

// Encoder 定义复杂缓存值的序列化函数，默认使用 encoding/json。
type Encoder func(value any) (string, error)

// Decoder 定义缓存读取后的反序列化函数，默认使用 encoding/json。
type Decoder func(data []byte, dest any) error
