package tablecache

import (
	"context"
	"time"
)

// Store 定义 Manager 依赖的读取、批量变更和分布式锁契约。
type Store interface {
	// AcquireRefreshLock 原子获取刷新锁；竞争失败时同时返回当前 owner。
	AcquireRefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (locked bool, owner string, err error)
	// ApplyMutation 按故障安全顺序提交一次缓存变更。
	ApplyMutation(ctx context.Context, mutation StoreMutation) error
	// Exists 判断指定 key 是否存在。
	Exists(ctx context.Context, key string) (bool, error)
	// ExistsMulti 批量判断 key 是否存在，返回 map[key]exists。
	ExistsMulti(ctx context.Context, keys ...string) (map[string]bool, error)
	// Read 按缓存类型读取 Redis 原始值。
	Read(ctx context.Context, key string, typ CacheType) (any, error)
	// RefreshLock 仅当锁值与持有者标识一致时续期锁。
	RefreshLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	// ReleaseLock 仅当锁值与持有者标识一致时释放锁，避免误删其它实例刚抢到的锁。
	ReleaseLock(ctx context.Context, key string, value string) (bool, error)
}

// StoreValidator 定义 table-cache 构造期专用校验能力，避免碰撞自定义 Store 的通用 Validate 方法。
type StoreValidator interface {
	// ValidateTablecacheStore 校验 Store 配置能否安全服务 table-cache。
	ValidateTablecacheStore() error
}

// refreshSnapshot 保存 RedisStore 的一次读穿快照；handled=false 时 Manager 使用通用 Store 回退路径。
type refreshSnapshot struct {
	Value       any    // Value 是已命中的业务原始值
	ValueReady  bool   // ValueReady 表示业务值完整命中
	Result      string // Result 是读取业务值前观察到的完成代际
	ResultReady bool   // ResultReady 表示完成代际存在
}

// CollectionPageStore 表示可选的集合分页读取能力。
// RedisStore 默认实现该接口；业务可用它替代 HGetAll/LRange 0 -1 等全量读取。
type CollectionPageStore interface {
	// ReadPage 按缓存类型分页读取 Redis 原始值。
	ReadPage(ctx context.Context, key string, typ CacheType, options ReadPageOptions) (ReadPageResult, error)
}

// HashFieldsStore 定义带锁删除部分 Hash 字段的能力。
// RedisStore 默认实现该接口；fields 回源为空时必须删除请求范围内的旧值，不能把过期字段继续当成命中。
type HashFieldsStore interface {
	// DeleteHashFields 仅在全部 guard 仍归当前请求持有时删除指定 Hash 字段，并清除该 Hash 的字段空值 registry。
	DeleteHashFields(ctx context.Context, guards []LockGuard, key string, registryKey string, fields []string) error
}

// PrefixIndexStore 定义按前缀安全超集索引删除的能力，用于大 keyspace 下绕开全库 SCAN。
// RedisStore 默认实现该接口；自定义 Store 未实现时，Manager 仅可通过 GuardedPatternStore 执行带锁 guard 的 SCAN 降级。
type PrefixIndexStore interface {
	// DeletePrefixIndexKeys 分批遍历安全超集索引并删除真实 Redis key，索引成员在线保留。
	DeletePrefixIndexKeys(ctx context.Context, indexKey string, count int64, guards []LockGuard) (int64, error)
}

// PrefixReplaceStore 定义前缀目标的安全全量替换能力。
// 实现必须先完整校验并写入新数据，再删除不在 KeepKeys 中的旧数据；失败时允许保留冗余数据，但不能先清空旧前缀。
type PrefixReplaceStore interface {
	// ReplacePrefix 写入当前快照并删除旧快照中的过期 key，返回实际删除数量。
	ReplacePrefix(ctx context.Context, mutation PrefixReplaceMutation) (int64, error)
}

// PrefixReplaceMutation 描述一次前缀全量快照替换。
type PrefixReplaceMutation struct {
	Guards         []LockGuard   // Guards 是每批真实写删必须原子校验的锁 owner
	Prefix         string        // Prefix 是全量替换的业务前缀，用于校验分布式 Redis 的同槽约束
	Entries        []Entry       // Entries 是本次快照需要写入的业务或元信息条目
	KeepKeys       []string      // KeepKeys 是替换成功后必须保留的真实 Redis key，通常覆盖 Entries 及本轮保留元信息
	DeletePatterns []string      // DeletePatterns 是旧索引不可信时用于枚举旧 key 的已转义 Redis glob pattern
	IndexKey       string        // IndexKey 是前缀目标的索引 manifest key
	ReadyKey       string        // ReadyKey 是当前索引可信标记，只用于选择索引或 SCAN 枚举路径
	IndexTTL       time.Duration // IndexTTL 是索引可信窗口，分片成员会保留更长的安全窗口
	ScanCount      int64         // ScanCount 是索引或 keyspace 分页枚举的单批建议数量
}

// MarkerReplaceStore 定义 String marker 与旧缓存的一致切换能力。
type MarkerReplaceStore interface {
	// ReplaceWithMarker 先写入 marker，成功后再删除同槽旧 key。
	ReplaceWithMarker(ctx context.Context, guards []LockGuard, marker Entry, deleteKeys ...string) error
}

// FieldsEmptyStore 定义 Hash 字段组合空值的有界 registry 能力。
type FieldsEmptyStore interface {
	// ReplaceFieldsWithEmpty 删除请求字段并按独立 TTL 登记字段组合为空。
	ReplaceFieldsWithEmpty(ctx context.Context, guards []LockGuard, key string, registryKey string, fieldsID string, fields []string, ttl time.Duration) error
	// HasFieldsEmpty 判断字段组合是否仍在有效期内。
	HasFieldsEmpty(ctx context.Context, registryKey string, fieldsID string) (bool, error)
}

// GuardedPatternStore 定义带锁 owner 原子校验的 SCAN 删除能力。
type GuardedPatternStore interface {
	// DeletePatternGuarded 仅在全部 guard 仍归当前请求持有时删除每批匹配 key。
	DeletePatternGuarded(ctx context.Context, pattern string, count int64, guards []LockGuard) (int64, error)
}

// PrefixMutationValidator 在回源前校验前缀全量写删能否安全执行。
type PrefixMutationValidator interface {
	// ValidatePrefixMutation 拒绝无法与前缀锁原子提交的分布式跨槽前缀。
	ValidatePrefixMutation(ctx context.Context, prefix string) error
}

// PrefixMutationTopologyStore 暴露无需网络探测的前缀原子提交拓扑能力。
type PrefixMutationTopologyStore interface {
	// AllowsPrefixMutation 表示当前部署能否安全执行该前缀的全量写删与索引维护。
	AllowsPrefixMutation(prefix string) bool
}

// LockGuard 表示一次 Redis 提交必须仍持有的分布式锁 owner。
type LockGuard struct {
	Key   string // Key 是刷新锁 Redis key
	Owner string // Owner 是抢锁前生成且仅当前请求持有的随机标识
}

// StoreMutation 描述一次缓存刷新或清理需要提交到底层 Store 的变更集合。
type StoreMutation struct {
	Guards       []LockGuard           // Guards 是业务写删同一原子边界内必须校验的锁 owner
	DeleteKeys   []string              // DeleteKeys 是需要删除的真实 Redis key 列表，来源于旧业务 key、空值元信息或重建结果元信息
	WriteEntries []Entry               // WriteEntries 是需要写入的缓存条目，来源于 Loader 结果或内部元信息
	AddIndex     []PrefixIndexMutation // AddIndex 表示需要把哪些 key 加入前缀索引集合，通常来源于写入成功的业务 key 或元信息 key
}

// PrefixIndexMutation 描述一次前缀索引集合的成员变更。
type PrefixIndexMutation struct {
	IndexKey string        // IndexKey 是前缀目标对应的索引集合 Redis key
	TTL      time.Duration // TTL 是安全超集索引加入成员时刷新的有效保留时间，由 Store 校验
	Keys     []string      // Keys 是需要加入索引集合的真实 Redis key 成员列表
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

// ScanFallbackMetrics 定义前缀删除降级全库扫描的可选指标。
type ScanFallbackMetrics interface {
	Metrics
	// RecordScanFallback 记录一次 DeleteByPrefix 索引未命中后的 SCAN 降级。
	RecordScanFallback(ctx context.Context, index string, prefix string)
}

// Logger 定义可选的 printf 风格日志接口。
// go-utils.Logger 应通过 WithGoUtilsLogger 接入。
type Logger interface {
	// Infof 输出信息日志。
	Infof(format string, args ...any)
	// Warnf 输出警告日志。
	Warnf(format string, args ...any)
}

// Encoder 定义复杂缓存值的序列化函数，默认使用 encoding/json。
type Encoder func(value any) (string, error)

// Decoder 定义缓存读取后的反序列化函数，默认使用 encoding/json。
type Decoder func(data []byte, dest any) error
