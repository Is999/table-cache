package tablecache

import (
	"context"
	"crypto/rand"

	// sha1 仅用于日志 key 脱敏摘要；不用于任何安全/加密场景。
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	// defaultLockTTL 表示缓存重建锁默认持有时间。
	defaultLockTTL = 10 * time.Second
	// defaultKeyPrefix 表示业务缓存 key 的默认命名空间前缀，用于和非 tablecache 管理的 Redis key 隔离。
	defaultKeyPrefix = "tc:"
	// defaultWaitStepMin 表示默认动态等待策略的最小轮询间隔。
	defaultWaitStepMin = 50 * time.Millisecond
	// defaultWaitStepMax 表示默认动态等待策略的最大轮询间隔。
	defaultWaitStepMax = 250 * time.Millisecond
	// defaultEmptyTTL 表示空值占位缓存默认过期时间。
	defaultEmptyTTL = 2 * time.Minute
	// defaultScanCount 表示清理前缀缓存时 Redis SCAN 的单次扫描数量。
	defaultScanCount = int64(1000)
	// defaultPrefixDeleteConcurrency 表示 DeleteByPrefix 默认按 pattern 串行删除，避免默认清理任务过度抢占 Redis。
	defaultPrefixDeleteConcurrency = 1
	// defaultAllowScanFallback 表示默认禁止索引未就绪时在线降级全库 SCAN。
	defaultAllowScanFallback = false
	// defaultRebuildResultTTL 表示重建完成元信息默认保留时间。
	defaultRebuildResultTTL = 30 * time.Second
	// defaultPrefixEpochTTL 表示前缀全量刷新代际元信息默认保留时间，避免长期运行产生无限元信息堆积。
	defaultPrefixEpochTTL = 7 * 24 * time.Hour
	// defaultPrefixKeyIndexTTL 表示前缀 key 索引默认保留时间；过期后会自动降级全库 SCAN，避免索引长期无人维护。
	defaultPrefixKeyIndexTTL = 30 * 24 * time.Hour
	// defaultRefreshConcurrency 表示批量刷新默认并发度，1 表示保守串行。
	defaultRefreshConcurrency = 1
	// defaultLoaderTimeout 表示 Loader 默认最长执行时间，避免异常回源长期占用锁和并发槽。
	defaultLoaderTimeout = 30 * time.Second
	// defaultLoaderConcurrency 表示单个 Manager 默认允许的并发回源数量。
	defaultLoaderConcurrency = 32
	// minLockTTL 表示生产刷新锁允许的最短租约，避免亚毫秒级续期风暴。
	minLockTTL = 30 * time.Millisecond
	// minLockRenewInterval 表示显式锁续期允许的最短周期。
	minLockRenewInterval = 10 * time.Millisecond
	// minWaitStep 表示显式锁等待允许的最短轮询周期。
	minWaitStep = time.Millisecond
	// maxWaitAttempts 表示单轮显式锁等待允许的最大轮询次数。
	maxWaitAttempts = 100_000
	// maxWorkerConcurrency 表示 Manager 批处理和 Loader 允许的并发硬上限。
	maxWorkerConcurrency = 256
	// maxScanCount 表示单次 Redis SCAN count 的生产安全硬上限。
	maxScanCount = int64(100_000)
	// maxOperationTimeout 表示锁和Loader允许的最大生产超时，避免异常配置放大等待循环。
	maxOperationTimeout = 24 * time.Hour
	// maxReadbackTimeout 表示刷新完成后最后一次缓存回读的内部超时。
	maxReadbackTimeout = 30 * time.Second
	// refreshOperationBudgetMultiplier 覆盖等待现有 owner、自身 Loader 和两次前缀提交锁窗口。
	refreshOperationBudgetMultiplier = 4
)

const (
	// metaKeyRoot 表示内部元信息 Redis key 根前缀，保持短前缀并与默认业务前缀 tc: 区分。
	metaKeyRoot = "tcm"
)

var (
	// ErrTargetNotFound 表示没有找到匹配的缓存目标配置。
	ErrTargetNotFound = errors.New("tablecache target not found")
	// ErrLoaderRequired 表示缓存目标缺少数据加载函数。
	ErrLoaderRequired = errors.New("tablecache loader required")
	// ErrWaitRebuildTimeout 表示等待其它实例重建缓存超时。
	ErrWaitRebuildTimeout = errors.New("tablecache wait rebuild timeout")
	// ErrRefreshLockLost 表示当前实例在刷新过程中失去重建锁所有权。
	ErrRefreshLockLost = errors.New("tablecache refresh lock lost")
	// ErrCacheMiss 表示缓存未命中。
	ErrCacheMiss = errors.New("tablecache cache miss")
	// ErrNotFound 表示加载器确认源数据不存在，可触发空值占位。
	ErrNotFound = errors.New("tablecache data not found")
	// ErrKeyPrefixRequired 表示刷新、删除或写回操作必须使用已带 Manager 指定前缀的实际 Redis key。
	ErrKeyPrefixRequired = errors.New("tablecache key prefix required")
	// ErrInvalidClusterHashTag 表示 key 中的花括号无法安全生成 Redis Cluster 同槽元信息 key。
	ErrInvalidClusterHashTag = errors.New("tablecache invalid redis cluster hash tag key")
	// ErrDistributedPrefixHashTagRequired 表示分布式 Redis 的前缀全量写删缺少固定 hash tag。
	ErrDistributedPrefixHashTagRequired = errors.New("tablecache distributed prefix hash tag required")
	// ErrEntryKeyOutOfScope 表示加载器返回了不属于当前缓存目标管理范围的写入 key。
	ErrEntryKeyOutOfScope = errors.New("tablecache entry key out of target scope")
	// ErrRefreshInvalidated 表示刷新期间发生显式删除，当前刷新结果已失效。
	ErrRefreshInvalidated = errors.New("tablecache refresh invalidated")
	// ErrScanFallbackDisabled 表示 DeleteByPrefix 需要降级 SCAN，但当前配置禁止降级。
	ErrScanFallbackDisabled = errors.New("tablecache scan fallback disabled")
	// ErrCollectionTooLarge 表示集合读写超过配置上限。
	ErrCollectionTooLarge = errors.New("tablecache collection too large")
	// ErrInvalidKeyPrefix 表示 key 前缀不符合生产安全配置。
	ErrInvalidKeyPrefix = errors.New("tablecache invalid key prefix")
	// ErrInvalidConfig 表示 Manager 或 Target 配置存在不安全取值。
	ErrInvalidConfig = errors.New("tablecache invalid config")
	// ErrPartialHashUnsupported 表示当前 Store 或目标配置不支持安全的 Hash 局部刷新。
	ErrPartialHashUnsupported = errors.New("tablecache partial hash unsupported")
	// ErrPrefixReplaceUnsupported 表示 Store 不支持无破坏的前缀全量替换。
	ErrPrefixReplaceUnsupported = errors.New("tablecache prefix replace unsupported")
	// ErrPrefixDeleteUnsupported 表示 Store 不支持带锁校验的前缀删除。
	ErrPrefixDeleteUnsupported = errors.New("tablecache prefix delete unsupported")
	// ErrRedisTopologyUnsupported 表示当前 Redis 客户端拓扑不能安全满足 table-cache 一致性契约。
	ErrRedisTopologyUnsupported = errors.New("tablecache redis topology unsupported")
	// ErrMarkerReplaceUnsupported 表示 Store 不支持 marker 与旧缓存的安全切换。
	ErrMarkerReplaceUnsupported = errors.New("tablecache marker replace unsupported")
	// errPrefixRefreshBusy 表示当前单 key 刷新遇到同前缀全量刷新，应等待后重试。
	errPrefixRefreshBusy = errors.New("tablecache prefix refresh busy")
)

var (
	// lockValueFallbackSeq 在系统随机源失败时保证同进程 owner 标识不重复。
	lockValueFallbackSeq atomic.Uint64
	// lockValueFallbackSeed 隔离不同主机和进程的随机源失败路径。
	lockValueFallbackSeed = fallbackLockValueSeed()
)

// Option 表示 Manager 的可选配置。
type Option func(*Manager)

// Manager 管理一组表数据缓存目标，负责刷新、锁保护、TTL 抖动与空值占位。
type Manager struct {
	store                   Store                // store 是底层缓存存储适配器
	targets                 map[string]Target    // targets 按目标 Index 保存缓存配置，key 为业务声明的唯一索引，value 为已归一化目标
	ordered                 []Target             // ordered 保留配置顺序，便于列表展示和批量刷新
	fixedKeys               map[string]Target    // fixedKeys 按实际 Redis key 保存固定目标，key 为带命名空间后的完整缓存 key
	prefixTargets           []Target             // prefixTargets 仅包含前缀型目标，并按 key 长度降序排列以确保最长前缀优先
	prefixTargetsM          map[string]Target    // prefixTargetsM 按实际 Redis 前缀保存前缀目标，key 为带命名空间后的缓存前缀
	keyPrefix               string               // keyPrefix 是业务缓存 key 命名空间前缀，避免误删或覆盖非托管 Redis key
	lockTTL                 time.Duration        // lockTTL 是缓存重建锁持有时间
	waitStep                time.Duration        // waitStep 是等待其它实例重建时的轮询间隔
	waitTimes               int                  // waitTimes 是等待其它实例重建的最大次数
	waitConfigured          bool                 // waitConfigured 标识是否显式配置了等待策略
	emptyMarker             string               // emptyMarker 是空值缓存占位内容
	emptyTTL                time.Duration        // emptyTTL 是空值缓存默认过期时间
	scanCount               int64                // scanCount 是清理前缀缓存时的 SCAN count，值越大网络往返越少但单轮 Redis 工作量越高
	prefixDeleteConcurrency int                  // prefixDeleteConcurrency 是 DeleteByPrefix 同时扫描删除的 pattern 数，用于大 keyspace 下缩短清理耗时
	allowScanFallback       bool                 // allowScanFallback 表示索引不可用时是否允许降级全库 SCAN
	lockRenew               time.Duration        // lockRenew 是缓存重建锁续期间隔，0 表示按 lockTTL 自动计算
	resultTTL               time.Duration        // resultTTL 是重建完成元信息保留时间
	prefixEpochTTL          time.Duration        // prefixEpochTTL 是前缀全量刷新代际元信息保留时间
	prefixKeyIndex          bool                 // prefixKeyIndex 表示是否启用前缀 key 索引，用于大 keyspace 下优先绕开全库 SCAN
	prefixKeyIndexTTL       time.Duration        // prefixKeyIndexTTL 是前缀 key 索引和可信标记的保留时间，过期后自动降级扫描
	loaderTTL               time.Duration        // loaderTTL 是默认 Loader 最长执行时间
	loaderConcurrency       int                  // loaderConcurrency 是全局 Loader 并发上限
	loaderSlots             chan struct{}        // loaderSlots 控制不同 key 回源总并发，避免随机 miss 压垮数据源
	ctxPolicy               RebuildContextPolicy // ctxPolicy 是缓存重建上下文取消策略
	concurrency             int                  // concurrency 是批量刷新并发度
	redactLogKeys           bool                 // redactLogKeys 表示事件日志中 key/prefix 类字段只输出摘要
	logger                  Logger               // logger 是可选日志适配器
	metrics                 Metrics              // metrics 是可选运行指标记录器
	decoder                 Decoder              // decoder 是缓存读取反序列化函数
	group                   singleflight.Group   // group 合并进程内相同 key 的并发刷新请求
}

// refreshOptions 表示一次刷新执行过程中的内部控制项。
type refreshOptions struct {
	requested           bool                  // requested 表示当前刷新不是 RefreshAll，可按目标配置提交空值 marker
	missTriggered       bool                  // missTriggered 表示刷新前已发生 miss，拿锁后必须二次检查缓存
	lookupTarget        Target                // lookupTarget 保留注册目标的读取语义，不受本次写 marker 开关覆盖
	observedResult      string                // observedResult 是初次读取前看到的同语义完成 owner
	observedResultReady bool                  // observedResultReady 表示初次读取前完成标记存在
	contextPolicy       *RebuildContextPolicy // contextPolicy 表示本次刷新是否覆盖默认上下文取消策略
	flightToken         string                // flightToken 隔离带单次覆盖项的刷新，避免复用其它调用语义
}

// refreshEpochSnapshot 表示刷新启动时看到的删除/前缀代际，用于写回前识别显式删除或全量刷新交错。
type refreshEpochSnapshot struct {
	prefixRefresh      string // prefixRefresh 是同前缀全量刷新代际
	prefixDelete       string // prefixDelete 是当前前缀显式删除代际
	prefixMemberDelete string // prefixMemberDelete 是前缀成员显式删除代际，仅约束全量刷新
}

// waitRebuildOutcome 表示等待其它实例刷新后的处理结果。
type waitRebuildOutcome int

const (
	// waitRebuildReady 表示当前请求依赖的数据已被其它实例准备好，可直接返回。
	waitRebuildReady waitRebuildOutcome = iota + 1
	// waitRebuildRetry 表示当前锁域已释放，但目标 key 仍未就绪，应重新尝试抢锁。
	waitRebuildRetry
)

// NewManager 创建通用表缓存管理器。
func NewManager(store Store, targets []Target, opts ...Option) (*Manager, error) {
	if isNilInterface(store) {
		return nil, errors.Errorf("tablecache store不能为空")
	}
	if validator, ok := store.(StoreValidator); ok {
		if err := validator.ValidateTablecacheStore(); err != nil {
			return nil, errors.Wrapf(ErrInvalidConfig, "Store配置非法: %v", err)
		}
	}
	manager := &Manager{
		store:                   store,
		targets:                 make(map[string]Target, len(targets)),
		ordered:                 make([]Target, 0, len(targets)),
		fixedKeys:               make(map[string]Target, len(targets)),
		prefixTargets:           make([]Target, 0, len(targets)),
		prefixTargetsM:          make(map[string]Target, len(targets)),
		keyPrefix:               defaultKeyPrefix,
		lockTTL:                 defaultLockTTL,
		emptyMarker:             DefaultEmptyMarker,
		emptyTTL:                defaultEmptyTTL,
		scanCount:               defaultScanCount,
		prefixDeleteConcurrency: defaultPrefixDeleteConcurrency,
		allowScanFallback:       defaultAllowScanFallback,
		resultTTL:               defaultRebuildResultTTL,
		prefixEpochTTL:          defaultPrefixEpochTTL,
		prefixKeyIndex:          true,
		prefixKeyIndexTTL:       defaultPrefixKeyIndexTTL,
		loaderTTL:               defaultLoaderTimeout,
		loaderConcurrency:       defaultLoaderConcurrency,
		concurrency:             defaultRefreshConcurrency,
		redactLogKeys:           true,
		decoder:                 json.Unmarshal,
	}
	for _, opt := range opts {
		if isNilInterface(opt) {
			return nil, errors.Wrapf(ErrInvalidConfig, "Manager Option不能为空")
		}
		opt(manager)
	}
	if err := manager.validateConfig(); err != nil {
		return nil, errors.Tag(err)
	}
	manager.loaderSlots = make(chan struct{}, manager.loaderConcurrency)
	if err := validateManagerKeyPrefix(manager.keyPrefix); err != nil {
		return nil, errors.Tag(err)
	}
	for _, target := range targets {
		normalized, err := normalizeTarget(target, manager.emptyTTL)
		if err != nil {
			return nil, errors.Tag(err)
		}
		if err := manager.validateLogicalTargetNamespace(normalized); err != nil {
			return nil, errors.Tag(err)
		}
		// 目标注册前统一转换成实际 Redis key 范围，后续匹配、删除和写回都基于同一命名空间判断。
		normalized = manager.withKeyPrefixTarget(normalized)
		if err := validateRedisClusterHashTagKey(normalized.Key); err != nil {
			return nil, errors.Tag(err)
		}
		if err := manager.validateTargetRuntimeConfig(normalized); err != nil {
			return nil, errors.Tag(err)
		}
		if err := manager.registerTarget(normalized); err != nil {
			return nil, errors.Tag(err)
		}
	}
	return manager, nil
}

// isNilInterface 识别接口中的 typed nil，避免构造期通过后在首次方法调用时 panic。
func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}

// validateLogicalTargetNamespace 拒绝 Target 预拼项目级前缀，避免逻辑 key 与物理 key 输入产生歧义。
func (m *Manager) validateLogicalTargetNamespace(target Target) error {
	if strings.HasPrefix(strings.TrimSpace(target.Key), m.keyPrefix) {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]的Key必须是不含项目级前缀[%s]的逻辑key", target.Index, m.keyPrefix)
	}
	if strings.HasPrefix(strings.TrimSpace(target.KeyTitle), m.keyPrefix) {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]的KeyTitle必须是不含项目级前缀[%s]的逻辑key", target.Index, m.keyPrefix)
	}
	return nil
}

// WithKeyPrefix 设置业务缓存 key 命名空间前缀，用于把 tablecache 管理的业务数据与其它 Redis key 隔离。
// 默认前缀为 "tc:"；配置必须以冒号结尾，刷新、删除和写回只接受已带该前缀的实际 Redis key。
func WithKeyPrefix(prefix string) Option {
	return func(manager *Manager) {
		manager.keyPrefix = strings.TrimSpace(prefix)
	}
}

// WithLockTTL 设置缓存重建锁持有时间。
func WithLockTTL(ttl time.Duration) Option {
	return func(manager *Manager) {
		if ttl != 0 {
			manager.lockTTL = ttl
		}
	}
}

// WithLockRenewInterval 设置缓存重建锁续期间隔。
func WithLockRenewInterval(interval time.Duration) Option {
	return func(manager *Manager) {
		manager.lockRenew = interval
	}
}

// WithRebuildResultTTL 设置重建完成元信息保留时间。
func WithRebuildResultTTL(ttl time.Duration) Option {
	return func(manager *Manager) {
		if ttl != 0 {
			manager.resultTTL = ttl
		}
	}
}

// WithPrefixEpochTTL 设置前缀全量刷新代际元信息保留时间。
func WithPrefixEpochTTL(ttl time.Duration) Option {
	return func(manager *Manager) {
		if ttl != 0 {
			manager.prefixEpochTTL = ttl
		}
	}
}

// WithPrefixKeyIndex 设置是否启用前缀 key 索引。
// 启用后 Manager 会在前缀目标写入时维护索引，并在 DeleteByPrefix 时优先按索引删除；降级 SCAN 仍要求 Store 实现 GuardedPatternStore。
func WithPrefixKeyIndex(enabled bool) Option {
	return func(manager *Manager) {
		manager.prefixKeyIndex = enabled
	}
}

// WithPrefixKeyIndexTTL 设置前缀 key 索引与可信标记的保留时间。
// TTL 过短会让删除更容易降级到 SCAN；TTL 过长会保留更多可能已过期的成员，建议按业务缓存最长 TTL 配置。
func WithPrefixKeyIndexTTL(ttl time.Duration) Option {
	return func(manager *Manager) {
		if ttl != 0 {
			manager.prefixKeyIndexTTL = ttl
		}
	}
}

// WithLoaderTimeout 设置默认 Loader 超时时间。
func WithLoaderTimeout(timeout time.Duration) Option {
	return func(manager *Manager) {
		if timeout != 0 {
			manager.loaderTTL = timeout
		}
	}
}

// WithLoaderConcurrency 设置单个 Manager 同时执行的 Loader 数量。
func WithLoaderConcurrency(concurrency int) Option {
	return func(manager *Manager) {
		manager.loaderConcurrency = concurrency
	}
}

// WithRebuildContextPolicy 设置缓存重建上下文取消策略。
func WithRebuildContextPolicy(policy RebuildContextPolicy) Option {
	return func(manager *Manager) {
		manager.ctxPolicy = policy
	}
}

// WithWait 设置未抢到锁时等待其它实例重建缓存的轮询策略。
func WithWait(step time.Duration, times int) Option {
	return func(manager *Manager) {
		if step != 0 {
			manager.waitStep = step
		}
		if times != 0 {
			manager.waitTimes = times
		}
		if step != 0 || times != 0 {
			manager.waitConfigured = true
		}
	}
}

// WithDecoder 设置缓存读取反序列化函数。
func WithDecoder(decoder Decoder) Option {
	return func(manager *Manager) {
		if decoder != nil {
			manager.decoder = decoder
		}
	}
}

// WithEmptyMarker 设置空值缓存占位内容和默认过期时间。
func WithEmptyMarker(marker string, ttl time.Duration) Option {
	return func(manager *Manager) {
		marker = strings.TrimSpace(marker)
		if marker != "" {
			manager.emptyMarker = marker
		}
		if ttl != 0 {
			manager.emptyTTL = ttl
		}
	}
}

// WithScanCount 设置前缀缓存清理时的 Redis SCAN count。
func WithScanCount(count int64) Option {
	return func(manager *Manager) {
		if count != 0 {
			manager.scanCount = count
		}
	}
}

// WithPrefixDeleteConcurrency 设置 DeleteByPrefix 同时扫描删除的 pattern 数。
// 默认 1 表示保守串行；大 keyspace 且可接受更高 Redis 瞬时压力时可提高到 2~4 来缩短清理耗时。
func WithPrefixDeleteConcurrency(concurrency int) Option {
	return func(manager *Manager) {
		if concurrency != 0 {
			manager.prefixDeleteConcurrency = concurrency
		}
	}
}

// WithScanFallback 设置 DeleteByPrefix 在前缀索引不可用时是否允许降级 SCAN。
// 默认关闭；仅在受控维护窗口显式开启，让线上调用优先建立可信索引或转后台任务。
func WithScanFallback(enabled bool) Option {
	return func(manager *Manager) {
		manager.allowScanFallback = enabled
	}
}

// WithRefreshConcurrency 设置 RefreshAll 和 RefreshByKeys 的有限并发度。
func WithRefreshConcurrency(concurrency int) Option {
	return func(manager *Manager) {
		if concurrency != 0 {
			manager.concurrency = concurrency
		}
	}
}

// WithLogger 设置缓存管理器日志适配器。
func WithLogger(logger Logger) Option {
	return func(manager *Manager) {
		if isNilInterface(logger) {
			manager.logger = nil
			return
		}
		manager.logger = logger
	}
}

// WithGoUtilsLogger 设置 go-utils.Logger 日志适配器。
// 推荐在项目已统一使用 utils.Configure(utils.WithLogger(...)) 时直接传入 utils.Log()。
func WithGoUtilsLogger(logger utils.Logger) Option {
	return func(manager *Manager) {
		if isNilInterface(logger) {
			manager.logger = nil
			return
		}
		manager.logger = &utilsLoggerAdapter{logger: logger}
	}
}

// WithLogKeyRedaction 设置事件日志中 key/prefix/lock_name/err 等可能携带业务 key 的字段是否只输出摘要，默认开启。
func WithLogKeyRedaction(enabled bool) Option {
	return func(manager *Manager) {
		manager.redactLogKeys = enabled
	}
}

// WithMetrics 设置缓存运行指标记录器。
func WithMetrics(metrics Metrics) Option {
	return func(manager *Manager) {
		if isNilInterface(metrics) {
			manager.metrics = nil
			return
		}
		manager.metrics = metrics
	}
}

// Items 返回缓存管理页展示的目标列表。
func (m *Manager) Items() []Item {
	items := make([]Item, 0, len(m.ordered))
	for _, target := range m.ordered {
		items = append(items, target.item())
	}
	return items
}

// IsFixedKey 判断 key 是否为固定内置缓存 key，模板和前缀型 key 不纳入固定 key。
func (m *Manager) IsFixedKey(key string) bool {
	key = m.withKeyPrefix(key)
	if key == "" {
		return false
	}
	for _, target := range m.ordered {
		if strings.HasSuffix(target.Key, ":") {
			continue
		}
		if target.Key == key {
			return true
		}
	}
	return false
}

// RefreshAll 刷新全部允许批量重建的缓存目标。
func (m *Manager) RefreshAll(ctx context.Context) error {
	_, _, err := m.RefreshAllWithSummary(ctx)
	return errors.Tag(err)
}

// RefreshAllDetailed 刷新全部允许批量重建的缓存目标，并逐项返回执行结果。
func (m *Manager) RefreshAllDetailed(ctx context.Context) []RefreshBatchResult {
	results, _, _ := m.RefreshAllWithSummary(ctx)
	return results
}

// RefreshAllWithSummary 刷新全部允许批量重建的缓存目标，并返回汇总信息与聚合错误。
func (m *Manager) RefreshAllWithSummary(ctx context.Context) ([]RefreshBatchResult, RefreshBatchSummary, error) {
	targets := make([]Target, 0, len(m.ordered))
	for _, target := range m.ordered {
		if !target.RefreshAll {
			continue
		}
		targets = append(targets, target)
	}
	results := make([]RefreshBatchResult, len(targets))
	group, groupCtx := errgroup.WithContext(ctx)
	if len(targets) > 0 {
		group.SetLimit(min(m.refreshConcurrency(), len(targets)))
	}
	for index, target := range targets {
		index := index
		target := target
		group.Go(func() error {
			results[index] = RefreshBatchResult{
				Key:   target.Key,
				Error: m.refreshWithSingleflight(groupCtx, target, target.Key, nil, refreshOptions{requested: false}),
			}
			return nil
		})
	}
	_ = group.Wait()
	summary := SummarizeRefreshBatchResults(results)
	m.recordRefreshBatch(ctx, "all", summary)
	m.logInfoEvent("refresh_all_done", "", "", "result", refreshBatchResultLabel(summary), "total", summary.Total, "success", summary.Success, "failed", summary.Failed)
	if summary.HasError() {
		return results, summary, errors.Tag(&RefreshBatchError{Summary: summary})
	}
	return results, summary, nil
}

// RefreshByKey 根据 Redis key 刷新匹配的缓存目标。
func (m *Manager) RefreshByKey(ctx context.Context, key string, fields ...string) error {
	target, params, err := m.resolveForMutation(key, fields)
	if err != nil {
		return errors.Tag(err)
	}
	if err := m.validateFieldsRequest(target, params.Key, params.Fields); err != nil {
		return errors.Tag(err)
	}
	return m.refreshWithSingleflight(ctx, target, params.Key, params.Fields, refreshOptions{requested: true})
}

// RefreshByKeys 批量刷新多个缓存 key；同 key 会自动去重。
func (m *Manager) RefreshByKeys(ctx context.Context, keys []string) error {
	_, _, err := m.RefreshByKeysWithSummary(ctx, keys)
	return errors.Tag(err)
}

// RefreshByKeysDetailed 批量刷新多个缓存 key，并逐项返回执行结果。
func (m *Manager) RefreshByKeysDetailed(ctx context.Context, keys []string) []RefreshBatchResult {
	results, _, _ := m.RefreshByKeysWithSummary(ctx, keys)
	return results
}

// RefreshByKeysWithSummary 批量刷新多个缓存 key，并返回汇总信息与聚合错误。
func (m *Manager) RefreshByKeysWithSummary(ctx context.Context, keys []string) ([]RefreshBatchResult, RefreshBatchSummary, error) {
	seen := make(map[string]struct{}, len(keys))
	uniqueKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		uniqueKeys = append(uniqueKeys, key)
	}
	results := make([]RefreshBatchResult, len(uniqueKeys))
	group, groupCtx := errgroup.WithContext(ctx)
	if len(uniqueKeys) > 0 {
		group.SetLimit(min(m.refreshConcurrency(), len(uniqueKeys)))
	}
	for index, key := range uniqueKeys {
		index := index
		key := key
		group.Go(func() error {
			results[index] = RefreshBatchResult{
				Key:   key,
				Error: m.RefreshByKey(groupCtx, key),
			}
			return nil
		})
	}
	_ = group.Wait()
	summary := SummarizeRefreshBatchResults(results)
	m.recordRefreshBatch(ctx, "keys", summary)
	m.logInfoEvent("refresh_batch_done", "", "", "result", refreshBatchResultLabel(summary), "total", summary.Total, "success", summary.Success, "failed", summary.Failed)
	if summary.HasError() {
		return results, summary, errors.Tag(&RefreshBatchError{Summary: summary})
	}
	return results, summary, nil
}

// Get 从缓存读取指定 key，并把缓存值反序列化到 dest。
func (m *Manager) Get(ctx context.Context, key string, dest any) (bool, error) {
	result, err := m.GetState(ctx, key, dest)
	if err != nil {
		return false, errors.Tag(err)
	}
	return result.State == LookupStateHit, nil
}

// GetState 从缓存读取指定 key，并返回命中、空值占位或未命中的明确状态。
func (m *Manager) GetState(ctx context.Context, key string, dest any) (LookupResult, error) {
	target, params, err := m.resolveForRead(key, nil)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	result, err := m.lookup(ctx, target, params, dest)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	m.recordLookupState(ctx, target.Index, result.State)
	return result, nil
}

// GetOrRefresh 先读取缓存；仅当真实未命中时触发刷新，命中空值占位时直接返回 empty。
func (m *Manager) GetOrRefresh(ctx context.Context, key string, dest any) (LookupResult, error) {
	return m.getOrRefresh(ctx, key, dest, LoadThroughOptions{})
}

// GetOrRefreshWithLoader 先读取缓存；miss 时使用本次传入的 Loader 回源、回填并返回最新结果。
func (m *Manager) GetOrRefreshWithLoader(ctx context.Context, key string, dest any, loader Loader) (LookupResult, error) {
	return m.GetOrRefreshWithOptions(ctx, key, dest, LoadThroughOptions{Loader: loader})
}

// GetOrRefreshWithOptions 先读取缓存；miss 时按本次 options 配置执行回源、回填并返回最新结果。
func (m *Manager) GetOrRefreshWithOptions(ctx context.Context, key string, dest any, options LoadThroughOptions) (LookupResult, error) {
	return m.getOrRefresh(ctx, key, dest, options)
}

// LoadThrough 提供更贴近业务侧语义的读穿缓存入口，内部复用 GetOrRefreshWithLoader。
func (m *Manager) LoadThrough(ctx context.Context, key string, dest any, loader Loader) (LookupResult, error) {
	return m.LoadThroughWithOptions(ctx, key, dest, LoadThroughOptions{Loader: loader})
}

// LoadThroughWithOptions 提供带可选参数的完整读穿缓存入口，适用于字段级局部刷新和临时覆盖 Loader。
func (m *Manager) LoadThroughWithOptions(ctx context.Context, key string, dest any, options LoadThroughOptions) (LookupResult, error) {
	return m.GetOrRefreshWithOptions(ctx, key, dest, options)
}

// LoadThroughBatch 批量执行读穿缓存；每个条目独立返回结果，单条失败不会中断整批处理。
func (m *Manager) LoadThroughBatch(ctx context.Context, items []LoadThroughItem) []LoadThroughBatchResult {
	return m.LoadThroughBatchWithBatchOptions(ctx, items, LoadThroughBatchOptions{})
}

// LoadThroughBatchWithBatchOptions 批量执行读穿缓存，并支持批次级默认参数和并发度覆盖。
func (m *Manager) LoadThroughBatchWithBatchOptions(ctx context.Context, items []LoadThroughItem, options LoadThroughBatchOptions) []LoadThroughBatchResult {
	results := make([]LoadThroughBatchResult, len(items))
	if options.Concurrency < 0 || options.Concurrency > maxWorkerConcurrency {
		err := errors.Wrapf(ErrInvalidConfig, "批量读穿并发度必须在0到%d之间: %d", maxWorkerConcurrency, options.Concurrency)
		for index, item := range items {
			results[index] = LoadThroughBatchResult{Key: strings.TrimSpace(item.Key), Error: err}
		}
		return results
	}
	var group errgroup.Group
	if len(items) > 0 {
		group.SetLimit(min(m.resolveBatchConcurrency(options.Concurrency), len(items)))
	}
	for index, item := range items {
		index := index
		item := item
		group.Go(func() error {
			key := strings.TrimSpace(item.Key)
			loadOptions := m.mergeBatchLoadThroughOptions(options.DefaultOptions, item.Options)
			result, err := m.LoadThroughWithOptions(ctx, key, item.Dest, loadOptions)
			results[index] = LoadThroughBatchResult{
				Key:          key,
				LookupResult: result,
				Error:        err,
			}
			return nil
		})
	}
	_ = group.Wait()
	return results
}

// LoadThroughBatchWithSummary 批量执行读穿缓存，并返回汇总信息与聚合错误，便于任务调度快速判定整批状态。
func (m *Manager) LoadThroughBatchWithSummary(ctx context.Context, items []LoadThroughItem) ([]LoadThroughBatchResult, LoadThroughBatchSummary, error) {
	return m.LoadThroughBatchWithSummaryOptions(ctx, items, LoadThroughBatchOptions{})
}

// LoadThroughBatchWithSummaryOptions 批量执行读穿缓存，并支持批次级默认参数、并发覆盖和汇总结果返回。
func (m *Manager) LoadThroughBatchWithSummaryOptions(ctx context.Context, items []LoadThroughItem, options LoadThroughBatchOptions) ([]LoadThroughBatchResult, LoadThroughBatchSummary, error) {
	results := m.LoadThroughBatchWithBatchOptions(ctx, items, options)
	summary := SummarizeLoadThroughBatchResults(results)
	if summary.HasError() {
		return results, summary, errors.Tag(&LoadThroughBatchError{Summary: summary})
	}
	return results, summary, nil
}

// DeleteByKey 删除指定 Redis key；前缀删除请使用 DeleteByPrefix。
func (m *Manager) DeleteByKey(ctx context.Context, key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	target, params, err := m.resolveForMutation(key, nil)
	if err != nil {
		return errors.Tag(err)
	}
	// params.Key 是 resolve 后的实际 Redis key，删除业务 key 与内部元信息必须使用同一个命名空间。
	key = params.Key
	return m.withExclusiveRefreshLock(ctx, target, key, nil, func(lockCtx context.Context, refreshGuard LockGuard) error {
		return m.withPrefixCommitLock(lockCtx, target, false, func(commitCtx context.Context, commitGuard LockGuard) error {
			guards := commitLockGuards(refreshGuard, commitGuard)
			if commitGuard.Key != "" {
				if err := m.writePrefixMemberDeleteEpoch(commitCtx, target, guards); err != nil {
					return errors.Tag(err)
				}
			}
			return m.deleteTargetKeys(
				commitCtx,
				target,
				guards,
				key,
				m.emptyKey(key),
				m.emptyCollectionKey(key),
				m.fieldsEmptyKey(key),
				m.refreshResultKey(key, nil, refreshOptions{requested: true}),
				m.refreshResultKey(key, nil, refreshOptions{requested: true, missTriggered: true}),
				m.refreshResultKey(key, nil, refreshOptions{requested: false}),
			)
		})
	})
}

// DeleteByPrefix 删除指定 Redis key 前缀下的所有缓存。
func (m *Manager) DeleteByPrefix(ctx context.Context, prefix string) error {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil
	}
	if err := m.requireKeyPrefix(prefix); err != nil {
		return errors.Tag(err)
	}
	target, err := m.resolvePrefixTarget(prefix)
	if err != nil {
		return errors.Tag(err)
	}
	if err := m.validateStrongPrefixOperation(ctx, target); err != nil {
		return errors.Tag(err)
	}
	return m.withPrefixCommitLock(ctx, target, false, func(lockCtx context.Context, guard LockGuard) error {
		guards := []LockGuard{guard}
		if err := m.writePrefixDeleteEpoch(lockCtx, target, guards); err != nil {
			return errors.Tag(err)
		}
		return m.deleteByPrefixUnlocked(lockCtx, target, guards)
	})
}

// deleteByPrefixUnlocked 执行前缀删除本体；调用方负责处理外部删除与刷新之间的互斥和代际标记。
func (m *Manager) deleteByPrefixUnlocked(ctx context.Context, target Target, guards []LockGuard) error {
	// indexCount/indexed/indexErr 表示索引快路径的删除结果；indexed=false 时说明索引未可信，应继续降级 SCAN。
	indexCount, indexed, indexErr := m.deletePrefixByIndex(ctx, target, guards)
	if indexErr != nil {
		return errors.Tag(indexErr)
	}
	if indexed {
		m.recordPrefixDelete(ctx, target.Index, target.Key, indexCount)
		return nil
	}
	if !m.allowScanFallback {
		return errors.Wrapf(ErrScanFallbackDisabled, "缓存前缀[%s]索引未就绪", target.Key)
	}
	m.recordScanFallback(ctx, target.Index, target.Key)
	m.logWarnEvent("prefix_scan_fallback", target.Index, target.Key)
	// patterns 覆盖业务缓存 key 与 tablecache 内部元信息 key，并对 Redis glob 字符做字面量转义。
	patterns := m.prefixDeletePatterns(target)
	// totalCount 汇总所有 pattern 实际扫描删除的 key 数，用于指标观察大前缀清理规模。
	totalCount, deleteErr := m.deletePrefixPatterns(ctx, patterns, guards)
	if deleteErr != nil {
		return errors.Tag(deleteErr)
	}
	m.recordPrefixDelete(ctx, target.Index, target.Key, totalCount)
	return nil
}

// deletePrefixPatterns 删除 DeleteByPrefix 需要覆盖的业务 key 与内部元信息 pattern。
// 并发度为 1 时保持串行，降低 Redis 扫描压力；并发度大于 1 时用于缩短大 keyspace 下多类元信息清理耗时。
func (m *Manager) deletePrefixPatterns(ctx context.Context, patterns []string, guards []LockGuard) (int64, error) {
	store, ok := m.store.(GuardedPatternStore)
	if !ok {
		return 0, errors.Wrapf(ErrPrefixDeleteUnsupported, "Store不支持带锁校验的前缀删除")
	}
	// concurrency 来自 Manager 配置，表示允许同时推进的 pattern 数；非法值会被收敛到默认保守值。
	concurrency := m.prefixDeleteConcurrencyValue()
	if concurrency <= 1 || len(patterns) <= 1 {
		// totalCount 是串行删除累计值，出现错误时连同已完成数量一起返回，便于调用方日志排障。
		var totalCount int64
		for _, pattern := range patterns {
			// 串行分支用于默认线上策略：每次只让一个 pattern 推进 SCAN，避免清理操作争抢 Redis 主线程。
			count, err := store.DeletePatternGuarded(ctx, pattern, m.scanCount, guards)
			if err != nil {
				return totalCount, errors.Wrapf(err, "删除前缀缓存pattern[%s]失败", pattern)
			}
			totalCount += count
		}
		return totalCount, nil
	}
	// totalCount 使用原子计数聚合多 goroutine 删除结果，避免并发 pattern 完成顺序不确定造成数据竞争。
	var totalCount atomic.Int64
	// groupCtx 会在任一 pattern 删除失败时取消其它扫描任务，避免故障场景继续施压 Redis。
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(min(concurrency, len(patterns)))
	for _, pattern := range patterns {
		// pattern 是当前 goroutine 独立持有的删除表达式，避免闭包捕获循环变量导致误删或漏删。
		pattern := pattern
		group.Go(func() error {
			// 并发分支只并发不同 pattern，每个 pattern 内部仍由 Store 决定如何安全推进 Redis SCAN。
			count, err := store.DeletePatternGuarded(groupCtx, pattern, m.scanCount, guards)
			if err != nil {
				return errors.Wrapf(err, "删除前缀缓存pattern[%s]失败", pattern)
			}
			totalCount.Add(count)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return totalCount.Load(), errors.Tag(err)
	}
	return totalCount.Load(), nil
}

// prefixDeleteConcurrencyValue 返回 DeleteByPrefix 的安全并发度，避免非法配置导致 errgroup SetLimit panic。
func (m *Manager) prefixDeleteConcurrencyValue() int {
	if m.prefixDeleteConcurrency <= 0 {
		return defaultPrefixDeleteConcurrency
	}
	return min(m.prefixDeleteConcurrency, maxWorkerConcurrency)
}

// deletePrefixByIndex 在索引可信时按前缀 key 索引删除，避免 Redis keyspace 特别大时执行全库 SCAN。
func (m *Manager) deletePrefixByIndex(ctx context.Context, target Target, guards []LockGuard) (int64, bool, error) {
	store, ok := m.prefixIndexStore() // store 是当前底层 Store 的索引能力，缺失时表示必须走 SCAN 降级路径
	if !ok || !m.isPrefixTarget(target) {
		return 0, false, nil
	}
	readyKey := m.prefixIndexReadyKey(target) // readyKey 表示该前缀索引已覆盖一次完整全量刷新，可作为 DeleteByPrefix 快路径依据
	indexKey := m.prefixIndexKey(target)      // indexKey 表示该前缀目标维护的真实 key 成员集合
	existsMap, err := m.store.ExistsMulti(ctx, readyKey, indexKey)
	if err != nil {
		return 0, false, errors.Tag(err)
	}
	if !existsMap[readyKey] {
		return 0, false, nil
	}
	if !existsMap[indexKey] {
		if err := m.applyStoreMutation(ctx, StoreMutation{Guards: guards, DeleteKeys: []string{readyKey}}); err != nil {
			return 0, false, errors.Tag(err)
		}
		return 0, false, nil
	}
	count, err := store.DeletePrefixIndexKeys(ctx, indexKey, m.scanCount, guards) // count 表示索引集合中被尝试删除的成员数量
	if err != nil {
		if errors.Is(err, ErrPrefixIndexUntrusted) {
			if deleteErr := m.applyStoreMutation(ctx, StoreMutation{Guards: guards, DeleteKeys: []string{readyKey}}); deleteErr != nil {
				return 0, false, errors.Tag(deleteErr)
			}
			return 0, false, nil
		}
		return count, true, errors.Wrapf(err, "按前缀索引删除缓存失败 index=%s", target.Index)
	}
	return count, true, nil
}

// deleteTargetKeys 删除某个目标关联的真实 Redis key；前缀索引保持安全超集并按TTL自然收敛。
func (m *Manager) deleteTargetKeys(ctx context.Context, target Target, guards []LockGuard, keys ...string) error {
	cleanKeys := cleanRedisKeys(keys) // cleanKeys 表示本次实际需要删除的业务或元信息 key
	if len(cleanKeys) == 0 {
		return nil
	}
	mutation := StoreMutation{
		Guards:     guards,
		DeleteKeys: cleanKeys,
	}
	return m.applyStoreMutation(ctx, mutation)
}

// applyStoreMutation 通过 Store 的故障安全批量契约提交变更。
func (m *Manager) applyStoreMutation(ctx context.Context, mutation StoreMutation) error {
	return errors.Tag(m.store.ApplyMutation(ctx, mutation))
}

// prefixIndexMutation 构造当前目标的索引成员变更；索引关闭、固定目标或空成员时返回 false。
func (m *Manager) prefixIndexMutation(target Target, keys []string, ttl time.Duration) (PrefixIndexMutation, bool) {
	if !m.prefixKeyIndex || !m.allowsPrefixMutation(target) {
		return PrefixIndexMutation{}, false
	}
	cleanKeys := cleanRedisKeys(keys) // cleanKeys 表示需要写入索引集合的真实 Redis key 成员
	if len(cleanKeys) == 0 {
		return PrefixIndexMutation{}, false
	}
	return PrefixIndexMutation{
		IndexKey: m.prefixIndexKey(target),
		TTL:      ttl,
		Keys:     cleanKeys,
	}, true
}

// markPrefixIndexReady 在前缀全量刷新成功后写入可信标记，后续 DeleteByPrefix 才能安全使用索引快路径。
func (m *Manager) markPrefixIndexReady(ctx context.Context, target Target, key string, guards []LockGuard) error {
	if !m.prefixKeyIndex || !m.isPrefixTarget(target) || key != target.Key {
		return nil
	}
	if _, ok := m.prefixIndexStore(); !ok {
		return nil
	}
	return m.applyStoreMutation(ctx, StoreMutation{
		Guards: guards,
		WriteEntries: []Entry{{
			Key:   m.prefixIndexReadyKey(target),
			Type:  TypeString,
			Value: time.Now().Format(time.RFC3339Nano),
			TTL:   m.prefixKeyIndexTTL,
		}},
	})
}

// prefixIndexStore 返回当前 Store 的前缀索引能力；未实现时仅可通过 GuardedPatternStore 回退到 SCAN。
func (m *Manager) prefixIndexStore() (PrefixIndexStore, bool) {
	if !m.prefixKeyIndex || m.store == nil {
		return nil, false
	}
	store, ok := m.store.(PrefixIndexStore) // store 是可选索引接口实现；ok=false 时使用 SCAN 删除
	return store, ok
}

// isPrefixTarget 判断目标是否为前缀型缓存目标；只有前缀目标才需要维护 key 索引。
func (m *Manager) isPrefixTarget(target Target) bool {
	return strings.HasSuffix(target.Key, ":")
}

// allowsPrefixMutation 判断当前目标是否存在合法的跨 key 前缀写删路径。
func (m *Manager) allowsPrefixMutation(target Target) bool {
	if !m.isPrefixTarget(target) {
		return false
	}
	if topology, ok := m.store.(PrefixMutationTopologyStore); ok {
		return topology.AllowsPrefixMutation(target.Key)
	}
	return true
}

// validateStrongPrefixOperation 在 Loader 或任何前缀写删前确认 Store 明确开放强前缀能力。
func (m *Manager) validateStrongPrefixOperation(ctx context.Context, target Target) error {
	if validator, ok := m.store.(PrefixMutationValidator); ok {
		if err := validator.ValidatePrefixMutation(ctx, target.Key); err != nil {
			return errors.Tag(err)
		}
	}
	if topology, ok := m.store.(PrefixMutationTopologyStore); ok && !topology.AllowsPrefixMutation(target.Key) {
		return errors.Wrapf(ErrRedisTopologyUnsupported, "Store不允许缓存前缀[%s]执行强一致全量操作", target.Key)
	}
	return nil
}

// prefixIndexKey 返回前缀目标的 key 索引集合名，集合成员为该目标管理的业务 key 与内部元信息 key。
func (m *Manager) prefixIndexKey(target Target) string {
	if hasRedisClusterHashTag(target.Key) {
		return tablecacheMetaKey("pidx", target.Key)
	}
	return metaKeyRoot + ":pidx:" + target.Key
}

// prefixIndexReadyKey 返回前缀索引可信标记 key；只有完成一次全量刷新后该标记才会存在。
func (m *Manager) prefixIndexReadyKey(target Target) string {
	if hasRedisClusterHashTag(target.Key) {
		return tablecacheMetaKey("pidx:ready", target.Key)
	}
	return metaKeyRoot + ":pidx:ready:" + target.Key
}

// getOrRefresh 统一封装读取、按需回源、回填和返回结果的完整链路。
func (m *Manager) getOrRefresh(ctx context.Context, key string, dest any, options LoadThroughOptions) (LookupResult, error) {
	target, params, err := m.resolveForRead(key, options.Fields)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	if err := m.validateLoadThroughOptions(target, options); err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	effectiveTarget := m.mergeLoadThroughTarget(target, options)
	lookupTarget := target
	lookupTarget.AllowEmptyMarker = target.AllowEmptyMarker || effectiveTarget.AllowEmptyMarker
	if err := m.validateFieldsRequest(lookupTarget, params.Key, params.Fields); err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	refreshOptions := refreshOptions{
		requested:     true,
		missTriggered: true,
		lookupTarget:  lookupTarget,
		contextPolicy: options.ContextPolicy,
	}
	isolatedFlight := hasLoadThroughOverride(options)
	if isolatedFlight {
		// 非空占位只负责让公共 result key 保持关闭；真正的随机 flight token 延迟到 miss 后生成。
		refreshOptions.flightToken = "isolated"
	}
	resultKey := m.refreshResultKey(params.Key, params.Fields, refreshOptions)
	var result LookupResult
	if resultKey != "" && !effectiveTarget.AllowEmptyMarker {
		result, refreshOptions.observedResult, refreshOptions.observedResultReady, err = m.lookupRefreshSnapshot(ctx, lookupTarget, params, dest, resultKey)
	} else {
		result, err = m.lookup(ctx, lookupTarget, params, dest)
	}
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	if result.State != LookupStateMiss {
		m.recordLookupState(ctx, lookupTarget.Index, result.State)
		return result, nil
	}
	if err := m.requireKeyPrefix(key); err != nil {
		m.recordLookupState(ctx, lookupTarget.Index, result.State)
		return result, nil
	}
	if isolatedFlight {
		refreshOptions.flightToken = newLockValue()
	}
	m.recordLookupRefreshTriggered(ctx, effectiveTarget.Index)
	if err := m.refreshWithSingleflight(ctx, effectiveTarget, params.Key, params.Fields, refreshOptions); err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	readbackCtx, cancelReadback := m.buildReadbackContext(ctx, options.ContextPolicy)
	defer cancelReadback()
	refreshedResult, err := m.lookup(readbackCtx, lookupTarget, params, dest)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	refreshedResult.Refreshed = true
	m.recordLookupState(ctx, lookupTarget.Index, refreshedResult.State)
	return refreshedResult, nil
}

// mergeLoadThroughTarget 合并单次读穿调用参数，避免直接修改已注册目标配置。
func (m *Manager) mergeLoadThroughTarget(target Target, options LoadThroughOptions) Target {
	if options.Loader != nil {
		target.Loader = options.Loader
	}
	if options.LoaderTimeout > 0 {
		target.LoaderTimeout = options.LoaderTimeout
	}
	if options.AllowEmptyMarker != nil {
		target.AllowEmptyMarker = *options.AllowEmptyMarker
	}
	return target
}

// mergeBatchLoadThroughOptions 合并批次级默认读穿参数和条目级参数，条目级配置优先。
func (m *Manager) mergeBatchLoadThroughOptions(defaultOptions LoadThroughOptions, itemOptions LoadThroughOptions) LoadThroughOptions {
	merged := defaultOptions
	if len(itemOptions.Fields) > 0 {
		merged.Fields = itemOptions.Fields
	}
	if itemOptions.Loader != nil {
		merged.Loader = itemOptions.Loader
	}
	if itemOptions.LoaderTimeout != 0 {
		merged.LoaderTimeout = itemOptions.LoaderTimeout
	}
	if itemOptions.AllowEmptyMarker != nil {
		merged.AllowEmptyMarker = itemOptions.AllowEmptyMarker
	}
	if itemOptions.ContextPolicy != nil {
		merged.ContextPolicy = itemOptions.ContextPolicy
	}
	return merged
}

// refreshWithSingleflight 合并同一进程内相同 key 的并发刷新请求。
func (m *Manager) refreshWithSingleflight(ctx context.Context, target Target, key string, fields []string, options refreshOptions) error {
	flightKey := "refresh:" + m.refreshRequestName(key, fields, options)
	waitCtx := ctx
	if m.effectiveRebuildContextPolicy(options.contextPolicy) == RebuildContextIgnoreCancel {
		waitCtx = context.WithoutCancel(ctx)
	}
	for {
		result := m.group.DoChan(flightKey, func() (any, error) {
			err := m.refreshTarget(waitCtx, target, key, fields, options)
			return waitCtx.Err(), err
		})
		select {
		case <-waitCtx.Done():
			return errors.Tag(waitCtx.Err())
		case response := <-result:
			if response.Err == nil {
				return nil
			}
			// leader 被自己的 ctx 取消时，仍存活的 waiter 重开一轮，避免首调用者污染同 key 请求。
			leaderContextErr, _ := response.Val.(error)
			if waitCtx.Err() == nil && leaderContextErr != nil && (errors.Is(response.Err, context.Canceled) || errors.Is(response.Err, context.DeadlineExceeded)) {
				continue
			}
			return errors.Tag(response.Err)
		}
	}
}

// refreshTarget 在锁保护下回源并写入缓存，避免热点缓存击穿。
func (m *Manager) refreshTarget(ctx context.Context, target Target, key string, fields []string, options refreshOptions) error {
	startedAt := time.Now()
	metricResult := "error"
	defer func() {
		m.recordRefresh(ctx, target.Index, metricResult, time.Since(startedAt))
	}()
	if target.Loader == nil {
		return errors.Tag(ErrLoaderRequired)
	}
	if err := m.validateFieldsRequest(target, key, fields); err != nil {
		return errors.Tag(err)
	}
	refreshCtx, cancelRefresh := m.buildRefreshContext(ctx, target, options.contextPolicy)
	defer cancelRefresh()
	if strings.HasSuffix(target.Key, ":") && key == target.Key {
		if _, ok := m.store.(PrefixReplaceStore); !ok {
			return errors.Wrapf(ErrPrefixReplaceUnsupported, "缓存目标[%s]的Store不支持安全前缀替换", target.Index)
		}
		if err := m.validateStrongPrefixOperation(refreshCtx, target); err != nil {
			return errors.Tag(err)
		}
	}
	resultKey := m.refreshResultKey(key, fields, options)
	for {
		if err := refreshCtx.Err(); err != nil {
			return errors.Tag(err)
		}
		lockName, lockKey, lockValue, observedOwner, locked, err := m.acquireRefreshLock(refreshCtx, target, key)
		if err != nil {
			return errors.Tag(err)
		}
		if !locked {
			m.recordLockFailed(ctx, target.Index)
			outcome, waitErr := m.waitRefreshRebuilt(refreshCtx, target, key, lockName, resultKey, observedOwner)
			if waitErr != nil {
				metricResult = "wait_error"
				return errors.Tag(waitErr)
			}
			if outcome == waitRebuildReady {
				metricResult = "wait_success"
				return nil
			}
			if err := waitWithContext(refreshCtx, m.waitDelay(target, 0)); err != nil {
				return errors.Tag(err)
			}
			continue
		}
		err = m.executeRefreshWithLock(ctx, refreshCtx, target, key, fields, options, LockGuard{Key: lockKey, Owner: lockValue}, resultKey)
		if errors.Is(err, errPrefixRefreshBusy) {
			m.recordPrefixRetry(ctx, target.Index)
			m.logInfoEvent("prefix_retry", target.Index, key, "lock_name", lockName)
			// 避免在前缀全量刷新期间 tight loop 自旋抢锁，对 Redis 与 CPU 造成无意义压力。
			if err := waitWithContext(refreshCtx, m.waitDelay(target, 0)); err != nil {
				return errors.Tag(err)
			}
			continue
		}
		if errors.Is(err, ErrRefreshLockLost) {
			metricResult = "lock_lost"
			m.logWarnEvent("lock_lost", target.Index, key, "lock_name", lockName, "err", err)
		}
		if errors.Is(err, ErrRefreshInvalidated) {
			metricResult = "invalidated"
			m.logWarnEvent("refresh_invalidated", target.Index, key, "lock_name", lockName, "err", err)
		}
		if err != nil {
			return errors.Tag(err)
		}
		metricResult = "success"
		return nil
	}
}

// buildRefreshContext 构建带完整操作预算的刷新上下文，IgnoreCancel 只忽略调用方取消，不移除内部截止时间。
func (m *Manager) buildRefreshContext(ctx context.Context, target Target, override *RebuildContextPolicy) (context.Context, context.CancelFunc) {
	baseCtx := ctx
	if m.effectiveRebuildContextPolicy(override) == RebuildContextIgnoreCancel {
		baseCtx = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(baseCtx, m.refreshOperationTimeout(target))
}

// acquireRefreshLock 为当前刷新请求申请分布式锁，并返回锁相关元信息。
func (m *Manager) acquireRefreshLock(ctx context.Context, target Target, key string) (string, string, string, string, bool, error) {
	lockName := m.refreshLockName(target, key)
	lockKey := m.lockKey(lockName)
	lockValue := newLockValue()
	locked, owner, err := m.store.AcquireRefreshLock(ctx, lockKey, lockValue, m.lockTTL)
	return lockName, lockKey, lockValue, owner, locked, errors.Tag(err)
}

// withExclusiveRefreshLock 在与刷新相同的锁域内执行显式删除，避免删除与旧刷新写回交错。
func (m *Manager) withExclusiveRefreshLock(ctx context.Context, target Target, key string, onWait func(), run func(lockCtx context.Context, guard LockGuard) error) error {
	_, waitTimes := m.waitPolicy(target)
	var lastErr error
	var lastLockName string
	waitRecorded := false
	for i := 0; i < waitTimes; i++ {
		lockName, lockKey, lockValue, _, locked, err := m.acquireRefreshLock(ctx, target, key)
		if err != nil {
			return errors.Tag(err)
		}
		if locked {
			lockCtx, cancelCause := context.WithCancelCause(ctx)
			defer cancelCause(nil)
			stopRenew := m.startLockRenew(lockCtx, lockKey, lockValue, cancelCause)
			defer m.releaseRefreshLock(ctx, lockKey, lockValue, stopRenew)
			if err := run(lockCtx, LockGuard{Key: lockKey, Owner: lockValue}); err != nil {
				return errors.Tag(err)
			}
			return nil
		}
		lastErr = ErrWaitRebuildTimeout
		lastLockName = lockName
		if !waitRecorded && onWait != nil {
			waitRecorded = true
			onWait()
		}
		if i+1 >= waitTimes {
			break
		}
		if err := waitWithContext(ctx, m.waitDelay(target, i)); err != nil {
			return errors.Tag(err)
		}
	}
	if lastErr != nil {
		m.recordWaitTimeout(ctx, target.Index)
		return errors.Wrapf(lastErr, "缓存锁[%s]等待互斥删除超时", lastLockName)
	}
	return nil
}

// withPrefixCommitLock 在前缀锁内提交单 key 变更，关闭“检查后前缀删除”的竞态窗口。
func (m *Manager) withPrefixCommitLock(ctx context.Context, target Target, recordWait bool, run func(commitCtx context.Context, guard LockGuard) error) error {
	if !m.allowsPrefixMutation(target) {
		return run(ctx, LockGuard{})
	}
	if !hasRedisClusterHashTag(target.Key) {
		if validator, ok := m.store.(PrefixMutationValidator); ok {
			if err := validator.ValidatePrefixMutation(ctx, target.Key); err != nil {
				if errors.Is(err, ErrDistributedPrefixHashTagRequired) {
					return run(ctx, LockGuard{})
				}
				return errors.Tag(err)
			}
		}
	}
	var onWait func()
	if recordWait {
		onWait = func() {
			m.recordPrefixWait(ctx, target.Index)
			m.logInfoEvent("prefix_wait", target.Index, target.Key, "prefix", target.Key)
		}
	}
	return m.withExclusiveRefreshLock(ctx, target, m.prefixCommitLockName(target), onWait, func(commitCtx context.Context, guard LockGuard) error {
		if !m.prefixKeyIndex {
			if err := m.applyStoreMutation(commitCtx, StoreMutation{Guards: []LockGuard{guard}, DeleteKeys: []string{m.prefixIndexReadyKey(target)}}); err != nil {
				return errors.Tag(err)
			}
		}
		return run(commitCtx, guard)
	})
}

// waitRefreshRebuilt 等待其它实例完成当前 key 的缓存重建。
func (m *Manager) waitRefreshRebuilt(ctx context.Context, target Target, key string, lockName string, resultKey string, observedOwner string) (waitRebuildOutcome, error) {
	return m.waitRebuilt(ctx, target, key, lockName, resultKey, observedOwner)
}

// executeRefreshWithLock 在已拿到分布式锁后执行完整刷新流程。
func (m *Manager) executeRefreshWithLock(ctx context.Context, refreshCtx context.Context, target Target, key string, fields []string, options refreshOptions, refreshGuard LockGuard, resultKey string) error {
	rebuildCtx, cancelCause := context.WithCancelCause(refreshCtx)
	defer cancelCause(nil)
	stopRenew := m.startLockRenew(rebuildCtx, refreshGuard.Key, refreshGuard.Owner, cancelCause)
	defer m.releaseRefreshLock(ctx, refreshGuard.Key, refreshGuard.Owner, stopRenew)
	if options.missTriggered {
		if resultKey != "" {
			currentResult, currentReady, err := m.readStringValue(rebuildCtx, resultKey)
			if err != nil {
				return errors.Tag(err)
			}
			if currentReady && (!options.observedResultReady || currentResult != options.observedResult) {
				m.markRefreshResult(rebuildCtx, target, resultKey, refreshGuard.Owner, []LockGuard{refreshGuard})
				return nil
			}
		}
		result, err := m.lookupWithoutMetrics(rebuildCtx, options.lookupTarget, m.loadParams(options.lookupTarget, key, fields))
		if err != nil {
			return errors.Tag(err)
		}
		if result.State != LookupStateMiss {
			if resultKey == "" {
				return nil
			}
			m.markRefreshResult(rebuildCtx, target, resultKey, refreshGuard.Owner, []LockGuard{refreshGuard})
			return nil
		}
	}
	if err := m.clearRefreshResult(rebuildCtx, target, resultKey, []LockGuard{refreshGuard}); err != nil {
		return errors.Tag(err)
	}
	epochs, err := m.prepareRefreshSnapshot(rebuildCtx, target, key, refreshGuard)
	if err != nil {
		return errors.Tag(err)
	}
	entries, wroteHiddenEmptyMarker, err := m.loadRefreshEntries(ctx, rebuildCtx, target, key, fields, options)
	if err != nil {
		return errors.Tag(err)
	}
	recordCommitWait := strings.HasSuffix(target.Key, ":") && key != target.Key
	return m.withPrefixCommitLock(rebuildCtx, target, recordCommitWait, func(commitCtx context.Context, commitGuard LockGuard) error {
		guards := commitLockGuards(refreshGuard, commitGuard)
		if err := m.validateRefreshEpochs(commitCtx, target, key, epochs); err != nil {
			return errors.Tag(err)
		}
		if err := m.ensureRefreshLockOwned(commitCtx, refreshGuard.Key, refreshGuard.Owner); err != nil {
			return errors.Tag(err)
		}
		if strings.HasSuffix(target.Key, ":") && key == target.Key {
			var err error
			epochs.prefixRefresh, err = m.advancePrefixEpoch(commitCtx, target, guards)
			if err != nil {
				return errors.Tag(err)
			}
			if err := m.replacePrefixEntries(commitCtx, target, entries, guards); err != nil {
				return errors.Tag(err)
			}
		} else {
			if wroteHiddenEmptyMarker {
				if err := m.writeRefreshEmptyMarker(commitCtx, target, key, fields, guards); err != nil {
					return errors.Tag(err)
				}
				m.recordEmptyMarkerWrite(ctx, target.Index)
			} else if len(fields) > 0 && len(entries) == 0 {
				store := m.store.(HashFieldsStore)
				if err := store.DeleteHashFields(commitCtx, guards, key, m.fieldsEmptyKey(key), fields); err != nil {
					return errors.Tag(err)
				}
			}
			if err := m.writeRefreshEntries(ctx, commitCtx, target, entries, fields, guards); err != nil {
				return errors.Tag(err)
			}
		}
		return m.finalizeRefreshState(ctx, commitCtx, target, key, entries, wroteHiddenEmptyMarker, fields, resultKey, refreshGuard.Owner, guards)
	})
}

// commitLockGuards 返回当前真实提交必须同时持有的锁 owner。
func commitLockGuards(refreshGuard LockGuard, commitGuard LockGuard) []LockGuard {
	guards := make([]LockGuard, 0, 2)
	if refreshGuard.Key != "" && refreshGuard.Owner != "" {
		guards = append(guards, refreshGuard)
	}
	if commitGuard.Key != "" && commitGuard.Owner != "" {
		guards = append(guards, commitGuard)
	}
	return guards
}

// ensureRefreshLockOwned 在关键写入前复核当前实例仍持有刷新锁，避免锁过期后旧 owner 继续写入脏缓存。
func (m *Manager) ensureRefreshLockOwned(ctx context.Context, lockKey string, lockValue string) error {
	ok, err := m.store.RefreshLock(ctx, lockKey, lockValue, m.lockTTL)
	if err != nil {
		return errors.Wrapf(errors.Join(ErrRefreshLockLost, err), "缓存锁[%s]owner复核失败", lockKey)
	}
	if !ok {
		return errors.Wrapf(ErrRefreshLockLost, "缓存锁[%s]owner复核失败: owner_mismatch", lockKey)
	}
	return nil
}

// releaseRefreshLock 停止锁续期并在后台安全释放当前实例持有的刷新锁。
func (m *Manager) releaseRefreshLock(ctx context.Context, lockKey string, lockValue string, stopRenew func()) {
	stopRenew()
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
	defer cancel()
	_, _ = m.store.ReleaseLock(releaseCtx, lockKey, lockValue)
}

// loadRefreshEntries 执行回源和本地校验；提交前不修改业务缓存。
func (m *Manager) loadRefreshEntries(ctx context.Context, rebuildCtx context.Context, target Target, key string, fields []string, options refreshOptions) ([]Entry, bool, error) {
	params := m.loadParams(target, key, fields)
	wroteHiddenEmptyMarker := false
	entries, err := m.loadEntries(rebuildCtx, target, params)
	if err != nil {
		if cause := context.Cause(rebuildCtx); cause != nil {
			return nil, false, cause
		}
		if !errors.Is(err, ErrNotFound) {
			m.recordLoaderError(ctx, target.Index, err)
			return nil, false, errors.Tag(err)
		}
		entries = nil
	}
	fullPrefix := strings.HasSuffix(target.Key, ":") && key == target.Key
	if len(entries) == 0 && target.AllowEmptyMarker && options.requested && !fullPrefix {
		wroteHiddenEmptyMarker = true
	}
	entries, err = m.normalizeRefreshEntries(target, key, fields, entries)
	if err != nil {
		return nil, false, errors.Tag(err)
	}
	return entries, wroteHiddenEmptyMarker, nil
}

// writeRefreshEntries 提交已完成本地校验的刷新结果。
func (m *Manager) writeRefreshEntries(ctx context.Context, rebuildCtx context.Context, target Target, entries []Entry, fields []string, guards []LockGuard) error {
	if len(entries) == 0 {
		return nil
	}
	normalEntries := make([]Entry, 0, len(entries))          // normalEntries 表示需要真实写入业务 key 的缓存条目
	emptyCollectionEntries := make([]Entry, 0, len(entries)) // emptyCollectionEntries 表示真实空集合条目，只写元信息避免 Redis 无法保存空集合
	for _, entry := range entries {
		if isEmptyCollectionEntry(entry) {
			emptyCollectionEntries = append(emptyCollectionEntries, entry)
		} else {
			normalEntries = append(normalEntries, entry)
		}
	}
	for _, entry := range emptyCollectionEntries {
		if err := m.writeEmptyCollectionMarker(rebuildCtx, target, entry, guards); err != nil {
			return errors.Tag(err)
		}
	}
	cleanupKeys := make([]string, 0, len(normalEntries)*2) // cleanupKeys 表示普通写入前需要清理的旧空值元信息
	for _, entry := range normalEntries {
		cleanupKeys = append(cleanupKeys, m.emptyKey(entry.Key), m.emptyCollectionKey(entry.Key))
		if target.Type == TypeHash {
			// 任一正向 Hash 写回都撤销该 key 的全部字段负缓存，避免重叠字段组合继续误报空值。
			cleanupKeys = append(cleanupKeys, m.fieldsEmptyKey(entry.Key))
		}
	}
	addIndexKeys := make([]string, 0, len(normalEntries)) // addIndexKeys 表示普通写入成功后需要纳入前缀索引的 key
	for _, entry := range normalEntries {
		addIndexKeys = append(addIndexKeys, entry.Key)
	}
	if len(normalEntries) > 0 {
		mutation := StoreMutation{
			Guards:       guards,
			DeleteKeys:   cleanupKeys,
			WriteEntries: normalEntries,
		}
		if indexMutation, ok := m.prefixIndexMutation(target, addIndexKeys, m.prefixKeyIndexTTL); ok {
			mutation.AddIndex = append(mutation.AddIndex, indexMutation)
		}
		if err := m.applyStoreMutation(rebuildCtx, mutation); err != nil {
			return errors.Tag(err)
		}
	}
	m.recordRefreshEntryCount(ctx, target.Index, len(entries))
	return nil
}

// finalizeRefreshState 处理空结果收尾、结果元信息写入和补充指标记录。
func (m *Manager) finalizeRefreshState(ctx context.Context, rebuildCtx context.Context, target Target, key string, entries []Entry, wroteHiddenEmptyMarker bool, fields []string, resultKey string, lockValue string, guards []LockGuard) error {
	fullPrefix := strings.HasSuffix(target.Key, ":") && key == target.Key
	if len(fields) == 0 && len(entries) == 0 && !wroteHiddenEmptyMarker && !fullPrefix {
		if err := m.deleteRefreshedKey(rebuildCtx, target, key, guards); err != nil {
			return errors.Tag(err)
		}
	}
	if err := m.markPrefixIndexReady(rebuildCtx, target, key, guards); err != nil {
		return errors.Tag(err)
	}
	// 成功但不落持久空值 marker 时也发布短完成标记，避免跨实例 waiter 依次重跑同一轮 Loader。
	m.markRefreshResult(rebuildCtx, target, resultKey, lockValue, guards)
	if len(entries) == 0 && !fullPrefix {
		m.recordRefreshEntryCount(ctx, target.Index, 0)
	}
	return nil
}

// clearRefreshResult 清除本次语义范围内的旧完成标记，避免 waiter 接受上一轮 owner。
func (m *Manager) clearRefreshResult(ctx context.Context, target Target, resultKey string, guards []LockGuard) error {
	if resultKey == "" {
		return nil
	}
	return m.deleteTargetKeys(ctx, target, guards, resultKey)
}

// markRefreshResult 封装一次短超时的重建结果元信息写入，并统一处理日志。
func (m *Manager) markRefreshResult(ctx context.Context, target Target, resultKey string, lockValue string, guards []LockGuard) {
	if resultKey == "" {
		return
	}
	resultCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
	defer cancel()
	if err := m.markRebuildResult(resultCtx, target, resultKey, lockValue, guards); err != nil {
		m.logWarnEvent("rebuild_result_write_failed", target.Index, resultKey, "err", err)
	}
}

// waitRebuilt 等待其它实例完成缓存重建。
func (m *Manager) waitRebuilt(ctx context.Context, target Target, key string, lockName string, resultKey string, observedOwner string) (waitRebuildOutcome, error) {
	_, waitTimes := m.waitPolicy(target)
	lockKey := m.lockKey(lockName)
	lastOwner := observedOwner
	for i := 0; i < waitTimes; i++ {
		lockValue, locked, err := m.readStringValue(ctx, lockKey)
		if err != nil {
			return 0, errors.Tag(err)
		}
		if locked {
			lastOwner = lockValue
			if resultKey != "" {
				resultValue, resultReady, err := m.readStringValue(ctx, resultKey)
				if err != nil {
					return 0, errors.Tag(err)
				}
				// 锁仍存在时必须校验 result marker 来源于当前锁 owner，避免上一轮短 TTL marker 被误判成本轮完成。
				if resultReady && resultValue == lockValue {
					return waitRebuildReady, nil
				}
			}
		} else {
			if resultKey != "" {
				resultValue, resultReady, err := m.readStringValue(ctx, resultKey)
				if err != nil {
					return 0, errors.Tag(err)
				}
				if lastOwner != "" && resultReady && resultValue == lastOwner {
					return waitRebuildReady, nil
				}
			}
			return waitRebuildRetry, nil
		}
		if i+1 >= waitTimes {
			break
		}
		if err := waitWithContext(ctx, m.waitDelay(target, i)); err != nil {
			return 0, errors.Tag(err)
		}
	}
	m.recordWaitTimeout(ctx, target.Index)
	return 0, errors.Wrapf(ErrWaitRebuildTimeout, "缓存key[%s]等待重建超时", key)
}

// readStringValue 读取 String 类型内部元信息，返回值、存在状态和错误，供锁等待逻辑做 owner 级判定。
func (m *Manager) readStringValue(ctx context.Context, key string) (string, bool, error) {
	value, err := m.store.Read(ctx, key, TypeString)
	if errors.Is(err, ErrCacheMiss) {
		return "", false, nil
	}
	if err != nil {
		return "", false, errors.Tag(err)
	}
	text, ok := value.(string)
	if !ok {
		return "", false, errors.Errorf("缓存key[%s]不是String类型元信息", key)
	}
	return text, true, nil
}

// resolveForRead 根据 Redis key 找到对应缓存目标和加载参数。
// 读取路径允许传逻辑 key；Manager 会映射到带前缀的实际 Redis key，只查询新命名空间数据。
func (m *Manager) resolveForRead(key string, fields []string) (Target, LoadParams, error) {
	key = m.withKeyPrefix(key)
	return m.resolvePhysical(key, fields)
}

// resolveForMutation 根据 Redis key 找到对应缓存目标和加载参数。
// 刷新、删除和写回属于有副作用操作，必须由调用方显式传入已带指定前缀的实际 Redis key。
func (m *Manager) resolveForMutation(key string, fields []string) (Target, LoadParams, error) {
	key = strings.TrimSpace(key)
	if err := m.requireKeyPrefix(key); err != nil {
		return Target{}, LoadParams{}, errors.Tag(err)
	}
	return m.resolvePhysical(key, fields)
}

// resolvePhysical 根据实际 Redis key 找到对应缓存目标和加载参数。
func (m *Manager) resolvePhysical(key string, fields []string) (Target, LoadParams, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Target{}, LoadParams{}, errors.Tag(ErrTargetNotFound)
	}
	if err := validateRedisClusterHashTagKey(key); err != nil {
		return Target{}, LoadParams{}, errors.Tag(err)
	}
	if target, ok := m.fixedKeys[key]; ok {
		params := m.loadParams(target, key, fields)
		return target, params, nil
	}
	for _, target := range m.prefixTargets {
		if strings.HasPrefix(key, target.Key) {
			params := m.loadParams(target, key, fields)
			return target, params, nil
		}
	}
	return Target{}, LoadParams{}, errors.Wrapf(ErrTargetNotFound, "缓存key[%s]未配置", key)
}

// loadParams 构建加载器参数。
func (m *Manager) loadParams(target Target, key string, fields []string) LoadParams {
	parts := make([]string, 0)
	if strings.HasSuffix(target.Key, ":") && strings.HasPrefix(key, target.Key) {
		remain := strings.TrimPrefix(key, target.Key)
		if remain != "" {
			parts = strings.Split(remain, ":")
		}
	}
	return LoadParams{
		Target:   target,
		Key:      key,
		Index:    target.Index,
		KeyParts: parts,
		Fields:   normalizeFields(fields),
	}
}

// withKeyPrefix 把读取路径传入的逻辑 key 转换成实际 Redis key。
// Target 构造期已拒绝预拼项目前缀的逻辑 key，因此此前缀判断不会与已注册目标产生歧义。
func (m *Manager) withKeyPrefix(key string) string {
	key = strings.TrimSpace(key)
	if key == "" || m.keyPrefix == "" || strings.HasPrefix(key, m.keyPrefix) {
		return key
	}
	return m.keyPrefix + key
}

// requireKeyPrefix 校验有副作用操作只能处理已带指定命名空间前缀的实际 Redis key。
func (m *Manager) requireKeyPrefix(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return errors.Tag(ErrTargetNotFound)
	}
	if m.keyPrefix == "" || strings.HasPrefix(key, m.keyPrefix) {
		return nil
	}
	return errors.Wrapf(ErrKeyPrefixRequired, "缓存key[%s]缺少指定前缀[%s]", key, m.keyPrefix)
}

// withKeyPrefixTarget 把缓存目标声明转换成实际 Redis key 范围。
// Target.Key 是后续匹配、删除和集群 hash tag 的数据来源，因此注册阶段统一加前缀，避免每条路径重复判断。
func (m *Manager) withKeyPrefixTarget(target Target) Target {
	target.Key = m.keyPrefix + strings.TrimSpace(target.Key)
	if target.KeyTitle != "" {
		target.KeyTitle = m.keyPrefix + strings.TrimSpace(target.KeyTitle)
	}
	return target
}

// decodeValue 把缓存原始值写入调用方传入的目标对象。
func (m *Manager) decodeValue(value any, dest any) error {
	if dest == nil {
		return nil
	}
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr || destValue.IsNil() {
		return errors.Errorf("缓存读取目标必须是非空指针")
	}
	if text, ok := value.(string); ok {
		if destText, ok := dest.(*string); ok {
			*destText = text
			return nil
		}
		return m.decoder([]byte(text), dest)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return errors.Tag(err)
	}
	return m.decoder(body, dest)
}

// lookup 统一执行一次缓存读取，供 Get、GetState 与 GetOrRefresh 复用。
func (m *Manager) lookup(ctx context.Context, target Target, params LoadParams, dest any) (LookupResult, error) {
	return m.lookupWithMetrics(ctx, target, params, dest, true)
}

// lookupWithoutMetrics 执行锁内二次检查，不重复累计调用方已记录的 hit/miss 指标。
func (m *Manager) lookupWithoutMetrics(ctx context.Context, target Target, params LoadParams) (LookupResult, error) {
	return m.lookupWithMetrics(ctx, target, params, nil, false)
}

// lookupRefreshSnapshot 优先用 RedisStore 的同槽快照把代际采样和业务读取合并为一次网络往返。
func (m *Manager) lookupRefreshSnapshot(ctx context.Context, target Target, params LoadParams, dest any, resultKey string) (LookupResult, string, bool, error) {
	// 只对精确 RedisStore 启用内部优化；嵌入并重写 Read 的装饰 Store 必须继续走其公开读取语义。
	if store, ok := m.store.(*RedisStore); ok {
		snapshot, handled, err := store.readRefreshSnapshot(ctx, params.Key, target.Type, params.Fields, resultKey)
		if err != nil {
			return LookupResult{}, "", false, errors.Tag(err)
		}
		if handled {
			readErr := error(nil)
			if !snapshot.ValueReady {
				readErr = ErrCacheMiss
			}
			result, err := m.lookupReadResult(ctx, target, params, dest, true, snapshot.Value, readErr)
			return result, snapshot.Result, snapshot.ResultReady, errors.Tag(err)
		}
	}
	observed, ready, err := m.readStringValue(ctx, resultKey)
	if err != nil {
		return LookupResult{}, "", false, errors.Tag(err)
	}
	result, err := m.lookup(ctx, target, params, dest)
	return result, observed, ready, errors.Tag(err)
}

// lookupWithMetrics 统一执行缓存读取，并按调用链决定是否记录底层命中指标。
func (m *Manager) lookupWithMetrics(ctx context.Context, target Target, params LoadParams, dest any, recordMetrics bool) (LookupResult, error) {
	value, err := m.readLookupValue(ctx, target, params)
	return m.lookupReadResult(ctx, target, params, dest, recordMetrics, value, err)
}

// lookupReadResult 统一解释业务读取结果，并在 miss 时检查各类空值元信息。
func (m *Manager) lookupReadResult(ctx context.Context, target Target, params LoadParams, dest any, recordMetrics bool, value any, err error) (LookupResult, error) {
	if err != nil {
		if errors.Is(err, ErrCacheMiss) {
			hidden, hiddenErr := m.hasHiddenEmptyMarker(ctx, target, params.Key)
			if hiddenErr != nil {
				return LookupResult{}, errors.Tag(hiddenErr)
			}
			if hidden {
				if recordMetrics {
					m.recordCacheMiss(ctx, target.Index)
				}
				return LookupResult{State: LookupStateEmpty}, nil
			}
			fieldsEmpty, fieldsEmptyErr := m.hasFieldsEmptyMarker(ctx, target, params.Key, params.Fields)
			if fieldsEmptyErr != nil {
				return LookupResult{}, errors.Tag(fieldsEmptyErr)
			}
			if fieldsEmpty {
				if recordMetrics {
					m.recordCacheMiss(ctx, target.Index)
				}
				return LookupResult{State: LookupStateEmpty}, nil
			}
			emptyCollection, emptyCollectionErr := m.hasEmptyCollectionMarker(ctx, target, params.Key)
			if emptyCollectionErr != nil {
				return LookupResult{}, errors.Tag(emptyCollectionErr)
			}
			if emptyCollection {
				if err := m.decodeValue(emptyCollectionValue(target.Type), dest); err != nil {
					return LookupResult{}, errors.Tag(err)
				}
				if recordMetrics {
					m.recordCacheHit(ctx, target.Index)
				}
				return LookupResult{State: LookupStateHit}, nil
			}
			if recordMetrics {
				m.recordCacheMiss(ctx, target.Index)
			}
			return LookupResult{State: LookupStateMiss}, nil
		}
		return LookupResult{}, errors.Tag(err)
	}
	if err := m.decodeValue(value, dest); err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	if recordMetrics {
		m.recordCacheHit(ctx, target.Index)
	}
	return LookupResult{State: LookupStateHit}, nil
}

// withTargetDefaults 为单条写入补齐目标默认值。
func (m *Manager) withTargetDefaults(target Target, entry Entry) Entry {
	if entry.Type == "" {
		entry.Type = target.Type
	}
	if entry.TTL == 0 {
		entry.TTL = target.TTL
	}
	if entry.Jitter == 0 {
		entry.Jitter = target.Jitter
	}
	return entry
}

// validateRefreshEntryScope 确保 Loader 只能写入当前注册目标管理范围内的 key。
func (m *Manager) validateRefreshEntryScope(target Target, entry Entry) error {
	entryKey := strings.TrimSpace(entry.Key)
	if entryKey == "" {
		return errors.Errorf("Redis缓存key不能为空")
	}
	if err := validateRedisClusterHashTagKey(entryKey); err != nil {
		return errors.Tag(err)
	}
	if strings.HasSuffix(target.Key, ":") {
		if strings.HasPrefix(entryKey, target.Key) {
			return nil
		}
		return errors.Wrapf(ErrEntryKeyOutOfScope, "缓存写入key[%s]不属于目标前缀[%s]", entryKey, target.Key)
	}
	if entryKey == target.Key {
		return nil
	}
	return errors.Wrapf(ErrEntryKeyOutOfScope, "缓存写入key[%s]不等于固定目标key[%s]", entryKey, target.Key)
}

// isEmptyCollectionEntry 判断 Entry 是否表示一个真实存在但内容为空的 Redis 集合结构。
func isEmptyCollectionEntry(entry Entry) bool {
	if !isCollectionType(entry.Type) {
		return false
	}
	if entry.Value == nil {
		return true
	}
	value := reflect.ValueOf(entry.Value)
	switch entry.Type {
	case TypeHash:
		return value.Kind() == reflect.Map && value.Len() == 0
	case TypeList, TypeSet:
		return (value.Kind() == reflect.Slice || value.Kind() == reflect.Array) && value.Len() == 0
	case TypeZSet:
		switch typed := entry.Value.(type) {
		case []ZMember:
			return len(typed) == 0
		case map[string]float64:
			return len(typed) == 0
		default:
			return false
		}
	default:
		return false
	}
}

// isCollectionType 判断 Redis 类型是否无法原生持久化空结构。
func isCollectionType(typ CacheType) bool {
	switch typ {
	case TypeHash, TypeList, TypeSet, TypeZSet:
		return true
	default:
		return false
	}
}

// emptyCollectionValue 返回空集合 marker 命中时用于反序列化的零元素值。
func emptyCollectionValue(typ CacheType) any {
	switch typ {
	case TypeHash:
		return map[string]string{}
	case TypeList, TypeSet:
		return []string{}
	case TypeZSet:
		return []ZMember{}
	default:
		return nil
	}
}

// emptyCollectionMarkerEntries 构造真实空集合元信息写入项，避免空集合被误判为缓存 miss。
func (m *Manager) emptyCollectionMarkerEntries(entries []Entry) []Entry {
	markerEntries := make([]Entry, 0, len(entries)) // markerEntries 表示需要写入 Redis 的空集合占位元信息条目
	for _, entry := range entries {
		markerEntries = append(markerEntries, Entry{
			Key:    m.emptyCollectionKey(entry.Key),
			Type:   TypeString,
			Value:  m.emptyMarker,
			TTL:    entry.TTL,
			Jitter: entry.Jitter,
		})
	}
	return markerEntries
}

// startLockRenew 在加载器运行期间定时续期当前实例持有的锁。
func (m *Manager) startLockRenew(ctx context.Context, key string, value string, onLockLost context.CancelCauseFunc) func() {
	if m.lockTTL <= 0 {
		return func() {}
	}
	interval := m.lockRenew
	if interval <= 0 {
		interval = m.lockTTL / 3
	}
	if interval < minLockRenewInterval {
		interval = minLockRenewInterval
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				ok, err := m.store.RefreshLock(ctx, key, value, m.lockTTL)
				if err != nil {
					m.logWarnEvent("lock_renew_failed", "", "", "lock_key", key, "err", err)
					onLockLost(errors.Wrapf(ErrRefreshLockLost, "缓存锁[%s]续期失败: %v", key, err))
					return
				}
				if !ok {
					m.logWarnEvent("lock_renew_failed", "", "", "lock_key", key, "owner_mismatch", true)
					onLockLost(errors.Wrapf(ErrRefreshLockLost, "缓存锁[%s]续期失败: owner_mismatch", key))
					return
				}
			}
		}
	}()
	return func() {
		close(done)
	}
}

// refreshLockName 返回当前刷新请求的锁域名称；前缀全量刷新使用前缀锁，前缀内单 key 刷新使用独立 key 锁。
func (m *Manager) refreshLockName(target Target, key string) string {
	if strings.HasSuffix(target.Key, ":") && key == target.Key {
		return target.Key
	}
	return key
}

// prefixCommitLockName 返回前缀变更的短提交锁域，Loader 不持有该锁。
func (m *Manager) prefixCommitLockName(target Target) string {
	return tablecacheMetaKey("rebuild:commit_scope", target.Key)
}

// prepareRefreshEpochs 为当前刷新记录启动时看到的代际快照。
func (m *Manager) prepareRefreshEpochs(ctx context.Context, target Target, key string, guards []LockGuard) (refreshEpochSnapshot, error) {
	if !m.allowsPrefixMutation(target) {
		return refreshEpochSnapshot{}, nil
	}
	prefixEpoch, err := m.preparePrefixRefreshEpoch(ctx, target, key, guards)
	if err != nil {
		return refreshEpochSnapshot{}, errors.Tag(err)
	}
	prefixDeleteEpoch, err := m.ensurePrefixDeleteEpoch(ctx, target, guards)
	if err != nil {
		return refreshEpochSnapshot{}, errors.Tag(err)
	}
	prefixMemberDeleteEpoch, err := m.ensurePrefixMemberDeleteEpoch(ctx, target, key, guards)
	if err != nil {
		return refreshEpochSnapshot{}, errors.Tag(err)
	}
	return refreshEpochSnapshot{
		prefixRefresh:      prefixEpoch,
		prefixDelete:       prefixDeleteEpoch,
		prefixMemberDelete: prefixMemberDeleteEpoch,
	}, nil
}

// preparePrefixRefreshEpoch 为前缀目标准备当前刷新代际。
// 前缀全量刷新会写入新的代际标记；单 key 刷新会确保代际非空，供后续写回前 fail-close 校验。
func (m *Manager) preparePrefixRefreshEpoch(ctx context.Context, target Target, key string, guards []LockGuard) (string, error) {
	if !strings.HasSuffix(target.Key, ":") {
		return "", nil
	}
	if key == target.Key {
		epoch := newLockValue()
		if err := m.writePrefixRefreshEpoch(ctx, target, epoch, guards); err != nil {
			return "", errors.Tag(err)
		}
		return epoch, nil
	}
	epoch, err := m.readPrefixRefreshEpoch(ctx, target)
	if err != nil || epoch != "" {
		return epoch, errors.Tag(err)
	}
	epoch = newLockValue()
	if err := m.writePrefixRefreshEpoch(ctx, target, epoch, guards); err != nil {
		return "", errors.Tag(err)
	}
	return epoch, nil
}

// validateRefreshEpochs 校验删除和全量刷新代际；持有前缀提交锁时使用该方法避免把自己的锁误判为冲突。
func (m *Manager) validateRefreshEpochs(ctx context.Context, target Target, key string, epochs refreshEpochSnapshot) error {
	if err := context.Cause(ctx); err != nil {
		return errors.Tag(err)
	}
	if !m.allowsPrefixMutation(target) {
		return nil
	}
	if epochs.prefixRefresh == "" || epochs.prefixDelete == "" || (key == target.Key && epochs.prefixMemberDelete == "") {
		return errors.Wrapf(ErrRefreshInvalidated, "缓存前缀[%s]刷新代际快照不完整", target.Key)
	}
	prefixDeleteEpoch, err := m.readPrefixDeleteEpoch(ctx, target)
	if err != nil {
		return errors.Tag(err)
	}
	if prefixDeleteEpoch != epochs.prefixDelete {
		return errors.Wrapf(ErrRefreshInvalidated, "缓存前缀[%s]刷新期间发生显式删除", target.Key)
	}
	prefixMemberDeleteEpoch, err := m.readPrefixMemberDeleteEpoch(ctx, target, key)
	if err != nil {
		return errors.Tag(err)
	}
	if prefixMemberDeleteEpoch != epochs.prefixMemberDelete {
		return errors.Wrapf(ErrRefreshInvalidated, "缓存前缀[%s]全量刷新期间发生成员删除", target.Key)
	}
	currentEpoch, err := m.readPrefixRefreshEpoch(ctx, target)
	if err != nil {
		return errors.Tag(err)
	}
	if currentEpoch != epochs.prefixRefresh {
		return errors.Tag(errPrefixRefreshBusy)
	}
	return nil
}

// prefixEpochKey 返回前缀全量刷新代际元信息 key。
func (m *Manager) prefixEpochKey(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "unknown"
	}
	return tablecacheMetaKey("rebuild:epoch", prefix)
}

// readPrefixRefreshEpoch 读取当前前缀全量刷新代际；未命中时返回空字符串。
func (m *Manager) readPrefixRefreshEpoch(ctx context.Context, target Target) (string, error) {
	if !strings.HasSuffix(target.Key, ":") {
		return "", nil
	}
	value, err := m.store.Read(ctx, m.prefixEpochKey(target.Key), TypeString)
	if err != nil {
		if errors.Is(err, ErrCacheMiss) {
			return "", nil
		}
		return "", errors.Tag(err)
	}
	text, ok := value.(string)
	if !ok {
		return "", errors.Errorf("缓存前缀[%s]代际值类型错误", target.Key)
	}
	return text, nil
}

// writePrefixRefreshEpoch 写入当前前缀全量刷新代际标记，供单 key 刷新识别是否已过期。
func (m *Manager) writePrefixRefreshEpoch(ctx context.Context, target Target, epoch string, guards []LockGuard) error {
	if !strings.HasSuffix(target.Key, ":") {
		return nil
	}
	ttl := m.prefixEpochTTL
	return m.applyStoreMutation(ctx, StoreMutation{Guards: guards, WriteEntries: []Entry{{
		Key: m.prefixEpochKey(target.Key), Type: TypeString, Value: epoch, TTL: ttl,
	}}})
}

// advancePrefixEpoch 推进前缀代际，使提交锁外加载的旧快照在后续提交时失效。
func (m *Manager) advancePrefixEpoch(ctx context.Context, target Target, guards []LockGuard) (string, error) {
	if !strings.HasSuffix(target.Key, ":") {
		return "", nil
	}
	epoch := newLockValue()
	if err := m.writePrefixRefreshEpoch(ctx, target, epoch, guards); err != nil {
		return "", errors.Tag(err)
	}
	return epoch, nil
}

// writePrefixDeleteEpoch 写入前缀显式删除代际，阻断同前缀已开始但尚未写回的旧刷新。
func (m *Manager) writePrefixDeleteEpoch(ctx context.Context, target Target, guards []LockGuard) error {
	if !strings.HasSuffix(target.Key, ":") {
		return nil
	}
	return m.writeDeleteEpoch(ctx, m.prefixDeleteEpochKey(target.Key), newLockValue(), guards)
}

// writePrefixMemberDeleteEpoch 记录前缀成员删除，阻断加载中的旧全量快照提交。
func (m *Manager) writePrefixMemberDeleteEpoch(ctx context.Context, target Target, guards []LockGuard) error {
	if !strings.HasSuffix(target.Key, ":") {
		return nil
	}
	return m.writeDeleteEpoch(ctx, m.prefixMemberDeleteEpochKey(target.Key), newLockValue(), guards)
}

// ensurePrefixDeleteEpoch 确保前缀删除代际非空；缺失可能来自首次运行或 Redis 淘汰。
func (m *Manager) ensurePrefixDeleteEpoch(ctx context.Context, target Target, guards []LockGuard) (string, error) {
	if !strings.HasSuffix(target.Key, ":") {
		return "", nil
	}
	return m.ensureDeleteEpoch(ctx, m.prefixDeleteEpochKey(target.Key), guards)
}

// ensurePrefixMemberDeleteEpoch 仅为全量刷新确保成员删除代际非空。
func (m *Manager) ensurePrefixMemberDeleteEpoch(ctx context.Context, target Target, key string, guards []LockGuard) (string, error) {
	if !strings.HasSuffix(target.Key, ":") || key != target.Key {
		return "", nil
	}
	return m.ensureDeleteEpoch(ctx, m.prefixMemberDeleteEpochKey(target.Key), guards)
}

// ensureDeleteEpoch 在提交锁保护下读取或建立删除代际。
func (m *Manager) ensureDeleteEpoch(ctx context.Context, key string, guards []LockGuard) (string, error) {
	epoch, err := m.readDeleteEpoch(ctx, key)
	if err != nil || epoch != "" {
		return epoch, errors.Tag(err)
	}
	epoch = newLockValue()
	if err := m.writeDeleteEpoch(ctx, key, epoch, guards); err != nil {
		return "", errors.Tag(err)
	}
	return epoch, nil
}

// writeDeleteEpoch 在当前锁栅栏内写入指定删除代际。
func (m *Manager) writeDeleteEpoch(ctx context.Context, key string, epoch string, guards []LockGuard) error {
	return m.applyStoreMutation(ctx, StoreMutation{Guards: guards, WriteEntries: []Entry{{
		Key: key, Type: TypeString, Value: epoch, TTL: m.prefixEpochTTL,
	}}})
}

// readDeleteEpoch 读取删除代际；不存在时返回空字符串。
func (m *Manager) readDeleteEpoch(ctx context.Context, key string) (string, error) {
	value, err := m.store.Read(ctx, key, TypeString)
	if err != nil {
		if errors.Is(err, ErrCacheMiss) {
			return "", nil
		}
		return "", errors.Tag(err)
	}
	text, ok := value.(string)
	if !ok {
		return "", errors.Errorf("缓存删除代际[%s]值类型错误", key)
	}
	return text, nil
}

// readPrefixDeleteEpoch 读取前缀显式删除代际；固定目标没有前缀删除代际。
func (m *Manager) readPrefixDeleteEpoch(ctx context.Context, target Target) (string, error) {
	if !strings.HasSuffix(target.Key, ":") {
		return "", nil
	}
	return m.readDeleteEpoch(ctx, m.prefixDeleteEpochKey(target.Key))
}

// readPrefixMemberDeleteEpoch 仅为全量刷新读取成员删除代际；单 key 刷新由同 key 锁和写入 guard 约束。
func (m *Manager) readPrefixMemberDeleteEpoch(ctx context.Context, target Target, key string) (string, error) {
	if !strings.HasSuffix(target.Key, ":") || key != target.Key {
		return "", nil
	}
	return m.readDeleteEpoch(ctx, m.prefixMemberDeleteEpochKey(target.Key))
}

// lockKey 返回缓存重建锁 Redis key。
func (m *Manager) lockKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	return tablecacheMetaKey("rebuild:lock", key)
}

// refreshResultKey 按 requested/scheduled 与 fields 隔离跨实例完成标记；单次覆盖请求不共享结果。
func (m *Manager) refreshResultKey(key string, fields []string, options refreshOptions) string {
	if options.flightToken != "" {
		return ""
	}
	scope := "scheduled"
	if options.missTriggered {
		scope = "load_through"
	} else if options.requested {
		scope = "requested"
	}
	if len(fields) > 0 {
		return tablecacheMetaKey("rebuild:fields_result", key) + ":" + fieldsID(fields) + ":" + scope
	}
	return tablecacheMetaKey("rebuild:result", key) + ":" + scope
}

// emptyKey 返回隐藏空值占位元信息 key。
func (m *Manager) emptyKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	return tablecacheMetaKey("empty", key)
}

// emptyCollectionKey 返回真实空集合元信息 key。
func (m *Manager) emptyCollectionKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	return tablecacheMetaKey("empty_collection", key)
}

// prefixDeleteEpochKey 返回前缀显式删除代际元信息 key。
func (m *Manager) prefixDeleteEpochKey(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "unknown"
	}
	return tablecacheMetaKey("delete:prefix_epoch", prefix)
}

// prefixMemberDeleteEpochKey 返回前缀成员删除代际元信息 key。
func (m *Manager) prefixMemberDeleteEpochKey(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "unknown"
	}
	return tablecacheMetaKey("delete:member_epoch", prefix)
}

// tablecacheMetaKey 生成内部元信息 key，并保持 Redis Cluster 同槽。
// 业务 key 自带合法 {...} tag 时沿用该 tag；否则把完整业务 key 作为 hash tag，避免重复拼接导致 key 过长。
func tablecacheMetaKey(kind string, key string) string {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "unknown"
	}
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	if hasRedisClusterHashTag(key) {
		return metaKeyRoot + ":" + kind + ":" + key
	}
	return metaKeyRoot + ":" + kind + ":{" + key + "}"
}

// tablecacheMetaKeyPattern 返回前缀清理元信息时使用的 pattern。
func tablecacheMetaKeyPattern(kind string, prefix string) string {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "unknown"
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "unknown"
	}
	if hasRedisClusterHashTag(prefix) {
		return metaKeyRoot + ":" + kind + ":" + prefix + "*"
	}
	// 普通 key 的元信息形如 tcm:{kind}:{prefix...}，pattern 保留左花括号即可匹配整段前缀。
	return metaKeyRoot + ":" + kind + ":{" + prefix + "*"
}

// redisClusterHashTag 返回 Redis Cluster 对 key 实际使用的 hash tag；无显式 tag 时使用完整 key。
func redisClusterHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return key
	}
	return key[start+1 : start+1+end]
}

// hasRedisClusterHashTag 判断 key 是否包含 Redis Cluster 会实际采用的非空 hash tag。
func hasRedisClusterHashTag(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	return redisClusterHashTag(key) != key
}

// validateRedisClusterHashTagKey 拒绝无法让 tablecache 元信息 key 与业务 key 保持同槽的异常花括号形式。
func validateRedisClusterHashTagKey(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	start := strings.IndexByte(key, '{')
	if start < 0 {
		if strings.Contains(key, "}") {
			return errors.Wrapf(ErrInvalidClusterHashTag, "缓存key[%s]包含未配对的右花括号", key)
		}
		return nil
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return errors.Wrapf(ErrInvalidClusterHashTag, "缓存key[%s]包含空或未闭合的Redis Cluster hash tag", key)
	}
	return nil
}

// newLockValue 生成锁持有者标识，用于释放锁时确认只删除自己的锁。
func newLockValue() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return fmt.Sprintf("%s:%d:%d", lockValueFallbackSeed, time.Now().UnixNano(), lockValueFallbackSeq.Add(1))
}

// fallbackLockValueSeed 生成不暴露主机信息的进程级 owner 隔离种子。
func fallbackLockValueSeed() string {
	hostname, _ := os.Hostname()
	value := fmt.Sprintf("%s:%d:%d", hostname, os.Getpid(), time.Now().UnixNano())
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:8])
}

// recordRefresh 记录刷新耗时和结果。
func (m *Manager) recordRefresh(ctx context.Context, index string, result string, duration time.Duration) {
	if m.metrics != nil {
		m.metrics.RecordRefresh(ctx, index, result, duration)
	}
}

// recordCacheHit 记录缓存命中。
func (m *Manager) recordCacheHit(ctx context.Context, index string) {
	if m.metrics != nil {
		m.metrics.RecordCacheHit(ctx, index)
	}
}

// recordCacheMiss 记录缓存未命中。
func (m *Manager) recordCacheMiss(ctx context.Context, index string) {
	if m.metrics != nil {
		m.metrics.RecordCacheMiss(ctx, index)
	}
}

// recordLockFailed 记录分布式锁竞争失败。
func (m *Manager) recordLockFailed(ctx context.Context, index string) {
	if m.metrics != nil {
		m.metrics.RecordLockFailed(ctx, index)
	}
}

// recordLoaderError 记录加载器回源失败。
func (m *Manager) recordLoaderError(ctx context.Context, index string, err error) {
	extended, ok := m.metrics.(ExtendedMetrics)
	if ok {
		extended.RecordLoaderError(ctx, index, err)
	}
}

// recordEmptyMarkerWrite 记录空值占位写入。
func (m *Manager) recordEmptyMarkerWrite(ctx context.Context, index string) {
	extended, ok := m.metrics.(ExtendedMetrics)
	if ok {
		extended.RecordEmptyMarkerWrite(ctx, index)
	}
}

// recordWaitTimeout 记录等待其它实例重建超时。
func (m *Manager) recordWaitTimeout(ctx context.Context, index string) {
	extended, ok := m.metrics.(ExtendedMetrics)
	if ok {
		extended.RecordWaitTimeout(ctx, index)
	}
}

// recordPrefixWait 记录单 key 刷新等待前缀短提交锁的次数。
func (m *Manager) recordPrefixWait(ctx context.Context, index string) {
	extended, ok := m.metrics.(ExtendedMetrics)
	if ok {
		extended.RecordPrefixWait(ctx, index)
	}
}

// recordPrefixRetry 记录单 key 因前缀全量刷新而重试的次数。
func (m *Manager) recordPrefixRetry(ctx context.Context, index string) {
	extended, ok := m.metrics.(ExtendedMetrics)
	if ok {
		extended.RecordPrefixRetry(ctx, index)
	}
}

// recordPrefixDelete 记录前缀删除数量。
func (m *Manager) recordPrefixDelete(ctx context.Context, index string, prefix string, count int64) {
	extended, ok := m.metrics.(ExtendedMetrics)
	if ok {
		extended.RecordPrefixDelete(ctx, index, prefix, count)
	}
}

// recordRefreshEntryCount 记录单次刷新写入条数。
func (m *Manager) recordRefreshEntryCount(ctx context.Context, index string, count int) {
	extended, ok := m.metrics.(ExtendedMetrics)
	if ok {
		extended.RecordRefreshEntryCount(ctx, index, count)
	}
}

// recordLookupState 记录缓存读取最终状态，便于区分命中、未命中与空值命中。
func (m *Manager) recordLookupState(ctx context.Context, index string, state LookupState) {
	lookupMetrics, ok := m.metrics.(LookupMetrics)
	if ok {
		lookupMetrics.RecordLookupState(ctx, index, state)
	}
}

// recordLookupRefreshTriggered 记录 GetOrRefresh/LoadThrough 在 miss 后触发回源刷新的次数。
func (m *Manager) recordLookupRefreshTriggered(ctx context.Context, index string) {
	lookupMetrics, ok := m.metrics.(LookupMetrics)
	if ok {
		lookupMetrics.RecordLookupRefreshTriggered(ctx, index)
	}
}

// recordRefreshBatch 记录批量刷新与全量刷新任务的汇总指标。
func (m *Manager) recordRefreshBatch(ctx context.Context, mode string, summary RefreshBatchSummary) {
	refreshBatchMetrics, ok := m.metrics.(RefreshBatchMetrics)
	if ok {
		refreshBatchMetrics.RecordRefreshBatch(ctx, mode, refreshBatchResultLabel(summary), summary.Total, summary.Success, summary.Failed)
	}
}

// recordScanFallback 记录前缀删除降级 SCAN。
func (m *Manager) recordScanFallback(ctx context.Context, index string, prefix string) {
	scanFallbackMetrics, ok := m.metrics.(ScanFallbackMetrics)
	if ok {
		scanFallbackMetrics.RecordScanFallback(ctx, index, prefix)
	}
}

// logInfoEvent 输出 gozero-admin 风格的信息级事件日志。
func (m *Manager) logInfoEvent(event string, index string, key string, fields ...any) {
	m.logEvent("info", event, index, key, fields...)
}

// logWarnEvent 输出 gozero-admin 风格的警告级事件日志。
func (m *Manager) logWarnEvent(event string, index string, key string, fields ...any) {
	m.logEvent("warn", event, index, key, fields...)
}

// logEvent 统一输出 tablecache 事件日志，便于在 gozero-admin 中按字段检索。
func (m *Manager) logEvent(level string, event string, index string, key string, fields ...any) {
	if m.logger == nil {
		return
	}
	parts := []string{
		"component=tablecache",
		formatLogField("event", event),
	}
	if index != "" {
		parts = append(parts, formatLogField("index", index))
	}
	if key != "" {
		parts = append(parts, formatLogField("key", m.logFieldValue("key", key)))
	}
	for i := 0; i+1 < len(fields); i += 2 {
		name, ok := fields[i].(string)
		if !ok || strings.TrimSpace(name) == "" {
			continue
		}
		parts = append(parts, formatLogField(name, m.logFieldValue(name, fields[i+1])))
	}
	message := strings.Join(parts, " ")
	switch level {
	case "warn":
		m.logger.Warnf("%s", message)
	default:
		m.logger.Infof("%s", message)
	}
}

// logFieldValue 返回日志字段值；启用脱敏后 key/prefix/lock_name/err 类字段只保留摘要。
func (m *Manager) logFieldValue(name string, value any) any {
	if !m.redactLogKeys {
		return value
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" {
		return value
	}
	if isLogErrorField(name) {
		return redactKeyForLog(text)
	}
	if !isLogKeyField(name) {
		return value
	}
	return redactKeyForLog(text)
}

// isLogKeyField 判断日志字段是否可能包含业务 Redis key。
func isLogKeyField(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	switch name {
	case "key", "prefix", "lock_name", "lock_key":
		return true
	default:
		return strings.HasSuffix(name, "_key") || strings.HasSuffix(name, "_prefix")
	}
}

// isLogErrorField 判断日志字段是否为错误文本；开启脱敏时错误文本可能携带业务 key，也需要摘要化。
func isLogErrorField(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	return name == "err" || name == "error"
}

// redactKeyForLog 把 Redis key 收敛为稳定摘要，便于关联排障同时避免泄漏业务参数。
func redactKeyForLog(key string) string {
	sum := sha1.Sum([]byte(key))
	return fmt.Sprintf("sha1:%s:len:%d", hex.EncodeToString(sum[:])[:12], len(key))
}

// formatLogField 把日志字段统一格式化为 key=value 风格，便于日志平台检索。
func formatLogField(name string, value any) string {
	switch data := value.(type) {
	case string:
		return fmt.Sprintf("%s=%q", name, data)
	case error:
		return fmt.Sprintf("%s=%q", name, data.Error())
	default:
		return fmt.Sprintf("%s=%v", name, value)
	}
}

// utilsLoggerAdapter 把 go-utils.Logger 适配为当前缓存管理器使用的 Logger。
type utilsLoggerAdapter struct {
	logger utils.Logger // logger 是 go-utils 日志实现
}

// Infof 输出信息级别日志。
func (l *utilsLoggerAdapter) Infof(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Info(fmt.Sprintf(format, args...))
}

// Warnf 输出警告级别日志。
func (l *utilsLoggerAdapter) Warnf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Warn(fmt.Sprintf(format, args...))
}

// refreshBatchResultLabel 返回当前批量刷新或全量刷新的结果标签。
func refreshBatchResultLabel(summary RefreshBatchSummary) string {
	if summary.Total == 0 {
		return "empty"
	}
	if summary.HasError() {
		return "partial_failed"
	}
	return "success"
}

// registerTarget 注册并校验缓存目标配置，避免重复与前缀重叠导致线上匹配歧义。
func (m *Manager) registerTarget(target Target) error {
	if _, ok := m.targets[target.Index]; ok {
		return errors.Errorf("tablecache目标index重复: %s", target.Index)
	}
	for _, item := range m.ordered {
		if item.Key == target.Key {
			return errors.Errorf("tablecache目标key重复: %s", target.Key)
		}
		if targetKeysOverlap(item.Key, target.Key) {
			return errors.Errorf("tablecache目标key存在重叠: %s <-> %s", item.Key, target.Key)
		}
	}
	m.targets[target.Index] = target
	m.ordered = append(m.ordered, target)
	if strings.HasSuffix(target.Key, ":") {
		m.prefixTargetsM[target.Key] = target
		insertAt := len(m.prefixTargets)
		for index, item := range m.prefixTargets {
			if len(target.Key) > len(item.Key) {
				insertAt = index
				break
			}
		}
		m.prefixTargets = append(m.prefixTargets, Target{})
		copy(m.prefixTargets[insertAt+1:], m.prefixTargets[insertAt:])
		m.prefixTargets[insertAt] = target
		return nil
	}
	m.fixedKeys[target.Key] = target
	return nil
}

// resolvePrefixTarget 校验并返回一个已注册的前缀型缓存目标，避免误删未托管的 Redis 前缀。
func (m *Manager) resolvePrefixTarget(prefix string) (Target, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return Target{}, errors.Tag(ErrTargetNotFound)
	}
	if target, ok := m.prefixTargetsM[prefix]; ok {
		return target, nil
	}
	return Target{}, errors.Wrapf(ErrTargetNotFound, "缓存前缀[%s]未注册", prefix)
}

// targetKeysOverlap 判断两个目标的实际 Redis key 范围是否重叠。
// 前缀目标会执行批量删除和全量重建，若固定 key 落在该前缀内，必须在启动期拒绝以避免误删其它目标。
func targetKeysOverlap(left string, right string) bool {
	leftPrefix := strings.HasSuffix(left, ":")
	rightPrefix := strings.HasSuffix(right, ":")
	switch {
	case leftPrefix && rightPrefix:
		// 两个前缀互相包含时会产生匹配歧义，例如 user: 与 user:profile:。
		return strings.HasPrefix(left, right) || strings.HasPrefix(right, left)
	case leftPrefix:
		// 左侧是前缀、右侧是固定 key 时，只要固定 key 落入前缀范围就存在批量删除风险。
		return strings.HasPrefix(right, left)
	case rightPrefix:
		// 右侧是前缀、左侧是固定 key 时，同样需要阻断固定 key 被前缀目标覆盖。
		return strings.HasPrefix(left, right)
	default:
		// 两个固定 key 不相等时互不重叠；相等场景已由重复 key 校验提前处理。
		return false
	}
}

// refreshConcurrency 返回安全的批量刷新并发度。
func (m *Manager) refreshConcurrency() int {
	concurrency := m.concurrency
	if concurrency <= 0 {
		concurrency = defaultRefreshConcurrency
	}
	if m.loaderConcurrency > 0 {
		concurrency = min(concurrency, m.loaderConcurrency)
	}
	return min(concurrency, maxWorkerConcurrency)
}

// resolveBatchConcurrency 返回当前批量读穿安全的并发度。
func (m *Manager) resolveBatchConcurrency(concurrency int) int {
	if concurrency > 0 {
		return min(concurrency, maxWorkerConcurrency)
	}
	return m.refreshConcurrency()
}

// waitPolicy 返回当前等待其它实例完成刷新的轮询策略。
func (m *Manager) waitPolicy(target Target) (time.Duration, int) {
	if m.waitConfigured && m.waitStep > 0 && m.waitTimes > 0 {
		return m.waitStep, m.waitTimes
	}
	total := m.defaultWaitTimeout(target)
	step := defaultWaitStepMin
	if total < step {
		step = total
	}
	// 只计算有限个指数阶段，剩余固定上限阶段用ceil公式收敛，避免超大Duration循环或溢出。
	times := 0
	waited := time.Duration(0)
	delay := step
	for delay < defaultWaitStepMax && waited < total {
		waited += delay
		times++
		delay *= 2
		if delay > defaultWaitStepMax {
			delay = defaultWaitStepMax
		}
	}
	if waited < total {
		remaining := total - waited
		steps := remaining / defaultWaitStepMax
		if remaining%defaultWaitStepMax != 0 {
			steps++
		}
		maxInt := int(^uint(0) >> 1)
		if steps > time.Duration(maxInt-times) {
			return step, maxInt
		}
		times += int(steps)
	}
	return step, times + 1
}

// waitDelay 返回第 N 次等待的实际休眠时长。
// 显式配置 WithWait 时保持固定步长；默认策略则使用逐步退避，降低热点场景下对 Redis 的轮询压力。
func (m *Manager) waitDelay(target Target, attempt int) time.Duration {
	step, _ := m.waitPolicy(target)
	if step <= 0 {
		step = defaultWaitStepMin
	}
	if m.waitConfigured {
		return step
	}
	delay := step
	for i := 0; i < attempt; i++ {
		if delay >= defaultWaitStepMax {
			return defaultWaitStepMax
		}
		delay *= 2
		if delay >= defaultWaitStepMax {
			return defaultWaitStepMax
		}
	}
	return delay
}

// defaultWaitTimeout 返回默认等待其它实例完成刷新的总时长。
func (m *Manager) defaultWaitTimeout(target Target) time.Duration {
	total := m.lockTTL
	loaderTimeout := target.LoaderTimeout
	if loaderTimeout <= 0 {
		loaderTimeout = m.loaderTTL
	}
	if loaderTimeout > total {
		total = loaderTimeout
	}
	if total <= 0 {
		total = time.Second
	}
	return total
}

// refreshOperationTimeout 返回一次完整刷新允许的总预算，阻断 owner 轮换或前缀代际持续变化造成的无限重试。
func (m *Manager) refreshOperationTimeout(target Target) time.Duration {
	base := m.defaultWaitTimeout(target)
	if m.waitConfigured && m.waitStep > 0 && m.waitTimes > 0 {
		configured := maxOperationTimeout
		if m.waitTimes <= int(maxOperationTimeout/m.waitStep) {
			configured = m.waitStep * time.Duration(m.waitTimes)
		}
		base = max(base, configured)
	}
	if base > maxOperationTimeout/refreshOperationBudgetMultiplier {
		return maxOperationTimeout
	}
	return base * refreshOperationBudgetMultiplier
}

// waitWithContext 在支持取消的前提下等待指定时长，避免在多处重复编写 Timer 模板代码。
func waitWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return errors.Tag(ctx.Err())
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.Tag(ctx.Err())
	case <-timer.C:
		return nil
	}
}

// effectiveRebuildContextPolicy 返回本次执行实际生效的上下文取消策略。
func (m *Manager) effectiveRebuildContextPolicy(override *RebuildContextPolicy) RebuildContextPolicy {
	if override != nil {
		return *override
	}
	return m.ctxPolicy
}

// buildReadbackContext 构建带内部截止时间的回读上下文，IgnoreCancel 不会形成永久挂起。
func (m *Manager) buildReadbackContext(ctx context.Context, override *RebuildContextPolicy) (context.Context, context.CancelFunc) {
	baseCtx := ctx
	if m.effectiveRebuildContextPolicy(override) == RebuildContextIgnoreCancel {
		baseCtx = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(baseCtx, maxReadbackTimeout)
}

// hasHiddenEmptyMarker 判断当前 key 是否存在隐藏空值占位。
func (m *Manager) hasHiddenEmptyMarker(ctx context.Context, target Target, key string) (bool, error) {
	if !target.AllowEmptyMarker {
		return false, nil
	}
	return m.store.Exists(ctx, m.emptyKey(key))
}

// hasEmptyCollectionMarker 判断当前 key 是否存在真实空集合元信息。
func (m *Manager) hasEmptyCollectionMarker(ctx context.Context, target Target, key string) (bool, error) {
	if !isCollectionType(target.Type) {
		return false, nil
	}
	return m.store.Exists(ctx, m.emptyCollectionKey(key))
}

// writeHiddenEmptyMarker 写入隐藏空值占位元信息，避免业务 key 与空占位发生值碰撞。
func (m *Manager) writeHiddenEmptyMarker(ctx context.Context, target Target, key string, guards []LockGuard) error {
	ttl := target.EmptyTTL
	if ttl <= 0 {
		ttl = m.emptyTTL
	}
	markerKey := m.emptyKey(key) // markerKey 表示隐藏空值占位元信息 key，需要纳入前缀索引以支持快速删除
	deleteKeys := []string{key, m.emptyCollectionKey(key), m.fieldsEmptyKey(key)}
	marker := Entry{
		Key:   markerKey,
		Type:  TypeString,
		Value: m.emptyMarker,
		TTL:   ttl,
	}
	return m.replaceWithMarker(ctx, target, marker, deleteKeys, guards)
}

// writeEmptyCollectionMarker 写入真实空集合元信息，并原子删除同槽旧业务值。
func (m *Manager) writeEmptyCollectionMarker(ctx context.Context, target Target, entry Entry, guards []LockGuard) error {
	markers := m.emptyCollectionMarkerEntries([]Entry{entry})
	if len(markers) != 1 {
		return errors.Errorf("空集合marker构造失败")
	}
	deleteKeys := []string{entry.Key, m.emptyKey(entry.Key), m.fieldsEmptyKey(entry.Key)}
	return m.replaceWithMarker(ctx, target, markers[0], deleteKeys, guards)
}

// replaceWithMarker 通过同槽原子操作切换 marker，并以故障安全顺序维护前缀索引。
func (m *Manager) replaceWithMarker(ctx context.Context, target Target, marker Entry, deleteKeys []string, guards []LockGuard) error {
	store, ok := m.store.(MarkerReplaceStore)
	if !ok {
		return errors.Wrapf(ErrMarkerReplaceUnsupported, "缓存目标[%s]的Store不支持marker安全提交", target.Index)
	}
	// 先登记 marker，后续提交失败只会留下可清理的冗余索引成员。
	if add, ok := m.prefixIndexMutation(target, []string{marker.Key}, m.prefixKeyIndexTTL); ok {
		if err := m.applyStoreMutation(ctx, StoreMutation{Guards: guards, AddIndex: []PrefixIndexMutation{add}}); err != nil {
			return errors.Tag(err)
		}
	}
	if err := store.ReplaceWithMarker(ctx, guards, marker, deleteKeys...); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// deleteRefreshedKey 删除当前刷新目标的业务 key 和隐藏空值元信息，避免空结果保留历史脏数据。
func (m *Manager) deleteRefreshedKey(ctx context.Context, target Target, key string, guards []LockGuard) error {
	keys := []string{m.emptyKey(key), m.emptyCollectionKey(key), m.fieldsEmptyKey(key)} // keys 表示当前刷新 key 关联的全部空值元信息
	if !(strings.HasSuffix(target.Key, ":") && key == target.Key) {
		keys = append(keys, key)
	}
	return m.deleteTargetKeys(ctx, target, guards, keys...)
}

// markRebuildResult 写入重建完成元信息，供等待其它实例回填时快速判断刷新已完成。
func (m *Manager) markRebuildResult(ctx context.Context, target Target, resultKey string, lockValue string, guards []LockGuard) error {
	if m.resultTTL <= 0 {
		return nil
	}
	lockValue = strings.TrimSpace(lockValue)
	if lockValue == "" {
		// lockValue 来自当前刷新持有的锁 owner；为空时降级为时间戳，避免写入空 marker 干扰等待方判断。
		lockValue = time.Now().Format(time.RFC3339Nano)
	}
	mutation := StoreMutation{
		Guards: guards,
		WriteEntries: []Entry{{
			Key:   resultKey,
			Type:  TypeString,
			Value: lockValue,
			TTL:   m.resultTTL,
		}},
	}
	if indexMutation, ok := m.prefixIndexMutation(target, []string{resultKey}, m.prefixKeyIndexTTL); ok {
		mutation.AddIndex = append(mutation.AddIndex, indexMutation)
	}
	return m.applyStoreMutation(ctx, mutation)
}

// validateManagerKeyPrefix 拒绝空前缀、保留根和容易误删的通配或空白前缀。
func validateManagerKeyPrefix(prefix string) error {
	if prefix == "" {
		return errors.Wrapf(ErrInvalidKeyPrefix, "tablecache不允许空keyPrefix")
	}
	if strings.ContainsAny(prefix, " \t\r\n*?") {
		return errors.Wrapf(ErrInvalidKeyPrefix, "tablecache keyPrefix[%s]包含空白或通配字符", prefix)
	}
	if !strings.HasSuffix(prefix, ":") {
		return errors.Wrapf(ErrInvalidKeyPrefix, "tablecache keyPrefix[%s]必须以冒号结尾", prefix)
	}
	reserved := metaKeyRoot + ":"
	if strings.HasPrefix(reserved, prefix) || strings.HasPrefix(prefix, reserved) {
		return errors.Wrapf(ErrInvalidKeyPrefix, "tablecache keyPrefix[%s]占用内部保留根[%s:]", prefix, metaKeyRoot)
	}
	if err := validateRedisClusterHashTagKey(prefix); err != nil {
		return errors.Tag(err)
	}
	return nil
}
