package tablecache

import (
	"context"
	"crypto/rand"
	"io"

	// sha1 仅用于对 fields 签名做长度收敛，避免生成超长 result key；不用于任何安全/加密场景。
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	// defaultLockTTL 表示缓存重建锁默认持有时间。
	defaultLockTTL = 10 * time.Second
	// defaultWaitStepMin 表示默认动态等待策略的最小轮询间隔。
	defaultWaitStepMin = 50 * time.Millisecond
	// defaultWaitStepMax 表示默认动态等待策略的最大轮询间隔。
	defaultWaitStepMax = 250 * time.Millisecond
	// defaultEmptyTTL 表示空值占位缓存默认过期时间。
	defaultEmptyTTL = 2 * time.Minute
	// defaultScanCount 表示清理前缀缓存时 Redis SCAN 的单次扫描数量。
	defaultScanCount = int64(100)
	// defaultRebuildResultTTL 表示重建完成元信息默认保留时间。
	defaultRebuildResultTTL = 30 * time.Second
	// defaultPrefixEpochTTL 表示前缀全量刷新代际元信息默认保留时间，避免长期运行产生无限元信息堆积。
	defaultPrefixEpochTTL = 7 * 24 * time.Hour
	// defaultRefreshConcurrency 表示批量刷新默认并发度，1 表示保守串行。
	defaultRefreshConcurrency = 1
)

const (
	// hashEmptyMarkerField 表示 Hash 类型 visible 空值占位使用的保留字段名，避免与业务真实字段发生碰撞。
	hashEmptyMarkerField = "__tablecache_empty__"
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
	// ErrEntryKeyOutOfScope 表示加载器返回了不属于当前缓存目标管理范围的写入 key。
	ErrEntryKeyOutOfScope = errors.New("tablecache entry key out of target scope")
	// errPrefixRefreshBusy 表示当前单 key 刷新遇到同前缀全量刷新，应等待后重试。
	errPrefixRefreshBusy = errors.New("tablecache prefix refresh busy")
)

// Option 表示 Manager 的可选配置。
type Option func(*Manager)

// Manager 管理一组表数据缓存目标，负责刷新、锁保护、TTL 抖动与空值占位。
type Manager struct {
	store          Store                // store 是底层缓存存储适配器
	targets        map[string]Target    // targets 按索引保存缓存目标
	ordered        []Target             // ordered 保留配置顺序，便于列表展示和批量刷新
	fixedKeys      map[string]Target    // fixedKeys 用于加速固定 key 的精确匹配
	prefixTargets  []Target             // prefixTargets 仅包含前缀型目标，并按 key 长度降序排列以确保最长前缀优先
	prefixTargetsM map[string]Target    // prefixTargetsM 用于 DeleteByPrefix 等场景快速校验前缀是否已注册
	lockTTL        time.Duration        // lockTTL 是缓存重建锁持有时间
	waitStep       time.Duration        // waitStep 是等待其它实例重建时的轮询间隔
	waitTimes      int                  // waitTimes 是等待其它实例重建的最大次数
	waitConfigured bool                 // waitConfigured 标识是否显式配置了等待策略
	emptyMarker    string               // emptyMarker 是空值缓存占位内容
	emptyTTL       time.Duration        // emptyTTL 是空值缓存默认过期时间
	scanCount      int64                // scanCount 是清理前缀缓存时的 SCAN count
	lockRenew      time.Duration        // lockRenew 是缓存重建锁续期间隔，0 表示按 lockTTL 自动计算
	resultTTL      time.Duration        // resultTTL 是重建完成元信息保留时间
	prefixEpochTTL time.Duration        // prefixEpochTTL 是前缀全量刷新代际元信息保留时间
	loaderTTL      time.Duration        // loaderTTL 是默认 Loader 超时时间，0 表示不额外设置超时
	ctxPolicy      RebuildContextPolicy // ctxPolicy 是缓存重建上下文取消策略
	concurrency    int                  // concurrency 是批量刷新并发度
	logger         Logger               // logger 是可选日志适配器
	metrics        Metrics              // metrics 是可选运行指标记录器
	decoder        Decoder              // decoder 是缓存读取反序列化函数
	group          singleflight.Group   // group 合并进程内相同 key 的并发刷新请求
}

// refreshOptions 表示一次刷新执行过程中的内部控制项。
type refreshOptions struct {
	requested     bool                  // requested 表示当前刷新是否由读取 miss 主动触发
	contextPolicy *RebuildContextPolicy // contextPolicy 表示本次刷新是否覆盖默认上下文取消策略
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
	if store == nil {
		return nil, errors.Errorf("tablecache store不能为空")
	}
	manager := &Manager{
		store:          store,
		targets:        make(map[string]Target, len(targets)),
		ordered:        make([]Target, 0, len(targets)),
		fixedKeys:      make(map[string]Target, len(targets)),
		prefixTargets:  make([]Target, 0, len(targets)),
		prefixTargetsM: make(map[string]Target, len(targets)),
		lockTTL:        defaultLockTTL,
		emptyMarker:    DefaultEmptyMarker,
		emptyTTL:       defaultEmptyTTL,
		scanCount:      defaultScanCount,
		resultTTL:      defaultRebuildResultTTL,
		prefixEpochTTL: defaultPrefixEpochTTL,
		concurrency:    defaultRefreshConcurrency,
		decoder:        json.Unmarshal,
	}
	for _, opt := range opts {
		opt(manager)
	}
	for _, target := range targets {
		normalized, err := normalizeTarget(target, manager.emptyTTL)
		if err != nil {
			return nil, errors.Tag(err)
		}
		if err := manager.registerTarget(normalized); err != nil {
			return nil, errors.Tag(err)
		}
	}
	return manager, nil
}

// WithLockTTL 设置缓存重建锁持有时间。
func WithLockTTL(ttl time.Duration) Option {
	return func(manager *Manager) {
		if ttl > 0 {
			manager.lockTTL = ttl
		}
	}
}

// WithLockRenewInterval 设置缓存重建锁续期间隔。
func WithLockRenewInterval(interval time.Duration) Option {
	return func(manager *Manager) {
		if interval > 0 {
			manager.lockRenew = interval
		}
	}
}

// WithRebuildResultTTL 设置重建完成元信息保留时间。
func WithRebuildResultTTL(ttl time.Duration) Option {
	return func(manager *Manager) {
		if ttl > 0 {
			manager.resultTTL = ttl
		}
	}
}

// WithPrefixEpochTTL 设置前缀全量刷新代际元信息保留时间。
func WithPrefixEpochTTL(ttl time.Duration) Option {
	return func(manager *Manager) {
		if ttl > 0 {
			manager.prefixEpochTTL = ttl
		}
	}
}

// WithLoaderTimeout 设置默认 Loader 超时时间。
func WithLoaderTimeout(timeout time.Duration) Option {
	return func(manager *Manager) {
		if timeout > 0 {
			manager.loaderTTL = timeout
		}
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
		if step > 0 {
			manager.waitStep = step
		}
		if times > 0 {
			manager.waitTimes = times
		}
		if step > 0 || times > 0 {
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
		if ttl > 0 {
			manager.emptyTTL = ttl
		}
	}
}

// WithScanCount 设置前缀缓存清理时的 Redis SCAN count。
func WithScanCount(count int64) Option {
	return func(manager *Manager) {
		if count > 0 {
			manager.scanCount = count
		}
	}
}

// WithRefreshConcurrency 设置 RefreshAll 和 RefreshByKeys 的有限并发度。
func WithRefreshConcurrency(concurrency int) Option {
	return func(manager *Manager) {
		if concurrency > 0 {
			manager.concurrency = concurrency
		}
	}
}

// WithLogger 设置缓存管理器日志适配器。
// 该方法同时兼容 tablecache 自定义 Logger 与 go-utils.Logger。
func WithLogger(logger any) Option {
	return func(manager *Manager) {
		manager.logger = normalizeLogger(logger)
	}
}

// WithGoUtilsLogger 设置 go-utils.Logger 日志适配器。
// 推荐在项目已统一使用 utils.Configure(utils.WithLogger(...)) 时直接传入 utils.Log()。
func WithGoUtilsLogger(logger utils.Logger) Option {
	return func(manager *Manager) {
		manager.logger = normalizeLogger(logger)
	}
}

// WithMetrics 设置缓存运行指标记录器。
func WithMetrics(metrics Metrics) Option {
	return func(manager *Manager) {
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
	key = strings.TrimSpace(key)
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
	group.SetLimit(m.refreshConcurrency())
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
	target, params, err := m.resolve(key, fields)
	if err != nil {
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
	group.SetLimit(m.refreshConcurrency())
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
		return results, summary, &RefreshBatchError{Summary: summary}
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
	target, params, err := m.resolve(key, nil)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	return m.lookup(ctx, target, params, dest)
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
	var group errgroup.Group
	group.SetLimit(m.resolveBatchConcurrency(options.Concurrency))
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
		return results, summary, &LoadThroughBatchError{Summary: summary}
	}
	return results, summary, nil
}

// DeleteByKey 删除指定 Redis key；前缀删除请使用 DeleteByPrefix。
func (m *Manager) DeleteByKey(ctx context.Context, key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	if _, _, err := m.resolve(key, nil); err != nil {
		return errors.Tag(err)
	}
	return m.store.Delete(ctx, key, m.emptyKey(key), m.emptyCollectionKey(key), m.rebuildResultKey(key))
}

// DeleteByPrefix 删除指定 Redis key 前缀下的所有缓存。
func (m *Manager) DeleteByPrefix(ctx context.Context, prefix string) error {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil
	}
	target, err := m.resolvePrefixTarget(prefix)
	if err != nil {
		return errors.Tag(err)
	}
	patterns := []string{
		target.Key + "*",
		m.emptyKey(target.Key) + "*",
		m.emptyCollectionKey(target.Key) + "*",
		m.rebuildResultKey(target.Key) + "*",
	}
	var totalCount int64
	for _, pattern := range patterns {
		count, deleteErr := m.store.DeletePattern(ctx, pattern, m.scanCount)
		if deleteErr != nil {
			return deleteErr
		}
		totalCount += count
	}
	m.recordPrefixDelete(ctx, target.Index, target.Key, totalCount)
	return nil
}

// getOrRefresh 统一封装读取、按需回源、回填和返回结果的完整链路。
func (m *Manager) getOrRefresh(ctx context.Context, key string, dest any, options LoadThroughOptions) (LookupResult, error) {
	target, params, err := m.resolve(key, options.Fields)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	result, err := m.lookup(ctx, target, params, dest)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	if result.State != LookupStateMiss {
		return result, nil
	}
	target = m.mergeLoadThroughTarget(target, options)
	m.recordLookupRefreshTriggered(ctx, target.Index)
	refreshOptions := refreshOptions{
		requested:     true,
		contextPolicy: options.ContextPolicy,
	}
	if err := m.refreshWithSingleflight(ctx, target, params.Key, params.Fields, refreshOptions); err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	refreshedResult, err := m.lookup(m.buildReadbackContext(ctx, options.ContextPolicy), target, params, dest)
	if err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	refreshedResult.Refreshed = true
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
	if itemOptions.LoaderTimeout > 0 {
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
	flightKey := "refresh:" + m.refreshReadyName(key, fields)
	_, err, _ := m.group.Do(flightKey, func() (any, error) {
		return nil, m.refreshTarget(ctx, target, key, fields, options)
	})
	return errors.Tag(err)
}

// refreshTarget 在锁保护下回源并写入缓存，避免热点缓存击穿。
func (m *Manager) refreshTarget(ctx context.Context, target Target, key string, fields []string, options refreshOptions) error {
	startedAt := time.Now()
	metricResult := "error"
	defer func() {
		m.recordRefresh(ctx, target.Index, metricResult, time.Since(startedAt))
	}()
	if target.Loader == nil {
		return ErrLoaderRequired
	}
	refreshCtx := m.buildRefreshContext(ctx, options.contextPolicy)
	fieldsReadyName := m.refreshFieldsReadyName(key, fields)
	for {
		if err := m.waitPrefixRefreshIdle(refreshCtx, target, key); err != nil {
			metricResult = "wait_error"
			return errors.Tag(err)
		}
		lockName, lockKey, lockValue, locked, err := m.acquireRefreshLock(refreshCtx, target, key)
		if err != nil {
			return errors.Tag(err)
		}
		if !locked {
			m.recordLockFailed(ctx, target.Index)
			outcome, waitErr := m.waitRefreshRebuilt(refreshCtx, target, key, lockName, fieldsReadyName)
			if waitErr != nil {
				metricResult = "wait_error"
				return waitErr
			}
			if outcome == waitRebuildReady {
				metricResult = "wait_success"
				return nil
			}
			continue
		}
		err = m.executeRefreshWithLock(ctx, refreshCtx, target, key, fields, options, lockKey, lockValue, fieldsReadyName)
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
		if err != nil {
			return errors.Tag(err)
		}
		metricResult = "success"
		return nil
	}
}

// buildRefreshContext 构建单次刷新过程使用的基础上下文。
func (m *Manager) buildRefreshContext(ctx context.Context, override *RebuildContextPolicy) context.Context {
	if m.effectiveRebuildContextPolicy(override) == RebuildContextIgnoreCancel {
		return context.WithoutCancel(ctx)
	}
	return ctx
}

// refreshFieldsReadyName 返回 fields 级等待判定名称；无 fields 时直接返回空字符串。
func (m *Manager) refreshFieldsReadyName(key string, fields []string) string {
	if len(fields) == 0 {
		return ""
	}
	// fieldsReadyName 用于 fields 级并发等待判定；基础锁域仍以 key 为粒度，避免不同 fields 同时写回造成覆盖。
	return m.refreshReadyName(key, fields)
}

// acquireRefreshLock 为当前刷新请求申请分布式锁，并返回锁相关元信息。
func (m *Manager) acquireRefreshLock(ctx context.Context, target Target, key string) (string, string, string, bool, error) {
	lockName := m.refreshLockName(target, key)
	lockKey := m.lockKey(lockName)
	lockValue := newLockValue()
	locked, err := m.store.SetNX(ctx, lockKey, lockValue, m.lockTTL)
	return lockName, lockKey, lockValue, locked, errors.Tag(err)
}

// waitRefreshRebuilt 等待其它实例完成当前 key 的缓存重建。
func (m *Manager) waitRefreshRebuilt(ctx context.Context, target Target, key string, lockName string, fieldsReadyName string) (waitRebuildOutcome, error) {
	waitName := key
	if fieldsReadyName != "" {
		waitName = fieldsReadyName
	}
	return m.waitRebuilt(ctx, target, key, lockName, waitName)
}

// executeRefreshWithLock 在已拿到分布式锁后执行完整刷新流程。
func (m *Manager) executeRefreshWithLock(ctx context.Context, refreshCtx context.Context, target Target, key string, fields []string, options refreshOptions, lockKey string, lockValue string, fieldsReadyName string) error {
	baseRebuildCtx, cancel := m.buildRebuildContext(refreshCtx, target, options.contextPolicy)
	defer cancel()
	rebuildCtx, cancelCause := context.WithCancelCause(baseRebuildCtx)
	defer cancelCause(nil)
	stopRenew := m.startLockRenew(rebuildCtx, lockKey, lockValue, cancelCause)
	defer m.releaseRefreshLock(ctx, lockKey, lockValue, stopRenew)
	prefixEpoch, entries, wroteHiddenEmptyMarker, err := m.loadRefreshEntries(ctx, rebuildCtx, target, key, fields, options)
	if err != nil {
		return errors.Tag(err)
	}
	if err := m.writeRefreshEntries(ctx, rebuildCtx, target, key, prefixEpoch, entries); err != nil {
		return errors.Tag(err)
	}
	if err := m.finalizeRefreshState(ctx, rebuildCtx, target, key, prefixEpoch, entries, wroteHiddenEmptyMarker, options, fieldsReadyName); err != nil {
		return errors.Tag(err)
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

// loadRefreshEntries 执行回源、空值占位决策与前缀刷新前置清理。
func (m *Manager) loadRefreshEntries(ctx context.Context, rebuildCtx context.Context, target Target, key string, fields []string, options refreshOptions) (string, []Entry, bool, error) {
	prefixEpoch, err := m.preparePrefixRefreshEpoch(rebuildCtx, target, key)
	if err != nil {
		return "", nil, false, errors.Tag(err)
	}
	params := m.loadParams(target, key, fields)
	wroteHiddenEmptyMarker := false
	entries, err := target.Loader(rebuildCtx, params)
	if err != nil {
		if cause := context.Cause(rebuildCtx); cause != nil {
			return "", nil, false, cause
		}
		if !errors.Is(err, ErrNotFound) {
			m.recordLoaderError(ctx, target.Index, err)
			return "", nil, false, errors.Tag(err)
		}
		entries = nil
	}
	if err := m.ensureRefreshWritable(rebuildCtx, target, key, prefixEpoch); err != nil {
		return "", nil, false, errors.Tag(err)
	}
	if len(entries) == 0 && target.AllowEmptyMarker && options.requested {
		if target.VisibleEmptyMark {
			entries = []Entry{m.emptyEntry(target, key)}
		} else {
			if err := m.writeHiddenEmptyMarker(rebuildCtx, target, key); err != nil {
				return "", nil, false, errors.Tag(err)
			}
			wroteHiddenEmptyMarker = true
			m.recordEmptyMarkerWrite(ctx, target.Index)
		}
	}
	if strings.HasSuffix(target.Key, ":") && key == target.Key {
		// 前缀目标做全量刷新时先清理旧 key，避免删除源数据中已不存在的脏缓存。
		if err := m.DeleteByPrefix(rebuildCtx, target.Key); err != nil {
			return "", nil, false, errors.Tag(err)
		}
	}
	if err := m.ensureRefreshWritable(rebuildCtx, target, key, prefixEpoch); err != nil {
		return "", nil, false, errors.Tag(err)
	}
	return prefixEpoch, entries, wroteHiddenEmptyMarker, nil
}

// writeRefreshEntries 为当前刷新结果补齐默认配置并执行批量写回。
func (m *Manager) writeRefreshEntries(ctx context.Context, rebuildCtx context.Context, target Target, key string, prefixEpoch string, entries []Entry) error {
	for index, entry := range entries {
		entries[index] = m.withTargetDefaults(target, entry)
		if err := m.validateRefreshEntryScope(target, entries[index]); err != nil {
			return errors.Tag(err)
		}
	}
	if len(entries) == 0 {
		return m.ensureRefreshWritable(rebuildCtx, target, key, prefixEpoch)
	}
	normalEntries := make([]Entry, 0, len(entries))
	emptyCollectionEntries := make([]Entry, 0)
	for _, entry := range entries {
		if isEmptyCollectionEntry(entry) {
			emptyCollectionEntries = append(emptyCollectionEntries, entry)
		} else {
			normalEntries = append(normalEntries, entry)
		}
	}
	cleanupKeys := make([]string, 0, len(entries)*2)
	for _, entry := range normalEntries {
		cleanupKeys = append(cleanupKeys, m.emptyKey(entry.Key), m.emptyCollectionKey(entry.Key))
	}
	for _, entry := range emptyCollectionEntries {
		cleanupKeys = append(cleanupKeys, entry.Key, m.emptyKey(entry.Key))
	}
	if len(cleanupKeys) > 0 {
		if err := m.store.Delete(rebuildCtx, cleanupKeys...); err != nil {
			return errors.Tag(err)
		}
	}
	if len(normalEntries) > 0 {
		if err := m.store.WriteBatch(rebuildCtx, normalEntries); err != nil {
			return errors.Tag(err)
		}
	}
	if len(emptyCollectionEntries) > 0 {
		if err := m.writeEmptyCollectionMarkers(rebuildCtx, emptyCollectionEntries); err != nil {
			return errors.Tag(err)
		}
	}
	m.recordRefreshEntryCount(ctx, target.Index, len(entries))
	return m.ensureRefreshWritable(rebuildCtx, target, key, prefixEpoch)
}

// finalizeRefreshState 处理空结果收尾、结果元信息写入和补充指标记录。
func (m *Manager) finalizeRefreshState(ctx context.Context, rebuildCtx context.Context, target Target, key string, prefixEpoch string, entries []Entry, wroteHiddenEmptyMarker bool, options refreshOptions, fieldsReadyName string) error {
	if len(entries) == 0 && wroteHiddenEmptyMarker {
		if err := m.deleteBusinessKey(rebuildCtx, target, key); err != nil {
			return errors.Tag(err)
		}
		if err := m.store.Delete(rebuildCtx, m.emptyCollectionKey(key)); err != nil {
			return errors.Tag(err)
		}
	}
	if len(entries) == 0 && !wroteHiddenEmptyMarker && (!target.AllowEmptyMarker || !options.requested || !target.VisibleEmptyMark) {
		if err := m.deleteRefreshedKey(rebuildCtx, target, key); err != nil {
			return errors.Tag(err)
		}
	}
	if err := m.ensureRefreshWritable(rebuildCtx, target, key, prefixEpoch); err != nil {
		return errors.Tag(err)
	}
	m.markRefreshResults(ctx, key, fieldsReadyName)
	if len(entries) == 1 && target.AllowEmptyMarker && options.requested && target.VisibleEmptyMark {
		m.recordEmptyMarkerWrite(ctx, target.Index)
	}
	if len(entries) == 0 {
		m.recordRefreshEntryCount(ctx, target.Index, 0)
	}
	return nil
}

// markRefreshResults 写入 key 级与 fields 级重建完成元信息，供等待方快速判定完成。
func (m *Manager) markRefreshResults(ctx context.Context, key string, fieldsReadyName string) {
	m.markRefreshResult(ctx, key)
	if fieldsReadyName != "" {
		// fields 场景额外写入 fields 专属 result marker，避免等待方因只看 key 存在而误判完成。
		m.markRefreshResult(ctx, fieldsReadyName)
	}
}

// markRefreshResult 封装一次短超时的重建结果元信息写入，并统一处理日志。
func (m *Manager) markRefreshResult(ctx context.Context, key string) {
	resultCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
	defer cancel()
	if err := m.markRebuildResult(resultCtx, key); err != nil && m.logger != nil {
		m.logger.Warnf("tablecache写入重建结果元信息失败 key=%s err=%v", key, err)
	}
}

// existsMulti 尝试合并多 key exists 检查，降低等待轮询阶段的网络往返；若 Store 未实现则回退为逐个 Exists。
func (m *Manager) existsMulti(ctx context.Context, keys ...string) (map[string]bool, error) {
	if store, ok := m.store.(ExistsMultiStore); ok {
		return store.ExistsMulti(ctx, keys...)
	}
	result := make(map[string]bool, len(keys))
	for _, key := range keys {
		exists, err := m.store.Exists(ctx, key)
		if err != nil {
			return nil, errors.Tag(err)
		}
		result[key] = exists
	}
	return result, nil
}

// waitRebuilt 等待其它实例完成缓存重建。
func (m *Manager) waitRebuilt(ctx context.Context, target Target, key string, lockName string, waitName string) (waitRebuildOutcome, error) {
	_, waitTimes := m.waitPolicy(target)
	emptyKey := m.emptyKey(key)
	resultKey := m.rebuildResultKey(waitName)
	baseResultKey := m.rebuildResultKey(key)
	lockKey := m.lockKey(lockName)
	fieldsRefresh := waitName != key
	for i := 0; i < waitTimes; i++ {
		if err := waitWithContext(ctx, m.waitDelay(target, i)); err != nil {
			return 0, errors.Tag(err)
		}
		existsKeys := []string{emptyKey, resultKey, lockKey}
		// fieldsRefresh 表示本次等待属于 fields 级刷新；此时不依赖业务 key 是否存在来判定完成，
		// 而是依赖 result marker 或 empty marker，避免“局部刷新”场景误判就绪。
		if !fieldsRefresh {
			existsKeys = append([]string{key}, existsKeys...)
		} else {
			existsKeys = append(existsKeys, baseResultKey)
		}
		existsMap, err := m.existsMulti(ctx, existsKeys...)
		if err != nil {
			return 0, errors.Tag(err)
		}
		// fieldsRefresh 场景除 fields 专属 result 以外，也接受 baseResultKey，
		// 用于兼容“已有其它实例完成过完整重建”的情形，避免不必要的重复回源。
		if (!fieldsRefresh && existsMap[key]) || existsMap[emptyKey] || existsMap[resultKey] || (fieldsRefresh && existsMap[baseResultKey]) {
			return waitRebuildReady, nil
		}
		if !existsMap[lockKey] {
			existsKeys := []string{emptyKey, resultKey}
			if !fieldsRefresh {
				existsKeys = append([]string{key}, existsKeys...)
			} else {
				existsKeys = append(existsKeys, baseResultKey)
			}
			existsMap, err := m.existsMulti(ctx, existsKeys...)
			if err != nil {
				return 0, errors.Tag(err)
			}
			if (!fieldsRefresh && existsMap[key]) || existsMap[emptyKey] || existsMap[resultKey] || (fieldsRefresh && existsMap[baseResultKey]) {
				return waitRebuildReady, nil
			}
			return waitRebuildRetry, nil
		}
	}
	m.recordWaitTimeout(ctx, target.Index)
	return 0, errors.Wrapf(ErrWaitRebuildTimeout, "缓存key[%s]等待重建超时", key)
}

// resolve 根据 Redis key 找到对应缓存目标和加载参数。
func (m *Manager) resolve(key string, fields []string) (Target, LoadParams, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Target{}, LoadParams{}, ErrTargetNotFound
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
		remain := strings.Trim(strings.TrimPrefix(key, target.Key), ":")
		if remain != "" {
			parts = strings.Split(remain, ":")
		}
	}
	cleanFields := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			cleanFields = append(cleanFields, field)
		}
	}
	if len(cleanFields) > 0 {
		// fields 做排序去重，确保同一组 fields 形成稳定签名，便于 singleflight 合并与等待判定。
		sort.Strings(cleanFields)
		write := 0
		for read := 0; read < len(cleanFields); read++ {
			if read == 0 || cleanFields[read] != cleanFields[read-1] {
				cleanFields[write] = cleanFields[read]
				write++
			}
		}
		cleanFields = cleanFields[:write]
	}
	return LoadParams{
		Target:   target,
		Key:      key,
		Index:    target.Index,
		KeyParts: parts,
		Fields:   cleanFields,
	}
}

// refreshReadyName 返回本次刷新用于“就绪判定”的标识名。
// 当 fields 为空时使用原始 key；否则把 fields 拼接到 key 上，并在过长时对 fields 做 hash 收敛，避免生成超长 Redis key。
func (m *Manager) refreshReadyName(key string, fields []string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	if len(fields) == 0 {
		return key
	}
	totalLen := len(fields) - 1
	for _, field := range fields {
		totalLen += len(field)
	}
	const prefix = "|fields="
	if totalLen <= 64 {
		var builder strings.Builder
		builder.Grow(len(key) + len(prefix) + totalLen)
		builder.WriteString(key)
		builder.WriteString(prefix)
		for index, field := range fields {
			if index > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(field)
		}
		return builder.String()
	}
	hasher := sha1.New()
	for index, field := range fields {
		if index > 0 {
			_, _ = io.WriteString(hasher, ",")
		}
		_, _ = io.WriteString(hasher, field)
	}
	sum := hasher.Sum(nil)
	var encoded [sha1.Size * 2]byte
	hex.Encode(encoded[:], sum)
	var builder strings.Builder
	builder.Grow(len(key) + len(prefix) + len(encoded))
	builder.WriteString(key)
	builder.WriteString(prefix)
	builder.Write(encoded[:])
	return builder.String()
}

// emptyEntry 构造空值占位缓存。
func (m *Manager) emptyEntry(target Target, key string) Entry {
	ttl := target.EmptyTTL
	if ttl <= 0 {
		ttl = m.emptyTTL
	}
	switch target.Type {
	case TypeHash:
		return Entry{Key: key, Type: TypeHash, Value: map[string]any{hashEmptyMarkerField: m.emptyMarker}, TTL: ttl}
	case TypeList:
		return Entry{Key: key, Type: TypeList, Value: []any{m.emptyMarker}, TTL: ttl}
	case TypeSet:
		return Entry{Key: key, Type: TypeSet, Value: []any{m.emptyMarker}, TTL: ttl}
	case TypeZSet:
		return Entry{Key: key, Type: TypeZSet, Value: []ZMember{{Member: m.emptyMarker, Score: 0}}, TTL: ttl}
	default:
		return Entry{Key: key, Type: TypeString, Value: m.emptyMarker, TTL: ttl}
	}
}

// isEmptyValue 判断读取值是否为空值占位。
func (m *Manager) isEmptyValue(typ CacheType, value any) bool {
	switch typ {
	case TypeHash:
		if data, ok := value.(map[string]string); ok {
			return data[hashEmptyMarkerField] == m.emptyMarker || data["value"] == m.emptyMarker
		}
	case TypeList, TypeSet:
		if data, ok := value.([]string); ok {
			return len(data) == 1 && data[0] == m.emptyMarker
		}
	case TypeZSet:
		if data, ok := value.([]ZMember); ok {
			return len(data) == 1 && fmt.Sprint(data[0].Member) == m.emptyMarker
		}
	default:
		if data, ok := value.(string); ok {
			return data == m.emptyMarker
		}
	}
	return false
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
	value, err := m.store.Read(ctx, params.Key, target.Type)
	if err != nil {
		if errors.Is(err, ErrCacheMiss) {
			hidden, hiddenErr := m.hasHiddenEmptyMarker(ctx, target, params.Key)
			if hiddenErr != nil {
				return LookupResult{}, hiddenErr
			}
			if hidden {
				m.recordCacheMiss(ctx, target.Index)
				m.recordLookupState(ctx, target.Index, LookupStateEmpty)
				return LookupResult{State: LookupStateEmpty}, nil
			}
			emptyCollection, emptyCollectionErr := m.hasEmptyCollectionMarker(ctx, target, params.Key)
			if emptyCollectionErr != nil {
				return LookupResult{}, emptyCollectionErr
			}
			if emptyCollection {
				if err := m.decodeValue(emptyCollectionValue(target.Type), dest); err != nil {
					return LookupResult{}, errors.Tag(err)
				}
				m.recordCacheHit(ctx, target.Index)
				m.recordLookupState(ctx, target.Index, LookupStateHit)
				return LookupResult{State: LookupStateHit}, nil
			}
			m.recordCacheMiss(ctx, target.Index)
			m.recordLookupState(ctx, target.Index, LookupStateMiss)
			return LookupResult{State: LookupStateMiss}, nil
		}
		return LookupResult{}, errors.Tag(err)
	}
	if m.isEmptyValue(target.Type, value) {
		m.recordCacheMiss(ctx, target.Index)
		m.recordLookupState(ctx, target.Index, LookupStateEmpty)
		return LookupResult{State: LookupStateEmpty}, nil
	}
	if err := m.decodeValue(value, dest); err != nil {
		return LookupResult{}, errors.Tag(err)
	}
	m.recordCacheHit(ctx, target.Index)
	m.recordLookupState(ctx, target.Index, LookupStateHit)
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
	switch value.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		return value.Len() == 0
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

// writeEmptyCollectionMarkers 写入真实空集合元信息，避免空集合被误判为缓存 miss。
func (m *Manager) writeEmptyCollectionMarkers(ctx context.Context, entries []Entry) error {
	markerEntries := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		markerEntries = append(markerEntries, Entry{
			Key:    m.emptyCollectionKey(entry.Key),
			Type:   TypeString,
			Value:  m.emptyMarker,
			TTL:    entry.TTL,
			Jitter: entry.Jitter,
		})
	}
	return m.store.WriteBatch(ctx, markerEntries)
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
	if interval <= 0 {
		interval = time.Second
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

// preparePrefixRefreshEpoch 为前缀目标准备当前刷新代际。
// 前缀全量刷新会写入新的代际标记；单 key 刷新会记录启动时看到的代际，供后续写回前校验。
func (m *Manager) preparePrefixRefreshEpoch(ctx context.Context, target Target, key string) (string, error) {
	if !strings.HasSuffix(target.Key, ":") {
		return "", nil
	}
	if key == target.Key {
		epoch := newLockValue()
		if err := m.writePrefixRefreshEpoch(ctx, target, epoch); err != nil {
			return "", errors.Tag(err)
		}
		return epoch, nil
	}
	return m.readPrefixRefreshEpoch(ctx, target)
}

// ensureRefreshWritable 校验当前刷新上下文是否仍可继续写回。
// 若检测到同前缀全量刷新仍在执行，或前缀代际已变化，则返回重试信号，避免旧单 key 结果覆盖新全量结果。
func (m *Manager) ensureRefreshWritable(ctx context.Context, target Target, key string, prefixEpoch string) error {
	if err := context.Cause(ctx); err != nil {
		return errors.Tag(err)
	}
	prefixLockKey, ok := m.prefixRefreshLockKey(target, key)
	if !ok {
		return nil
	}
	exists, err := m.store.Exists(ctx, prefixLockKey)
	if err != nil {
		return errors.Tag(err)
	}
	if exists {
		return errPrefixRefreshBusy
	}
	currentEpoch, err := m.readPrefixRefreshEpoch(ctx, target)
	if err != nil {
		return errors.Tag(err)
	}
	if currentEpoch != prefixEpoch {
		return errPrefixRefreshBusy
	}
	return nil
}

// waitPrefixRefreshIdle 在前缀全量刷新期间阻塞当前单 key 刷新，避免写回与全量删除/回填交错。
func (m *Manager) waitPrefixRefreshIdle(ctx context.Context, target Target, key string) error {
	prefixLockKey, ok := m.prefixRefreshLockKey(target, key)
	if !ok {
		return nil
	}
	_, waitTimes := m.waitPolicy(target)
	waited := false
	for i := 0; i < waitTimes; i++ {
		exists, err := m.store.Exists(ctx, prefixLockKey)
		if err != nil {
			return errors.Tag(err)
		}
		if !exists {
			return nil
		}
		if !waited {
			waited = true
			m.recordPrefixWait(ctx, target.Index)
			m.logInfoEvent("prefix_wait", target.Index, key, "prefix", target.Key)
		}
		if err := waitWithContext(ctx, m.waitDelay(target, i)); err != nil {
			return errors.Tag(err)
		}
	}
	m.recordWaitTimeout(ctx, target.Index)
	return errors.Wrapf(ErrWaitRebuildTimeout, "缓存前缀[%s]等待全量刷新超时", target.Key)
}

// prefixRefreshLockKey 返回当前单 key 刷新需要避让的前缀全量刷新锁 key。
func (m *Manager) prefixRefreshLockKey(target Target, key string) (string, bool) {
	if !strings.HasSuffix(target.Key, ":") || key == target.Key {
		return "", false
	}
	return m.lockKey(target.Key), true
}

// prefixEpochKey 返回前缀全量刷新代际元信息 key。
func (m *Manager) prefixEpochKey(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "unknown"
	}
	return "tablecache:rebuild:epoch:" + prefix
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
func (m *Manager) writePrefixRefreshEpoch(ctx context.Context, target Target, epoch string) error {
	if !strings.HasSuffix(target.Key, ":") {
		return nil
	}
	ttl := m.prefixEpochTTL
	return m.store.Write(ctx, Entry{
		Key:   m.prefixEpochKey(target.Key),
		Type:  TypeString,
		Value: epoch,
		TTL:   ttl,
	})
}

// lockKey 返回缓存重建锁 Redis key。
func (m *Manager) lockKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	return "tablecache:rebuild:lock:" + key
}

// rebuildResultKey 返回缓存重建完成元信息 key。
func (m *Manager) rebuildResultKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	return "tablecache:rebuild:result:" + key
}

// emptyKey 返回隐藏空值占位元信息 key。
func (m *Manager) emptyKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	return "tablecache:empty:" + key
}

// emptyCollectionKey 返回真实空集合元信息 key。
func (m *Manager) emptyCollectionKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	return "tablecache:empty_collection:" + key
}

// newLockValue 生成锁持有者标识，用于释放锁时确认只删除自己的锁。
func newLockValue() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return time.Now().Format(time.RFC3339Nano)
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

// recordPrefixWait 记录前缀全量刷新阻塞单 key 的次数。
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
		parts = append(parts, formatLogField("key", key))
	}
	for i := 0; i+1 < len(fields); i += 2 {
		name, ok := fields[i].(string)
		if !ok || strings.TrimSpace(name) == "" {
			continue
		}
		parts = append(parts, formatLogField(name, fields[i+1]))
	}
	message := strings.Join(parts, " ")
	switch level {
	case "warn":
		m.logger.Warnf("%s", message)
	default:
		m.logger.Infof("%s", message)
	}
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

// normalizeLogger 把外部传入的日志实现统一转换为内部 Logger 接口。
func normalizeLogger(logger any) Logger {
	switch data := logger.(type) {
	case nil:
		return nil
	case Logger:
		return data
	case utils.Logger:
		return &utilsLoggerAdapter{logger: data}
	default:
		return nil
	}
}

// utilsLoggerAdapter 把 go-utils.Logger 适配为当前缓存管理器使用的 Logger。
type utilsLoggerAdapter struct {
	logger utils.Logger // logger 是 go-utils 日志实现
}

// Debugf 输出调试级别日志。
func (l *utilsLoggerAdapter) Debugf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Debug(fmt.Sprintf(format, args...))
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

// Errorf 输出错误级别日志。
func (l *utilsLoggerAdapter) Errorf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Error(fmt.Sprintf(format, args...))
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
		if strings.HasSuffix(item.Key, ":") && strings.HasSuffix(target.Key, ":") {
			if strings.HasPrefix(item.Key, target.Key) || strings.HasPrefix(target.Key, item.Key) {
				return errors.Errorf("tablecache前缀key存在重叠: %s <-> %s", item.Key, target.Key)
			}
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
	prefix = strings.TrimSpace(strings.TrimRight(prefix, "*"))
	if prefix == "" {
		return Target{}, ErrTargetNotFound
	}
	if target, ok := m.prefixTargetsM[prefix]; ok {
		return target, nil
	}
	return Target{}, errors.Wrapf(ErrTargetNotFound, "缓存前缀[%s]未注册", prefix)
}

// refreshConcurrency 返回安全的批量刷新并发度。
func (m *Manager) refreshConcurrency() int {
	if m.concurrency <= 0 {
		return defaultRefreshConcurrency
	}
	return m.concurrency
}

// resolveBatchConcurrency 返回当前批量读穿安全的并发度。
func (m *Manager) resolveBatchConcurrency(concurrency int) int {
	if concurrency > 0 {
		return concurrency
	}
	return m.refreshConcurrency()
}

// waitPolicy 返回当前等待其它实例完成刷新的轮询策略。
func (m *Manager) waitPolicy(target Target) (time.Duration, int) {
	if m.waitConfigured && m.waitStep > 0 && m.waitTimes > 0 {
		return m.waitStep, m.waitTimes
	}
	total := m.defaultWaitTimeout(target)
	step := total / 20
	if step < defaultWaitStepMin {
		step = defaultWaitStepMin
	}
	if step > defaultWaitStepMax {
		step = defaultWaitStepMax
	}
	if step > total {
		step = total
	}
	times := int(total / step)
	if total%step != 0 {
		times++
	}
	if times <= 0 {
		times = 1
	}
	return step, times
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

// waitWithContext 在支持取消的前提下等待指定时长，避免在多处重复编写 Timer 模板代码。
func waitWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
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

// buildReadbackContext 构建刷新完成后的回读上下文，确保忽略取消策略能覆盖完整读穿链路。
func (m *Manager) buildReadbackContext(ctx context.Context, override *RebuildContextPolicy) context.Context {
	if m.effectiveRebuildContextPolicy(override) == RebuildContextIgnoreCancel {
		return context.WithoutCancel(ctx)
	}
	return ctx
}

// buildRebuildContext 构建缓存重建上下文，可按策略忽略调用方取消并叠加加载超时。
func (m *Manager) buildRebuildContext(ctx context.Context, target Target, override *RebuildContextPolicy) (context.Context, context.CancelFunc) {
	baseCtx := ctx
	if m.effectiveRebuildContextPolicy(override) == RebuildContextIgnoreCancel {
		baseCtx = context.WithoutCancel(ctx)
	}
	timeout := target.LoaderTimeout
	if timeout <= 0 {
		timeout = m.loaderTTL
	}
	if timeout > 0 {
		return context.WithTimeout(baseCtx, timeout)
	}
	return baseCtx, func() {}
}

// hasHiddenEmptyMarker 判断当前 key 是否存在隐藏空值占位。
func (m *Manager) hasHiddenEmptyMarker(ctx context.Context, target Target, key string) (bool, error) {
	if !target.AllowEmptyMarker || target.VisibleEmptyMark {
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
func (m *Manager) writeHiddenEmptyMarker(ctx context.Context, target Target, key string) error {
	ttl := target.EmptyTTL
	if ttl <= 0 {
		ttl = m.emptyTTL
	}
	return m.store.Write(ctx, Entry{
		Key:   m.emptyKey(key),
		Type:  TypeString,
		Value: m.emptyMarker,
		TTL:   ttl,
	})
}

// deleteRefreshedKey 删除当前刷新目标的业务 key 和隐藏空值元信息，避免空结果保留历史脏数据。
func (m *Manager) deleteRefreshedKey(ctx context.Context, target Target, key string) error {
	keys := []string{m.emptyKey(key), m.emptyCollectionKey(key)}
	if !(strings.HasSuffix(target.Key, ":") && key == target.Key) {
		keys = append(keys, key)
	}
	return m.store.Delete(ctx, keys...)
}

// deleteBusinessKey 删除当前刷新目标的业务 key，保留隐藏空值元信息。
func (m *Manager) deleteBusinessKey(ctx context.Context, target Target, key string) error {
	if strings.HasSuffix(target.Key, ":") && key == target.Key {
		return nil
	}
	return m.store.Delete(ctx, key)
}

// markRebuildResult 写入重建完成元信息，供等待其它实例回填时快速判断刷新已完成。
func (m *Manager) markRebuildResult(ctx context.Context, key string) error {
	if m.resultTTL <= 0 {
		return nil
	}
	return m.store.Write(ctx, Entry{
		Key:   m.rebuildResultKey(key),
		Type:  TypeString,
		Value: time.Now().Format(time.RFC3339Nano),
		TTL:   m.resultTTL,
	})
}

// normalizeTarget 校验并归一化缓存目标配置。
func normalizeTarget(target Target, emptyTTL time.Duration) (Target, error) {
	target.Index = strings.TrimSpace(target.Index)
	target.Title = strings.TrimSpace(target.Title)
	target.Key = strings.TrimSpace(target.Key)
	target.KeyTitle = strings.TrimSpace(target.KeyTitle)
	target.Remark = strings.TrimSpace(target.Remark)
	if target.Index == "" {
		target.Index = strings.Trim(strings.Split(target.Key, ":")[0], "{}")
	}
	if target.Index == "" || target.Key == "" {
		return Target{}, errors.Errorf("tablecache目标配置缺少index或key")
	}
	if target.Type == "" {
		target.Type = TypeString
	}
	if target.EmptyTTL <= 0 {
		target.EmptyTTL = emptyTTL
	}
	return target, nil
}
