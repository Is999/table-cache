package tablecache

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var defaultRefreshEntryBuckets = []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 500}

// PrometheusMetricsOption 表示 Prometheus 指标适配器的可选配置。
type PrometheusMetricsOption func(*PrometheusMetricsConfig)

// PrometheusMetricsConfig 表示 Prometheus 指标适配器的构造配置。
type PrometheusMetricsConfig struct {
	Namespace           string                // Namespace 是 Prometheus 指标命名空间
	Subsystem           string                // Subsystem 是 Prometheus 指标子系统
	Registerer          prometheus.Registerer // Registerer 是指标注册器，nil 时默认使用全局注册器
	RefreshBuckets      []float64             // RefreshBuckets 是刷新耗时直方图桶
	RefreshEntryBuckets []float64             // RefreshEntryBuckets 是单次刷新写入条数直方图桶
}

// PrometheusMetrics 提供 Metrics、ExtendedMetrics、LookupMetrics 的 Prometheus 实现。
type PrometheusMetrics struct {
	refreshTotal                *prometheus.CounterVec   // refreshTotal 记录刷新次数
	refreshDuration             *prometheus.HistogramVec // refreshDuration 记录刷新耗时
	cacheHitTotal               *prometheus.CounterVec   // cacheHitTotal 记录基础缓存命中次数
	cacheMissTotal              *prometheus.CounterVec   // cacheMissTotal 记录基础缓存未命中次数
	lockFailedTotal             *prometheus.CounterVec   // lockFailedTotal 记录锁竞争失败次数
	loaderErrorTotal            *prometheus.CounterVec   // loaderErrorTotal 记录 Loader 错误次数
	emptyMarkerWriteTotal       *prometheus.CounterVec   // emptyMarkerWriteTotal 记录空值占位写入次数
	waitTimeoutTotal            *prometheus.CounterVec   // waitTimeoutTotal 记录等待重建超时次数
	prefixWaitTotal             *prometheus.CounterVec   // prefixWaitTotal 记录前缀全量刷新阻塞单 key 的次数
	prefixRetryTotal            *prometheus.CounterVec   // prefixRetryTotal 记录单 key 因前缀全量刷新重试的次数
	prefixDeleteTotal           *prometheus.CounterVec   // prefixDeleteTotal 记录前缀删除次数
	prefixDeleteKeysTotal       *prometheus.CounterVec   // prefixDeleteKeysTotal 记录前缀删除 key 数量
	refreshEntryCount           *prometheus.HistogramVec // refreshEntryCount 记录单次刷新写入条数
	lookupStateTotal            *prometheus.CounterVec   // lookupStateTotal 记录读取状态细分指标
	lookupRefreshTriggeredTotal *prometheus.CounterVec   // lookupRefreshTriggeredTotal 记录读穿触发刷新次数
	refreshBatchTotal           *prometheus.CounterVec   // refreshBatchTotal 记录批量刷新与全量刷新任务次数
	refreshBatchSize            *prometheus.HistogramVec // refreshBatchSize 记录每次批量刷新涉及的目标数量
	refreshBatchSuccessItems    *prometheus.CounterVec   // refreshBatchSuccessItems 记录批量刷新成功条目数量
	refreshBatchFailedItems     *prometheus.CounterVec   // refreshBatchFailedItems 记录批量刷新失败条目数量
}

// WithPrometheusNamespace 设置 Prometheus 指标命名空间。
func WithPrometheusNamespace(namespace string) PrometheusMetricsOption {
	return func(config *PrometheusMetricsConfig) {
		config.Namespace = namespace
	}
}

// WithPrometheusSubsystem 设置 Prometheus 指标子系统。
func WithPrometheusSubsystem(subsystem string) PrometheusMetricsOption {
	return func(config *PrometheusMetricsConfig) {
		config.Subsystem = subsystem
	}
}

// WithPrometheusRegisterer 设置 Prometheus 指标注册器。
func WithPrometheusRegisterer(registerer prometheus.Registerer) PrometheusMetricsOption {
	return func(config *PrometheusMetricsConfig) {
		config.Registerer = registerer
	}
}

// WithPrometheusRefreshBuckets 设置刷新耗时直方图桶。
func WithPrometheusRefreshBuckets(buckets []float64) PrometheusMetricsOption {
	return func(config *PrometheusMetricsConfig) {
		if len(buckets) > 0 {
			config.RefreshBuckets = buckets
		}
	}
}

// WithPrometheusRefreshEntryBuckets 设置单次刷新写入条数直方图桶。
func WithPrometheusRefreshEntryBuckets(buckets []float64) PrometheusMetricsOption {
	return func(config *PrometheusMetricsConfig) {
		if len(buckets) > 0 {
			config.RefreshEntryBuckets = buckets
		}
	}
}

// NewPrometheusMetrics 创建 Prometheus 指标适配器，默认兼容重复注册场景。
func NewPrometheusMetrics(opts ...PrometheusMetricsOption) (*PrometheusMetrics, error) {
	config := PrometheusMetricsConfig{
		Registerer:          prometheus.DefaultRegisterer,
		RefreshBuckets:      prometheus.DefBuckets,
		RefreshEntryBuckets: defaultRefreshEntryBuckets,
	}
	for _, opt := range opts {
		opt(&config)
	}
	if config.Registerer == nil {
		config.Registerer = prometheus.DefaultRegisterer
	}
	metrics := &PrometheusMetrics{}
	var err error
	metrics.refreshTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "refresh_total",
			Help:      "tablecache refresh total count",
		},
		[]string{"index", "result"},
	))
	if err != nil {
		return nil, err
	}
	metrics.refreshDuration, err = registerHistogramVec(config.Registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "refresh_duration_seconds",
			Help:      "tablecache refresh duration in seconds",
			Buckets:   config.RefreshBuckets,
		},
		[]string{"index", "result"},
	))
	if err != nil {
		return nil, err
	}
	metrics.cacheHitTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "cache_hit_total",
			Help:      "tablecache cache hit total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.cacheMissTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "cache_miss_total",
			Help:      "tablecache cache miss total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.lockFailedTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "lock_failed_total",
			Help:      "tablecache lock failed total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.loaderErrorTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "loader_error_total",
			Help:      "tablecache loader error total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.emptyMarkerWriteTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "empty_marker_write_total",
			Help:      "tablecache empty marker write total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.waitTimeoutTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "wait_timeout_total",
			Help:      "tablecache wait timeout total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.prefixWaitTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "prefix_wait_total",
			Help:      "tablecache prefix full refresh wait total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.prefixRetryTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "prefix_retry_total",
			Help:      "tablecache prefix full refresh retry total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.prefixDeleteTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "prefix_delete_total",
			Help:      "tablecache prefix delete total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.prefixDeleteKeysTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "prefix_delete_keys_total",
			Help:      "tablecache prefix delete deleted key total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.refreshEntryCount, err = registerHistogramVec(config.Registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "refresh_entry_count",
			Help:      "tablecache refresh written entry count",
			Buckets:   config.RefreshEntryBuckets,
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.lookupStateTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "lookup_state_total",
			Help:      "tablecache lookup state total count",
		},
		[]string{"index", "state"},
	))
	if err != nil {
		return nil, err
	}
	metrics.lookupRefreshTriggeredTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "lookup_refresh_triggered_total",
			Help:      "tablecache lookup refresh triggered total count",
		},
		[]string{"index"},
	))
	if err != nil {
		return nil, err
	}
	metrics.refreshBatchTotal, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "refresh_batch_total",
			Help:      "tablecache refresh batch total count",
		},
		[]string{"mode", "result"},
	))
	if err != nil {
		return nil, err
	}
	metrics.refreshBatchSize, err = registerHistogramVec(config.Registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "refresh_batch_size",
			Help:      "tablecache refresh batch size",
			Buckets:   config.RefreshEntryBuckets,
		},
		[]string{"mode", "result"},
	))
	if err != nil {
		return nil, err
	}
	metrics.refreshBatchSuccessItems, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "refresh_batch_success_items_total",
			Help:      "tablecache refresh batch success item total count",
		},
		[]string{"mode"},
	))
	if err != nil {
		return nil, err
	}
	metrics.refreshBatchFailedItems, err = registerCounterVec(config.Registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "refresh_batch_failed_items_total",
			Help:      "tablecache refresh batch failed item total count",
		},
		[]string{"mode"},
	))
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

// RecordRefresh 记录刷新次数和刷新耗时。
func (m *PrometheusMetrics) RecordRefresh(ctx context.Context, index string, result string, duration time.Duration) {
	m.refreshTotal.WithLabelValues(index, result).Inc()
	m.refreshDuration.WithLabelValues(index, result).Observe(duration.Seconds())
}

// RecordCacheHit 记录基础缓存命中次数。
func (m *PrometheusMetrics) RecordCacheHit(ctx context.Context, index string) {
	m.cacheHitTotal.WithLabelValues(index).Inc()
}

// RecordCacheMiss 记录基础缓存未命中次数。
func (m *PrometheusMetrics) RecordCacheMiss(ctx context.Context, index string) {
	m.cacheMissTotal.WithLabelValues(index).Inc()
}

// RecordLockFailed 记录锁竞争失败次数。
func (m *PrometheusMetrics) RecordLockFailed(ctx context.Context, index string) {
	m.lockFailedTotal.WithLabelValues(index).Inc()
}

// RecordLoaderError 记录 Loader 错误次数。
func (m *PrometheusMetrics) RecordLoaderError(ctx context.Context, index string, err error) {
	m.loaderErrorTotal.WithLabelValues(index).Inc()
}

// RecordEmptyMarkerWrite 记录空值占位写入次数。
func (m *PrometheusMetrics) RecordEmptyMarkerWrite(ctx context.Context, index string) {
	m.emptyMarkerWriteTotal.WithLabelValues(index).Inc()
}

// RecordWaitTimeout 记录等待重建超时次数。
func (m *PrometheusMetrics) RecordWaitTimeout(ctx context.Context, index string) {
	m.waitTimeoutTotal.WithLabelValues(index).Inc()
}

// RecordPrefixWait 记录前缀全量刷新阻塞单 key 的次数。
func (m *PrometheusMetrics) RecordPrefixWait(ctx context.Context, index string) {
	m.prefixWaitTotal.WithLabelValues(index).Inc()
}

// RecordPrefixRetry 记录单 key 因前缀全量刷新重试的次数。
func (m *PrometheusMetrics) RecordPrefixRetry(ctx context.Context, index string) {
	m.prefixRetryTotal.WithLabelValues(index).Inc()
}

// RecordPrefixDelete 记录前缀删除次数和删除 key 总数。
func (m *PrometheusMetrics) RecordPrefixDelete(ctx context.Context, index string, prefix string, count int64) {
	m.prefixDeleteTotal.WithLabelValues(index).Inc()
	m.prefixDeleteKeysTotal.WithLabelValues(index).Add(float64(count))
}

// RecordRefreshEntryCount 记录单次刷新写入条数。
func (m *PrometheusMetrics) RecordRefreshEntryCount(ctx context.Context, index string, count int) {
	m.refreshEntryCount.WithLabelValues(index).Observe(float64(count))
}

// RecordLookupState 记录读取状态细分指标。
func (m *PrometheusMetrics) RecordLookupState(ctx context.Context, index string, state LookupState) {
	m.lookupStateTotal.WithLabelValues(index, string(state)).Inc()
}

// RecordLookupRefreshTriggered 记录读穿触发刷新次数。
func (m *PrometheusMetrics) RecordLookupRefreshTriggered(ctx context.Context, index string) {
	m.lookupRefreshTriggeredTotal.WithLabelValues(index).Inc()
}

// RecordRefreshBatch 记录批量刷新与全量刷新任务的次数、规模和成功失败条目数。
func (m *PrometheusMetrics) RecordRefreshBatch(ctx context.Context, mode string, result string, total int, success int, failed int) {
	m.refreshBatchTotal.WithLabelValues(mode, result).Inc()
	m.refreshBatchSize.WithLabelValues(mode, result).Observe(float64(total))
	m.refreshBatchSuccessItems.WithLabelValues(mode).Add(float64(success))
	m.refreshBatchFailedItems.WithLabelValues(mode).Add(float64(failed))
}

// registerCounterVec 注册 CounterVec；若已注册则复用已有 Collector。
func registerCounterVec(registerer prometheus.Registerer, counter *prometheus.CounterVec) (*prometheus.CounterVec, error) {
	if err := registerer.Register(counter); err != nil {
		existingError, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			return nil, err
		}
		existingCounter, ok := existingError.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			return nil, err
		}
		return existingCounter, nil
	}
	return counter, nil
}

// registerHistogramVec 注册 HistogramVec；若已注册则复用已有 Collector。
func registerHistogramVec(registerer prometheus.Registerer, histogram *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
	if err := registerer.Register(histogram); err != nil {
		existingError, ok := err.(prometheus.AlreadyRegisteredError)
		if !ok {
			return nil, err
		}
		existingHistogram, ok := existingError.ExistingCollector.(*prometheus.HistogramVec)
		if !ok {
			return nil, err
		}
		return existingHistogram, nil
	}
	return histogram, nil
}
