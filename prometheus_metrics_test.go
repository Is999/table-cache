package tablecache

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestPrometheusMetricsRecord 验证 Prometheus 指标适配器可完整记录基础、扩展和读取链路指标。
func TestPrometheusMetricsRecord(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusMetrics(
		WithPrometheusRegisterer(registry),
		WithPrometheusNamespace("tablecache"),
		WithPrometheusSubsystem("test"),
	)
	if err != nil {
		t.Fatalf("NewPrometheusMetrics() error = %v", err)
	}
	metrics.RecordRefresh(ctx, "user", "success", 150*time.Millisecond)
	metrics.RecordCacheHit(ctx, "user")
	metrics.RecordCacheMiss(ctx, "user")
	metrics.RecordLockFailed(ctx, "user")
	metrics.RecordLoaderError(ctx, "user", ErrNotFound)
	metrics.RecordEmptyMarkerWrite(ctx, "user")
	metrics.RecordWaitTimeout(ctx, "user")
	metrics.RecordPrefixWait(ctx, "user")
	metrics.RecordPrefixRetry(ctx, "user")
	metrics.RecordPrefixDelete(ctx, "user", "user:", 3)
	metrics.RecordRefreshEntryCount(ctx, "user", 2)
	metrics.RecordLookupState(ctx, "user", LookupStateHit)
	metrics.RecordLookupState(ctx, "user", LookupStateMiss)
	metrics.RecordLookupState(ctx, "user", LookupStateEmpty)
	metrics.RecordLookupRefreshTriggered(ctx, "user")
	metrics.RecordRefreshBatch(ctx, "keys", "partial_failed", 3, 2, 1)

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather() error = %v", err)
	}
	assertMetricCounter(t, metricFamilies, "tablecache_test_refresh_total", map[string]string{"index": "user", "result": "success"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_cache_hit_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_cache_miss_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_lock_failed_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_loader_error_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_empty_marker_write_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_wait_timeout_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_prefix_wait_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_prefix_retry_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_prefix_delete_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_prefix_delete_keys_total", map[string]string{"index": "user"}, 3)
	assertMetricCounter(t, metricFamilies, "tablecache_test_lookup_state_total", map[string]string{"index": "user", "state": "hit"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_lookup_state_total", map[string]string{"index": "user", "state": "miss"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_lookup_state_total", map[string]string{"index": "user", "state": "empty"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_lookup_refresh_triggered_total", map[string]string{"index": "user"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_refresh_batch_total", map[string]string{"mode": "keys", "result": "partial_failed"}, 1)
	assertMetricCounter(t, metricFamilies, "tablecache_test_refresh_batch_success_items_total", map[string]string{"mode": "keys"}, 2)
	assertMetricCounter(t, metricFamilies, "tablecache_test_refresh_batch_failed_items_total", map[string]string{"mode": "keys"}, 1)
	assertHistogramSampleCount(t, metricFamilies, "tablecache_test_refresh_duration_seconds", map[string]string{"index": "user", "result": "success"}, 1)
	assertHistogramSampleCount(t, metricFamilies, "tablecache_test_refresh_entry_count", map[string]string{"index": "user"}, 1)
	assertHistogramSampleCount(t, metricFamilies, "tablecache_test_refresh_batch_size", map[string]string{"mode": "keys", "result": "partial_failed"}, 1)
}

// TestPrometheusMetricsDuplicateRegister 验证同一 Registry 下重复初始化时会复用已注册 Collector。
func TestPrometheusMetricsDuplicateRegister(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	firstMetrics, err := NewPrometheusMetrics(
		WithPrometheusRegisterer(registry),
		WithPrometheusNamespace("tablecache"),
	)
	if err != nil {
		t.Fatalf("first NewPrometheusMetrics() error = %v", err)
	}
	secondMetrics, err := NewPrometheusMetrics(
		WithPrometheusRegisterer(registry),
		WithPrometheusNamespace("tablecache"),
	)
	if err != nil {
		t.Fatalf("second NewPrometheusMetrics() error = %v", err)
	}
	firstMetrics.RecordCacheHit(ctx, "profile")
	secondMetrics.RecordCacheHit(ctx, "profile")

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather() error = %v", err)
	}
	assertMetricCounter(t, metricFamilies, "tablecache_cache_hit_total", map[string]string{"index": "profile"}, 2)
}

// assertMetricCounter 断言指定标签组合对应的 Counter 值。
func assertMetricCounter(t *testing.T, metricFamilies []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	metric := findMetric(t, metricFamilies, name, labels)
	if metric.GetCounter().GetValue() != want {
		t.Fatalf("metric %s labels=%v value=%v, want %v", name, labels, metric.GetCounter().GetValue(), want)
	}
}

// assertHistogramSampleCount 断言指定标签组合对应的 Histogram 样本数量。
func assertHistogramSampleCount(t *testing.T, metricFamilies []*dto.MetricFamily, name string, labels map[string]string, want uint64) {
	t.Helper()
	metric := findMetric(t, metricFamilies, name, labels)
	if metric.GetHistogram().GetSampleCount() != want {
		t.Fatalf("metric %s labels=%v sample_count=%d, want %d", name, labels, metric.GetHistogram().GetSampleCount(), want)
	}
}

// findMetric 从采集结果中查找指定名称和标签组合的指标样本。
func findMetric(t *testing.T, metricFamilies []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	for _, metricFamily := range metricFamilies {
		if metricFamily.GetName() != name {
			continue
		}
		for _, metric := range metricFamily.GetMetric() {
			if matchMetricLabels(metric, labels) {
				return metric
			}
		}
	}
	t.Fatalf("metric %s labels=%v not found", name, labels)
	return nil
}

// matchMetricLabels 判断指标样本标签是否完全匹配。
func matchMetricLabels(metric *dto.Metric, labels map[string]string) bool {
	if len(metric.GetLabel()) != len(labels) {
		return false
	}
	for _, labelPair := range metric.GetLabel() {
		if labels[labelPair.GetName()] != labelPair.GetValue() {
			return false
		}
	}
	return true
}
