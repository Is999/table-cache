package tablecache

import (
	"math"
	"strings"
	"time"

	"github.com/Is999/go-utils/errors"
)

const (
	// maxRedisBaseTTL 为默认 jitter 预留一倍时长空间，避免运行期 duration 溢出。
	maxRedisBaseTTL = time.Duration(math.MaxInt64 / 2)
)

// validateConfig 校验 Manager 启动参数，避免非法配置在流量进入后才暴露。
func (m *Manager) validateConfig() error {
	if m.lockTTL < minLockTTL {
		return errors.Wrapf(ErrInvalidConfig, "缓存锁TTL必须至少%s: %s", minLockTTL, m.lockTTL)
	}
	if m.lockTTL > maxOperationTimeout || m.loaderTTL > maxOperationTimeout {
		return errors.Wrapf(ErrInvalidConfig, "锁或Loader超时不能超过%s", maxOperationTimeout)
	}
	if m.lockRenew < 0 || m.lockRenew >= m.lockTTL {
		return errors.Wrapf(ErrInvalidConfig, "缓存锁续期间隔[%s]必须为0或小于锁TTL[%s]", m.lockRenew, m.lockTTL)
	}
	if m.lockRenew > 0 && m.lockRenew < minLockRenewInterval {
		return errors.Wrapf(ErrInvalidConfig, "缓存锁续期间隔必须为0或至少%s: %s", minLockRenewInterval, m.lockRenew)
	}
	if m.loaderTTL <= 0 {
		return errors.Wrapf(ErrInvalidConfig, "Loader超时时间必须大于0: %s", m.loaderTTL)
	}
	if m.loaderConcurrency <= 0 || m.loaderConcurrency > maxWorkerConcurrency {
		return errors.Wrapf(ErrInvalidConfig, "Loader并发上限必须在1到%d之间: %d", maxWorkerConcurrency, m.loaderConcurrency)
	}
	if !validRebuildContextPolicy(m.ctxPolicy) {
		return errors.Wrapf(ErrInvalidConfig, "缓存重建上下文策略非法: %d", m.ctxPolicy)
	}
	if m.concurrency <= 0 || m.concurrency > maxWorkerConcurrency {
		return errors.Wrapf(ErrInvalidConfig, "批量刷新并发度必须在1到%d之间: %d", maxWorkerConcurrency, m.concurrency)
	}
	if m.prefixDeleteConcurrency <= 0 || m.prefixDeleteConcurrency > maxWorkerConcurrency {
		return errors.Wrapf(ErrInvalidConfig, "前缀删除并发度必须在1到%d之间: %d", maxWorkerConcurrency, m.prefixDeleteConcurrency)
	}
	if m.scanCount <= 0 || m.scanCount > maxScanCount {
		return errors.Wrapf(ErrInvalidConfig, "Redis SCAN count必须在1到%d之间: %d", maxScanCount, m.scanCount)
	}
	if m.emptyTTL < time.Millisecond || m.resultTTL < time.Millisecond || m.prefixEpochTTL < time.Millisecond || m.prefixKeyIndexTTL < time.Millisecond {
		return errors.Wrapf(ErrInvalidConfig, "Manager内部Redis TTL必须全部至少1ms")
	}
	if m.emptyTTL > maxRedisBaseTTL || m.resultTTL > maxRedisBaseTTL || m.prefixEpochTTL > maxRedisBaseTTL || m.prefixKeyIndexTTL > maxRedisBaseTTL {
		return errors.Wrapf(ErrInvalidConfig, "Manager内部Redis TTL不能超过%s", maxRedisBaseTTL)
	}
	if m.waitConfigured {
		if m.waitStep < minWaitStep || m.waitTimes <= 0 {
			return errors.Wrapf(ErrInvalidConfig, "显式等待策略的step必须至少%s且times必须大于0", minWaitStep)
		}
		if m.waitTimes > maxWaitAttempts {
			return errors.Wrapf(ErrInvalidConfig, "显式等待次数不能超过%d: %d", maxWaitAttempts, m.waitTimes)
		}
		if m.waitStep > maxOperationTimeout/time.Duration(m.waitTimes) {
			return errors.Wrapf(ErrInvalidConfig, "显式等待总时长不能超过%s", maxOperationTimeout)
		}
	}
	return nil
}

// validateTargetConfig 校验目标类型、TTL 和全量刷新契约。
func validateTargetConfig(target Target) error {
	if !validCacheType(target.Type) {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]类型非法: %s", target.Index, target.Type)
	}
	if target.TTL < 0 || target.Jitter < 0 || target.EmptyTTL < 0 || target.LoaderTimeout < 0 {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]包含负数TTL或超时", target.Index)
	}
	if target.TTL == 0 && target.Jitter > 0 {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]永久TTL不能配置抖动", target.Index)
	}
	if target.Jitter > 0 && target.TTL > time.Duration(math.MaxInt64)-target.Jitter {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]的TTL与抖动之和溢出", target.Index)
	}
	if target.Jitter == 0 && target.TTL > maxRedisBaseTTL {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]的TTL超过默认抖动安全上限[%s]", target.Index, maxRedisBaseTTL)
	}
	if target.EmptyTTL > maxRedisBaseTTL {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]的EmptyTTL超过默认抖动安全上限[%s]", target.Index, maxRedisBaseTTL)
	}
	if (target.TTL > 0 && target.TTL < time.Millisecond) || (target.EmptyTTL > 0 && target.EmptyTTL < time.Millisecond) {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]的Redis TTL必须为0或至少1ms", target.Index)
	}
	if target.RefreshAll && target.Loader == nil {
		return errors.Wrapf(ErrLoaderRequired, "缓存目标[%s]启用RefreshAll但未配置Loader", target.Index)
	}
	if target.LoaderTimeout > maxOperationTimeout {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]Loader超时不能超过%s", target.Index, maxOperationTimeout)
	}
	return nil
}

// validateEntryConfig 校验 Loader 返回条目的类型和过期配置。
func validateEntryConfig(target Target, entry Entry) error {
	if !validCacheType(entry.Type) {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]返回了非法类型[%s]", target.Index, entry.Type)
	}
	if entry.TTL < 0 || entry.Jitter < 0 {
		return errors.Wrapf(ErrInvalidConfig, "缓存key[%s]包含负数TTL或抖动", entry.Key)
	}
	if entry.TTL == 0 && entry.Jitter > 0 {
		return errors.Wrapf(ErrInvalidConfig, "永久缓存key[%s]不能配置抖动", entry.Key)
	}
	if entry.Jitter > 0 && entry.TTL > time.Duration(math.MaxInt64)-entry.Jitter {
		return errors.Wrapf(ErrInvalidConfig, "缓存key[%s]的TTL与抖动之和溢出", entry.Key)
	}
	if entry.Jitter == 0 && entry.TTL > maxRedisBaseTTL {
		return errors.Wrapf(ErrInvalidConfig, "缓存key[%s]的TTL超过默认抖动安全上限[%s]", entry.Key, maxRedisBaseTTL)
	}
	if entry.TTL > 0 && entry.TTL < time.Millisecond {
		return errors.Wrapf(ErrInvalidConfig, "缓存key[%s]的TTL必须为0或至少1ms", entry.Key)
	}
	if (entry.Type == TypeString || entry.Type == TypeList) && entry.Overwrite != nil && !*entry.Overwrite {
		return errors.Wrapf(ErrInvalidConfig, "缓存key[%s]的类型[%s]不支持增量写入", entry.Key, entry.Type)
	}
	return nil
}

// validateLoadThroughOptions 校验单次读穿覆盖项，避免非法值被静默忽略。
func (m *Manager) validateLoadThroughOptions(target Target, options LoadThroughOptions) error {
	if options.LoaderTimeout < 0 {
		return errors.Wrapf(ErrInvalidConfig, "单次Loader超时时间不能为负数: %s", options.LoaderTimeout)
	}
	if options.LoaderTimeout > maxOperationTimeout {
		return errors.Wrapf(ErrInvalidConfig, "单次Loader超时不能超过%s", maxOperationTimeout)
	}
	if options.ContextPolicy != nil && !validRebuildContextPolicy(*options.ContextPolicy) {
		return errors.Wrapf(ErrInvalidConfig, "单次缓存重建上下文策略非法: %d", *options.ContextPolicy)
	}
	timeout := options.LoaderTimeout
	if timeout == 0 {
		timeout = target.LoaderTimeout
	}
	if timeout == 0 {
		timeout = m.loaderTTL
	}
	if m.allowsPrefixMutation(target) && timeout > m.prefixEpochTTL/2 {
		return errors.Wrapf(ErrInvalidConfig, "单次Loader超时[%s]超过前缀代际安全窗口[%s]", timeout, m.prefixEpochTTL/2)
	}
	return nil
}

// validateTargetRuntimeConfig 校验可执行跨 key 前缀操作的 Loader 不会越过代际保留窗口。
func (m *Manager) validateTargetRuntimeConfig(target Target) error {
	if !m.allowsPrefixMutation(target) {
		return nil
	}
	timeout := target.LoaderTimeout
	if timeout == 0 {
		timeout = m.loaderTTL
	}
	if timeout > m.prefixEpochTTL/2 {
		return errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]Loader超时[%s]超过前缀代际安全窗口[%s]", target.Index, timeout, m.prefixEpochTTL/2)
	}
	return nil
}

// validCacheType 判断缓存类型是否由当前 Store 契约支持。
func validCacheType(typ CacheType) bool {
	switch typ {
	case TypeString, TypeHash, TypeList, TypeSet, TypeZSet:
		return true
	default:
		return false
	}
}

// validRebuildContextPolicy 判断重建取消策略是否有效。
func validRebuildContextPolicy(policy RebuildContextPolicy) bool {
	return policy == RebuildContextInheritCancel || policy == RebuildContextIgnoreCancel
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
	if target.EmptyTTL == 0 {
		target.EmptyTTL = emptyTTL
	}
	if err := validateTargetConfig(target); err != nil {
		return Target{}, errors.Tag(err)
	}
	return target, nil
}
