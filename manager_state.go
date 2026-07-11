package tablecache

import (
	"context"
	"strings"
	"time"

	"github.com/Is999/go-utils/errors"
)

// prepareRefreshSnapshot 在短提交锁内建立非空代际快照，代际被淘汰时后续提交按失效处理。
func (m *Manager) prepareRefreshSnapshot(ctx context.Context, target Target, key string, refreshGuard LockGuard) (refreshEpochSnapshot, error) {
	if !strings.HasSuffix(target.Key, ":") || !m.allowsPrefixMutation(target) {
		return m.prepareRefreshEpochs(ctx, target, key, nil)
	}
	var snapshot refreshEpochSnapshot
	recordWait := key != target.Key
	err := m.withPrefixCommitLock(ctx, target, recordWait, func(commitCtx context.Context, commitGuard LockGuard) error {
		var err error
		snapshot, err = m.prepareRefreshEpochs(commitCtx, target, key, commitLockGuards(refreshGuard, commitGuard))
		return errors.Tag(err)
	})
	return snapshot, errors.Tag(err)
}

// normalizeRefreshEntries 在任何缓存提交前完成默认值、作用域和字段契约校验。
func (m *Manager) normalizeRefreshEntries(target Target, key string, fields []string, entries []Entry) ([]Entry, error) {
	normalized := make([]Entry, len(entries))
	seenKeys := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		entry.Key = strings.TrimSpace(entry.Key)
		entry = m.withTargetDefaults(target, entry)
		if err := m.validateRefreshEntryScope(target, entry); err != nil {
			return nil, errors.Tag(err)
		}
		if err := validateEntryConfig(target, entry); err != nil {
			return nil, errors.Tag(err)
		}
		if entry.Type != target.Type {
			return nil, errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]不能写入类型[%s]", target.Index, entry.Type)
		}
		if strings.HasSuffix(target.Key, ":") && key != target.Key && entry.Key != key {
			return nil, errors.Wrapf(ErrEntryKeyOutOfScope, "缓存目标[%s]单key刷新只能写入请求key[%s]", target.Index, key)
		}
		if isEmptyCollectionEntry(entry) && entry.Overwrite != nil && !*entry.Overwrite {
			return nil, errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]空集合不能使用增量写入", target.Index)
		}
		if strings.HasSuffix(target.Key, ":") && key == target.Key && entry.Overwrite != nil && !*entry.Overwrite {
			return nil, errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]全量快照不能使用增量写入", target.Index)
		}
		if _, ok := seenKeys[entry.Key]; ok {
			return nil, errors.Wrapf(ErrInvalidConfig, "缓存目标[%s]返回了重复key[%s]", target.Index, entry.Key)
		}
		seenKeys[entry.Key] = struct{}{}
		if len(fields) > 0 {
			if len(entries) != 1 {
				return nil, errors.Wrapf(ErrPartialHashUnsupported, "fields刷新key[%s]必须返回一条Entry", key)
			}
			var err error
			entry, err = normalizePartialHashEntry(key, fields, entry)
			if err != nil {
				return nil, errors.Tag(err)
			}
		}
		normalized[index] = entry
	}
	return normalized, nil
}

// writeRefreshEmptyMarker 按整 key 或 fields 范围写入隐藏空值占位。
func (m *Manager) writeRefreshEmptyMarker(ctx context.Context, target Target, key string, fields []string, guards []LockGuard) error {
	if len(fields) > 0 {
		return m.writeFieldsEmptyMarker(ctx, target, key, fields, guards)
	}
	return m.writeHiddenEmptyMarker(ctx, target, key, guards)
}

// refreshWriteEntries 把 Redis 无法保存的空集合转换成内部元信息条目。
func (m *Manager) refreshWriteEntries(entries []Entry) []Entry {
	writeEntries := make([]Entry, 0, len(entries))
	emptyCollections := make([]Entry, 0)
	for _, entry := range entries {
		if isEmptyCollectionEntry(entry) {
			emptyCollections = append(emptyCollections, entry)
			continue
		}
		writeEntries = append(writeEntries, entry)
	}
	return append(writeEntries, m.emptyCollectionMarkerEntries(emptyCollections)...)
}

// replacePrefixEntries 安全替换前缀快照；Store 不支持时拒绝破坏性降级。
func (m *Manager) replacePrefixEntries(ctx context.Context, target Target, entries []Entry, guards []LockGuard) error {
	store, ok := m.store.(PrefixReplaceStore)
	if !ok {
		return errors.Wrapf(ErrPrefixReplaceUnsupported, "缓存目标[%s]的Store不支持安全前缀替换", target.Index)
	}
	writeEntries := m.refreshWriteEntries(entries)
	keepKeys := make([]string, 0, len(writeEntries))
	for _, entry := range writeEntries {
		keepKeys = append(keepKeys, entry.Key)
	}
	indexKey := ""
	readyKey := ""
	indexTTL := time.Duration(0)
	if m.prefixKeyIndex {
		indexKey = m.prefixIndexKey(target)
		readyKey = m.prefixIndexReadyKey(target)
		indexTTL = m.prefixKeyIndexTTL
	}
	deleted, err := store.ReplacePrefix(ctx, PrefixReplaceMutation{
		Guards:         guards,
		Prefix:         target.Key,
		Entries:        writeEntries,
		KeepKeys:       keepKeys,
		DeletePatterns: m.prefixDeletePatterns(target),
		IndexKey:       indexKey,
		ReadyKey:       readyKey,
		IndexTTL:       indexTTL,
		ScanCount:      m.scanCount,
	})
	if err != nil {
		return errors.Tag(err)
	}
	m.recordPrefixDelete(ctx, target.Index, target.Key, deleted)
	m.recordRefreshEntryCount(ctx, target.Index, len(entries))
	return nil
}

// prefixDeletePatterns 返回前缀快照覆盖的业务与内部元信息 pattern。
func (m *Manager) prefixDeletePatterns(target Target) []string {
	patterns := []string{RedisPrefixPattern(target.Key)}
	for _, kind := range []string{"empty", "empty_collection", "empty_fields", "rebuild:result", "rebuild:fields_result"} {
		base := strings.TrimSuffix(tablecacheMetaKeyPattern(kind, target.Key), "*")
		patterns = append(patterns, RedisPrefixPattern(base))
	}
	return patterns
}
