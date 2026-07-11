package tablecache

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/Is999/go-utils/errors"
)

// normalizeFields 清理、排序并去重 Hash 局部读取字段。
func normalizeFields(fields []string) []string {
	clean := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			clean = append(clean, field)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	sort.Strings(clean)
	write := 0
	for read := 0; read < len(clean); read++ {
		if read == 0 || clean[read] != clean[read-1] {
			clean[write] = clean[read]
			write++
		}
	}
	return clean[:write]
}

// fieldsID 返回字段集合的稳定签名；长度前缀消除逗号等字段内容导致的拼接歧义。
func fieldsID(fields []string) string {
	capacity := len(fields)
	for _, field := range fields {
		capacity += len(field)
	}
	body := make([]byte, 0, capacity)
	for _, field := range fields {
		body = binary.AppendUvarint(body, uint64(len(field)))
		body = append(body, field...)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// refreshReadyName 返回刷新完成标识；fields 使用无歧义签名隔离不同字段集合。
func (m *Manager) refreshReadyName(key string, fields []string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		key = "unknown"
	}
	if len(fields) == 0 {
		return "key:" + strconv.Itoa(len(key)) + ":" + key + "|scope:full"
	}
	return "key:" + strconv.Itoa(len(key)) + ":" + key + "|scope:fields:" + fieldsID(fields)
}

// refreshRequestName 为带单次覆盖项的调用追加隔离标识。
func (m *Manager) refreshRequestName(key string, fields []string, options refreshOptions) string {
	name := m.refreshReadyName(key, fields)
	if options.missTriggered {
		name += "|scope=load_through"
	} else if options.requested {
		name += "|scope=requested"
	} else {
		name += "|scope=scheduled"
	}
	if options.flightToken == "" {
		return name
	}
	return name + "|request=" + options.flightToken
}

// hasLoadThroughOverride 判断本次调用是否覆盖了注册目标的刷新语义。
func hasLoadThroughOverride(options LoadThroughOptions) bool {
	return options.Loader != nil || options.LoaderTimeout != 0 || options.AllowEmptyMarker != nil || options.ContextPolicy != nil
}

// validateFieldsRequest 校验 Hash 局部刷新能否由当前 key、目标和 Store 安全表达。
func (m *Manager) validateFieldsRequest(target Target, key string, fields []string) error {
	if len(fields) == 0 {
		return nil
	}
	if strings.HasSuffix(target.Key, ":") && key == target.Key {
		return errors.Wrapf(ErrPartialHashUnsupported, "缓存目标[%s]的前缀全量入口不支持fields", target.Index)
	}
	if target.Type != TypeHash {
		return errors.Wrapf(ErrPartialHashUnsupported, "缓存目标[%s]不是Hash类型", target.Index)
	}
	if _, ok := m.store.(CollectionPageStore); !ok {
		return errors.Wrapf(ErrPartialHashUnsupported, "缓存目标[%s]的Store不支持HMGET", target.Index)
	}
	if _, ok := m.store.(HashFieldsStore); !ok {
		return errors.Wrapf(ErrPartialHashUnsupported, "缓存目标[%s]的Store不支持安全删除Hash字段", target.Index)
	}
	if target.AllowEmptyMarker {
		if _, ok := m.store.(FieldsEmptyStore); !ok {
			return errors.Wrapf(ErrPartialHashUnsupported, "缓存目标[%s]的Store不支持fields空值registry", target.Index)
		}
	}
	return nil
}

// readLookupValue 按请求范围读取缓存；fields 缺少任一字段都视为 miss。
func (m *Manager) readLookupValue(ctx context.Context, target Target, params LoadParams) (any, error) {
	if len(params.Fields) == 0 {
		return m.store.Read(ctx, params.Key, target.Type)
	}
	if err := m.validateFieldsRequest(target, params.Key, params.Fields); err != nil {
		return nil, errors.Tag(err)
	}
	store := m.store.(CollectionPageStore)
	page, err := store.ReadPage(ctx, params.Key, TypeHash, ReadPageOptions{Fields: params.Fields})
	if err != nil {
		return nil, errors.Tag(err)
	}
	values, ok := page.Value.(map[string]string)
	if !ok {
		return nil, errors.Errorf("缓存key[%s]的fields读取结果类型错误", params.Key)
	}
	if len(values) != len(params.Fields) {
		return nil, errors.Tag(ErrCacheMiss)
	}
	return values, nil
}

// normalizePartialHashEntry 校验局部刷新只写请求 key 和字段，并强制使用增量写。
func normalizePartialHashEntry(key string, fields []string, entry Entry) (Entry, error) {
	if entry.Key != key || entry.Type != TypeHash {
		return Entry{}, errors.Wrapf(ErrPartialHashUnsupported, "fields刷新只能写当前Hash key[%s]", key)
	}
	if entry.Overwrite != nil && *entry.Overwrite {
		return Entry{}, errors.Wrapf(ErrPartialHashUnsupported, "fields刷新不能覆盖整个Hash key[%s]", key)
	}
	value := reflect.ValueOf(entry.Value)
	if value.Kind() != reflect.Map || value.Type().Key().Kind() != reflect.String {
		return Entry{}, errors.Wrapf(ErrPartialHashUnsupported, "fields刷新key[%s]必须返回字符串key的map", key)
	}
	want := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		want[field] = struct{}{}
	}
	seen := make(map[string]struct{}, value.Len())
	iterator := value.MapRange()
	for iterator.Next() {
		field := iterator.Key().String()
		if _, ok := want[field]; !ok {
			return Entry{}, errors.Wrapf(ErrPartialHashUnsupported, "fields刷新key[%s]返回了未请求字段[%s]", key, field)
		}
		seen[field] = struct{}{}
	}
	if len(seen) != len(want) {
		return Entry{}, errors.Wrapf(ErrPartialHashUnsupported, "fields刷新key[%s]未返回全部请求字段", key)
	}
	entry.Overwrite = Bool(false)
	return entry, nil
}

// fieldsEmptyKey 返回一个业务 Hash 的字段空值registry key。
func (m *Manager) fieldsEmptyKey(key string) string {
	return tablecacheMetaKey("empty_fields", key)
}

// hasFieldsEmptyMarker 判断当前字段集合是否已有空值占位。
func (m *Manager) hasFieldsEmptyMarker(ctx context.Context, target Target, key string, fields []string) (bool, error) {
	if len(fields) == 0 || !target.AllowEmptyMarker {
		return false, nil
	}
	store, ok := m.store.(FieldsEmptyStore)
	if !ok {
		return false, errors.Tag(ErrPartialHashUnsupported)
	}
	return store.HasFieldsEmpty(ctx, m.fieldsEmptyKey(key), fieldsID(fields))
}

// writeFieldsEmptyMarker 删除请求字段并写入字段集合专属空值占位，其它业务字段保持不变。
func (m *Manager) writeFieldsEmptyMarker(ctx context.Context, target Target, key string, fields []string, guards []LockGuard) error {
	store, ok := m.store.(FieldsEmptyStore)
	if !ok {
		return errors.Tag(ErrPartialHashUnsupported)
	}
	registryKey := m.fieldsEmptyKey(key)
	if indexMutation, ok := m.prefixIndexMutation(target, []string{registryKey}, m.prefixKeyIndexTTL); ok {
		if err := m.applyStoreMutation(ctx, StoreMutation{Guards: guards, AddIndex: []PrefixIndexMutation{indexMutation}}); err != nil {
			return errors.Tag(err)
		}
	}
	ttl := target.EmptyTTL
	if ttl <= 0 {
		ttl = m.emptyTTL
	}
	return store.ReplaceFieldsWithEmpty(ctx, guards, key, registryKey, fieldsID(fields), fields, ttl)
}
