package tablecache

import (
	"context"
	"strings"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
)

// fieldsEmptyRegistryScript 管理字段组合独立TTL，并按Redis服务器时间清理过期成员。
var fieldsEmptyRegistryScript = redis.NewScript(fieldsEmptyRegistryLua)

// replaceFieldsWithEmptyScript 原子删除请求字段并登记字段组合为空。
var replaceFieldsWithEmptyScript = redis.NewScript(replaceFieldsWithEmptyLua)

// deleteHashFieldsScript 原子校验锁 owner 并删除请求字段。
var deleteHashFieldsScript = redis.NewScript(deleteHashFieldsLua)

// DeleteHashFields 删除本次请求的旧 Hash 字段和字段空值 registry，保留同 key 的其它字段与 TTL。
func (s *RedisStore) DeleteHashFields(ctx context.Context, guards []LockGuard, key string, registryKey string, fields []string) error {
	if err := s.validate(); err != nil {
		return errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	registryKey = strings.TrimSpace(registryKey)
	fields = uniqueSortedKeys(cleanRedisKeys(fields))
	if key == "" || registryKey == "" || len(fields) == 0 {
		return errors.Errorf("Hash业务key、registry key和fields不能为空")
	}
	if int64(len(fields)) > s.collectionPageLimit() {
		return errors.Wrapf(ErrCollectionTooLarge, "Hash字段数量超过单次删除上限[%d]", s.collectionPageLimit())
	}
	guards, err := cleanLockGuards(guards)
	if err != nil {
		return errors.Tag(err)
	}
	if len(guards) == 0 {
		return errors.Errorf("Hash字段删除缺少锁guard")
	}
	if err := rejectGuardKeyOverlap(guards, key, registryKey); err != nil {
		return errors.Tag(err)
	}
	keys := make([]string, 0, len(guards)+2)
	args := make([]any, 0, len(guards)+len(fields)+1)
	args = append(args, len(guards))
	for _, guard := range guards {
		keys = append(keys, guard.Key)
		args = append(args, guard.Owner)
	}
	keys = append(keys, key, registryKey)
	for _, field := range fields {
		args = append(args, field)
	}
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot(keys); err != nil {
			return errors.Tag(err)
		}
	}
	if _, err := deleteHashFieldsScript.Run(ctx, s.client, keys, args...).Int64(); err != nil {
		return errors.Tag(normalizeLockGuardError(err))
	}
	return nil
}

// ReplaceFieldsWithEmpty 删除请求字段，并登记字段组合为空。
func (s *RedisStore) ReplaceFieldsWithEmpty(ctx context.Context, guards []LockGuard, key string, registryKey string, fieldsID string, fields []string, ttl time.Duration) error {
	if err := s.validate(); err != nil {
		return errors.Tag(err)
	}
	key = strings.TrimSpace(key)
	registryKey = strings.TrimSpace(registryKey)
	fieldsID = strings.TrimSpace(fieldsID)
	fields = uniqueSortedKeys(cleanRedisKeys(fields))
	if key == "" || registryKey == "" || fieldsID == "" || len(fields) == 0 {
		return errors.Errorf("字段空值业务key、registry key、fieldsID和fields不能为空")
	}
	if ttl < time.Millisecond {
		return errors.Errorf("字段空值TTL不能小于1ms")
	}
	ttl = jitterDurationWithDefault(ttl, 0, s.defaultJitterRatio)
	if int64(len(fields)) > s.collectionPageLimit() {
		return errors.Wrapf(ErrCollectionTooLarge, "Hash字段数量超过单次空值替换上限[%d]", s.collectionPageLimit())
	}
	guards, err := cleanLockGuards(guards)
	if err != nil {
		return errors.Tag(err)
	}
	if len(guards) == 0 {
		return errors.Errorf("字段空值提交缺少锁guard")
	}
	if err := rejectGuardKeyOverlap(guards, key, registryKey); err != nil {
		return errors.Tag(err)
	}
	keys := make([]string, 0, len(guards)+2)
	args := make([]any, 0, len(guards)+len(fields)+3)
	args = append(args, len(guards))
	for _, guard := range guards {
		keys = append(keys, guard.Key)
		args = append(args, guard.Owner)
	}
	keys = append(keys, key, registryKey)
	if s.knownDistributedClient() {
		if err := validateSameRedisSlot(keys); err != nil {
			return errors.Tag(err)
		}
	}
	args = append(args, fieldsID, ttlMilliseconds(ttl))
	for _, field := range fields {
		args = append(args, field)
	}
	if _, err := replaceFieldsWithEmptyScript.Run(ctx, s.client, keys, args...).Int64(); err != nil {
		return errors.Tag(normalizeLockGuardError(err))
	}
	return nil
}

// HasFieldsEmpty 判断字段组合空值状态是否仍在独立有效期内。
func (s *RedisStore) HasFieldsEmpty(ctx context.Context, registryKey string, fieldsID string) (bool, error) {
	if err := s.validate(); err != nil {
		return false, errors.Tag(err)
	}
	registryKey = strings.TrimSpace(registryKey)
	fieldsID = strings.TrimSpace(fieldsID)
	if registryKey == "" || fieldsID == "" {
		return false, errors.Errorf("字段空值registry key和fieldsID不能为空")
	}
	result, err := fieldsEmptyRegistryScript.Run(ctx, s.client, []string{registryKey}, fieldsID).Int64()
	if err != nil {
		return false, errors.Tag(err)
	}
	return result == 1, nil
}
