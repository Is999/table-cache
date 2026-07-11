package tablecache

import (
	"context"
	"testing"
	"time"
)

// acquireTestLockGuard 获取测试专用锁，并在用例结束时安全释放。
func acquireTestLockGuard(t testing.TB, store Store, scope string) []LockGuard {
	t.Helper()
	key := tablecacheMetaKey("test:lock", scope)
	owner := newLockValue()
	locked, _, err := store.AcquireRefreshLock(context.Background(), key, owner, time.Minute)
	if err != nil || !locked {
		t.Fatalf("AcquireRefreshLock(%s) = %v,%v", key, locked, err)
	}
	t.Cleanup(func() {
		_, _ = store.ReleaseLock(context.Background(), key, owner)
	})
	return []LockGuard{{Key: key, Owner: owner}}
}

// commitTestPrefixIndex 使用真实测试锁提交索引 manifest，覆盖生产方法的 fencing 契约。
func commitTestPrefixIndex(t testing.TB, store *RedisStore, indexKey string, ttl time.Duration) error {
	t.Helper()
	return store.commitPrefixIndex(context.Background(), indexKey, ttl, acquireTestLockGuard(t, store, "index-commit:"+indexKey))
}
