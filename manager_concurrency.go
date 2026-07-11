package tablecache

import (
	"context"

	"github.com/Is999/go-utils/errors"
)

// loadEntries 在全局并发上限内执行 Loader，等待并发槽时支持上下文取消。
func (m *Manager) loadEntries(ctx context.Context, target Target, params LoadParams) ([]Entry, error) {
	timeout := target.LoaderTimeout
	if timeout <= 0 {
		timeout = m.loaderTTL
	}
	loaderCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		loaderCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	select {
	case m.loaderSlots <- struct{}{}:
		defer func() { <-m.loaderSlots }()
	case <-loaderCtx.Done():
		return nil, errors.Tag(loaderCtx.Err())
	}
	return target.Loader(loaderCtx, params)
}
