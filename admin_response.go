package tablecache

import "github.com/Is999/go-utils/errors"

const (
	// AdminCodeOK 表示管理页操作执行成功。
	AdminCodeOK = "ok"
	// AdminCodePartialFailure 表示批量操作中存在失败项。
	AdminCodePartialFailure = "partial_failure"
	// AdminCodeInvalidRequest 表示请求或配置不符合当前缓存操作约束。
	AdminCodeInvalidRequest = "invalid_request"
	// AdminCodeTargetNotFound 表示请求的缓存目标未注册。
	AdminCodeTargetNotFound = "target_not_found"
	// AdminCodeLoaderRequired 表示缓存目标缺少回源加载器。
	AdminCodeLoaderRequired = "loader_required"
	// AdminCodeWaitTimeout 表示等待缓存重建超时。
	AdminCodeWaitTimeout = "wait_timeout"
	// AdminCodeRefreshUnavailable 表示当前刷新结果不可继续使用。
	AdminCodeRefreshUnavailable = "refresh_unavailable"
	// AdminCodeCacheMiss 表示缓存最终仍未命中。
	AdminCodeCacheMiss = "cache_miss"
	// AdminCodeDataNotFound 表示源数据不存在。
	AdminCodeDataNotFound = "data_not_found"
	// AdminCodeInvalidKey 表示缓存 key 或前缀不符合约束。
	AdminCodeInvalidKey = "invalid_key"
	// AdminCodeScanDisabled 表示当前禁止前缀删除降级扫描。
	AdminCodeScanDisabled = "scan_fallback_disabled"
	// AdminCodeCollectionTooLarge 表示缓存集合超过安全上限。
	AdminCodeCollectionTooLarge = "collection_too_large"
	// AdminCodeInternalError 表示未公开内部细节的执行错误。
	AdminCodeInternalError = "internal_error"
)

// LoadThroughBatchAdminItem 表示管理页可直接返回的单条批量读穿结果。
type LoadThroughBatchAdminItem struct {
	Key       string      `json:"key"`       // Key 是当前批量项的缓存 key
	Success   bool        `json:"success"`   // Success 表示当前批量项是否执行成功
	Code      string      `json:"code"`      // Code 是稳定且不包含内部细节的结果码
	Message   string      `json:"message"`   // Message 是可安全展示的结果文案
	State     LookupState `json:"state"`     // State 是当前批量项最终命中状态
	Refreshed bool        `json:"refreshed"` // Refreshed 表示当前批量项是否参与了执行、等待或复用刷新结果
	Error     error       `json:"-"`         // Error 保留原始错误，供调用方在受控日志中使用
}

// LoadThroughBatchAdminResponse 表示管理页批量读穿接口可直接返回的标准 JSON 结构。
type LoadThroughBatchAdminResponse struct {
	Success bool                        `json:"success"` // Success 表示整批是否全部成功
	Code    string                      `json:"code"`    // Code 是稳定且不包含内部细节的结果码
	Message string                      `json:"message"` // Message 是面向管理页展示的安全文案
	Error   error                       `json:"-"`       // Error 保留整批聚合错误，供调用方在受控日志中使用
	Summary LoadThroughBatchSummary     `json:"summary"` // Summary 是当前批量结果的汇总统计
	Items   []LoadThroughBatchAdminItem `json:"items"`   // Items 是当前批量每一项的明细结果
}

// BuildLoadThroughBatchAdminResponse 根据批量结果构建管理页标准响应结构。
func BuildLoadThroughBatchAdminResponse(results []LoadThroughBatchResult) LoadThroughBatchAdminResponse {
	summary := SummarizeLoadThroughBatchResults(results)
	return BuildLoadThroughBatchAdminResponseWithSummary(results, summary, nil)
}

// BuildLoadThroughBatchAdminResponseWithSummary 根据批量结果、汇总信息和聚合错误构建管理页标准响应结构。
func BuildLoadThroughBatchAdminResponseWithSummary(results []LoadThroughBatchResult, summary LoadThroughBatchSummary, err error) LoadThroughBatchAdminResponse {
	// 有逐项结果时以逐项结果重新汇总，避免调用方传入过期 summary 后响应状态互相矛盾。
	if len(results) > 0 {
		summary = SummarizeLoadThroughBatchResults(results)
	}
	items := make([]LoadThroughBatchAdminItem, 0, len(results))
	for _, result := range results {
		code, message := adminErrorDetails(result.Error)
		items = append(items, LoadThroughBatchAdminItem{
			Key:       result.Key,
			Success:   result.Error == nil,
			Code:      code,
			Message:   message,
			State:     result.LookupResult.State,
			Refreshed: result.LookupResult.Refreshed,
			Error:     result.Error,
		})
	}
	success, code, message := adminBatchResult(
		summary.HasError(),
		err,
		"批量读穿执行成功",
		"批量读穿存在失败项",
	)
	return LoadThroughBatchAdminResponse{
		Success: success,
		Code:    code,
		Message: message,
		Error:   err,
		Summary: summary,
		Items:   items,
	}
}

// adminBatchResult 返回与批量执行结果一致的成功标记、结果码和安全文案。
func adminBatchResult(hasFailedItem bool, err error, successMessage string, partialMessage string) (bool, string, string) {
	if hasFailedItem {
		return false, AdminCodePartialFailure, partialMessage
	}
	if err != nil {
		code, message := adminErrorDetails(err)
		return false, code, message
	}
	return true, AdminCodeOK, successMessage
}

// adminErrorDetails 把内部错误映射为稳定结果码和安全文案，禁止把错误链原文输出到 JSON。
func adminErrorDetails(err error) (string, string) {
	switch {
	case err == nil:
		return AdminCodeOK, "执行成功"
	case errors.Is(err, ErrTargetNotFound):
		return AdminCodeTargetNotFound, "缓存目标不存在"
	case errors.Is(err, ErrLoaderRequired):
		return AdminCodeLoaderRequired, "缓存目标未配置加载器"
	case errors.Is(err, ErrWaitRebuildTimeout):
		return AdminCodeWaitTimeout, "等待缓存重建超时"
	case errors.Is(err, ErrRefreshLockLost), errors.Is(err, ErrRefreshInvalidated):
		return AdminCodeRefreshUnavailable, "缓存刷新结果已失效"
	case errors.Is(err, ErrCacheMiss):
		return AdminCodeCacheMiss, "缓存未命中"
	case errors.Is(err, ErrNotFound):
		return AdminCodeDataNotFound, "数据不存在"
	case errors.Is(err, ErrKeyPrefixRequired), errors.Is(err, ErrInvalidClusterHashTag), errors.Is(err, ErrEntryKeyOutOfScope), errors.Is(err, ErrInvalidKeyPrefix):
		return AdminCodeInvalidKey, "缓存 key 不符合要求"
	case errors.Is(err, ErrScanFallbackDisabled):
		return AdminCodeScanDisabled, "当前禁止降级扫描"
	case errors.Is(err, ErrCollectionTooLarge):
		return AdminCodeCollectionTooLarge, "缓存集合超过安全上限"
	case errors.Is(err, ErrInvalidConfig), errors.Is(err, ErrPartialHashUnsupported):
		return AdminCodeInvalidRequest, "请求不符合缓存操作要求"
	case errors.Is(err, ErrPrefixReplaceUnsupported), errors.Is(err, ErrPrefixDeleteUnsupported), errors.Is(err, ErrMarkerReplaceUnsupported), errors.Is(err, ErrDistributedPrefixHashTagRequired), errors.Is(err, ErrRedisTopologyUnsupported):
		return AdminCodeRefreshUnavailable, "当前缓存刷新不可用"
	default:
		return AdminCodeInternalError, "操作失败"
	}
}
