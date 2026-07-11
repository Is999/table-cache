package tablecache

// RefreshBatchAdminItem 表示管理页可直接返回的单条批量刷新结果。
type RefreshBatchAdminItem struct {
	Key     string `json:"key"`     // Key 是当前批量项的缓存 key
	Success bool   `json:"success"` // Success 表示当前批量项是否刷新成功
	Code    string `json:"code"`    // Code 是稳定且不包含内部细节的结果码
	Message string `json:"message"` // Message 是可安全展示的结果文案
	Error   error  `json:"-"`       // Error 保留原始错误，供调用方在受控日志中使用
}

// RefreshBatchAdminResponse 表示管理页批量刷新接口可直接返回的标准 JSON 结构。
type RefreshBatchAdminResponse struct {
	Success bool                    `json:"success"` // Success 表示整批是否全部成功
	Code    string                  `json:"code"`    // Code 是稳定且不包含内部细节的结果码
	Message string                  `json:"message"` // Message 是面向管理页展示的安全文案
	Error   error                   `json:"-"`       // Error 保留整批聚合错误，供调用方在受控日志中使用
	Summary RefreshBatchSummary     `json:"summary"` // Summary 是当前批量结果的汇总统计
	Items   []RefreshBatchAdminItem `json:"items"`   // Items 是当前批量每一项的明细结果
}

// BuildRefreshBatchAdminResponse 根据批量刷新结果构建管理页标准响应结构。
func BuildRefreshBatchAdminResponse(results []RefreshBatchResult) RefreshBatchAdminResponse {
	summary := SummarizeRefreshBatchResults(results)
	return BuildRefreshBatchAdminResponseWithSummary(results, summary, nil)
}

// BuildRefreshBatchAdminResponseWithSummary 根据批量刷新结果、汇总信息和聚合错误构建管理页标准响应结构。
func BuildRefreshBatchAdminResponseWithSummary(results []RefreshBatchResult, summary RefreshBatchSummary, err error) RefreshBatchAdminResponse {
	// 有逐项结果时以逐项结果重新汇总，避免调用方传入过期 summary 后响应状态互相矛盾。
	if len(results) > 0 {
		summary = SummarizeRefreshBatchResults(results)
	}
	items := make([]RefreshBatchAdminItem, 0, len(results))
	for _, result := range results {
		code, message := adminErrorDetails(result.Error)
		items = append(items, RefreshBatchAdminItem{
			Key:     result.Key,
			Success: result.Error == nil,
			Code:    code,
			Message: message,
			Error:   result.Error,
		})
	}
	success, code, message := adminBatchResult(
		summary.HasError(),
		err,
		"批量刷新执行成功",
		"批量刷新存在失败项",
	)
	return RefreshBatchAdminResponse{
		Success: success,
		Code:    code,
		Message: message,
		Error:   err,
		Summary: summary,
		Items:   items,
	}
}
