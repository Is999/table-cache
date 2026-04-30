package tablecache

// RefreshBatchAdminItem 表示管理页可直接返回的单条批量刷新结果。
type RefreshBatchAdminItem struct {
	Key     string `json:"key"`             // Key 是当前批量项的缓存 key
	Success bool   `json:"success"`         // Success 表示当前批量项是否刷新成功
	Error   string `json:"error,omitempty"` // Error 是当前批量项的错误描述
}

// RefreshBatchAdminResponse 表示管理页批量刷新接口可直接返回的标准 JSON 结构。
type RefreshBatchAdminResponse struct {
	Success bool                    `json:"success"` // Success 表示整批是否全部成功
	Message string                  `json:"message"` // Message 是面向管理页展示的批量执行说明
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
	if summary.Total == 0 && len(results) > 0 {
		summary = SummarizeRefreshBatchResults(results)
	}
	items := make([]RefreshBatchAdminItem, 0, len(results))
	for _, result := range results {
		item := RefreshBatchAdminItem{
			Key:     result.Key,
			Success: result.Error == nil,
		}
		if result.Error != nil {
			item.Error = result.Error.Error()
		}
		items = append(items, item)
	}
	message := "批量刷新执行成功"
	if summary.HasError() {
		message = "批量刷新存在失败项"
	}
	if err != nil {
		message = err.Error()
	}
	return RefreshBatchAdminResponse{
		Success: !summary.HasError(),
		Message: message,
		Summary: summary,
		Items:   items,
	}
}
