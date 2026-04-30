package tablecache

// LoadThroughBatchAdminItem 表示管理页可直接返回的单条批量读穿结果。
type LoadThroughBatchAdminItem struct {
	Key       string      `json:"key"`             // Key 是当前批量项的缓存 key
	Success   bool        `json:"success"`         // Success 表示当前批量项是否执行成功
	State     LookupState `json:"state"`           // State 是当前批量项最终命中状态
	Refreshed bool        `json:"refreshed"`       // Refreshed 表示当前批量项是否执行了回源刷新
	Error     string      `json:"error,omitempty"` // Error 是当前批量项的错误描述
}

// LoadThroughBatchAdminResponse 表示管理页批量读穿接口可直接返回的标准 JSON 结构。
type LoadThroughBatchAdminResponse struct {
	Success bool                        `json:"success"` // Success 表示整批是否全部成功
	Message string                      `json:"message"` // Message 是面向管理页展示的批量执行说明
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
	if summary.Total == 0 && len(results) > 0 {
		summary = SummarizeLoadThroughBatchResults(results)
	}
	items := make([]LoadThroughBatchAdminItem, 0, len(results))
	for _, result := range results {
		item := LoadThroughBatchAdminItem{
			Key:       result.Key,
			Success:   result.Error == nil,
			State:     result.LookupResult.State,
			Refreshed: result.LookupResult.Refreshed,
		}
		if result.Error != nil {
			item.Error = result.Error.Error()
		}
		items = append(items, item)
	}
	message := "批量读穿执行成功"
	if summary.HasError() {
		message = "批量读穿存在失败项"
	}
	if err != nil {
		message = err.Error()
	}
	return LoadThroughBatchAdminResponse{
		Success: !summary.HasError(),
		Message: message,
		Summary: summary,
		Items:   items,
	}
}
