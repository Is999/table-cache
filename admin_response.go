package tablecache

import (
	"encoding/json"

	"github.com/Is999/go-utils/errors"
)

// LoadThroughBatchAdminItem 表示管理页可直接返回的单条批量读穿结果。
type LoadThroughBatchAdminItem struct {
	Key       string      `json:"key"`       // Key 是当前批量项的缓存 key
	Success   bool        `json:"success"`   // Success 表示当前批量项是否执行成功
	State     LookupState `json:"state"`     // State 是当前批量项最终命中状态
	Refreshed bool        `json:"refreshed"` // Refreshed 表示当前批量项是否执行了回源刷新
	Error     error       `json:"-"`         // Error 是当前批量项的原始错误，序列化时再转为展示文案
}

// LoadThroughBatchAdminResponse 表示管理页批量读穿接口可直接返回的标准 JSON 结构。
type LoadThroughBatchAdminResponse struct {
	Success bool                        `json:"success"` // Success 表示整批是否全部成功
	Message string                      `json:"message"` // Message 是面向管理页展示的批量执行说明
	Error   error                       `json:"-"`       // Error 是整批聚合错误，序列化时再转为展示文案
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
			item.Error = result.Error
		}
		items = append(items, item)
	}
	message := "批量读穿执行成功"
	if summary.HasError() {
		message = "批量读穿存在失败项"
	}
	var responseErr error
	if err != nil {
		responseErr = errors.Tag(err)
	}
	return LoadThroughBatchAdminResponse{
		Success: !summary.HasError(),
		Message: message,
		Error:   responseErr,
		Summary: summary,
		Items:   items,
	}
}

// MarshalJSON 在真正输出管理页 JSON 时再把 error 转成字符串。
func (item LoadThroughBatchAdminItem) MarshalJSON() ([]byte, error) {
	type adminItem struct {
		Key       string      `json:"key"`             // Key 是当前批量项的缓存 key
		Success   bool        `json:"success"`         // Success 表示当前项是否成功
		State     LookupState `json:"state"`           // State 是当前项最终命中状态
		Refreshed bool        `json:"refreshed"`       // Refreshed 表示当前项是否触发回源
		Error     string      `json:"error,omitempty"` // Error 是输出给管理页的错误文案
	}
	errorText := ""
	if item.Error != nil {
		errorText = item.Error.Error()
	}
	body, err := json.Marshal(adminItem{
		Key:       item.Key,
		Success:   item.Success,
		State:     item.State,
		Refreshed: item.Refreshed,
		Error:     errorText,
	})
	return body, errors.Tag(err)
}

// MarshalJSON 在真正输出管理页 JSON 时再把聚合 error 转成 message。
func (response LoadThroughBatchAdminResponse) MarshalJSON() ([]byte, error) {
	type adminResponse struct {
		Success bool                        `json:"success"` // Success 表示整批是否成功
		Message string                      `json:"message"` // Message 是管理页展示文案
		Summary LoadThroughBatchSummary     `json:"summary"` // Summary 是批量读穿汇总统计
		Items   []LoadThroughBatchAdminItem `json:"items"`   // Items 是批量读穿明细
	}
	message := response.Message
	if response.Error != nil {
		message = response.Error.Error()
	}
	body, err := json.Marshal(adminResponse{
		Success: response.Success,
		Message: message,
		Summary: response.Summary,
		Items:   response.Items,
	})
	return body, errors.Tag(err)
}
