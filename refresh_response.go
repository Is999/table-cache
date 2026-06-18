package tablecache

import (
	"encoding/json"

	"github.com/Is999/go-utils/errors"
)

// RefreshBatchAdminItem 表示管理页可直接返回的单条批量刷新结果。
type RefreshBatchAdminItem struct {
	Key     string `json:"key"`     // Key 是当前批量项的缓存 key
	Success bool   `json:"success"` // Success 表示当前批量项是否刷新成功
	Error   error  `json:"-"`       // Error 是当前批量项的原始错误，序列化时再转为展示文案
}

// RefreshBatchAdminResponse 表示管理页批量刷新接口可直接返回的标准 JSON 结构。
type RefreshBatchAdminResponse struct {
	Success bool                    `json:"success"` // Success 表示整批是否全部成功
	Message string                  `json:"message"` // Message 是面向管理页展示的批量执行说明
	Error   error                   `json:"-"`       // Error 是整批聚合错误，序列化时再转为展示文案
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
			item.Error = result.Error
		}
		items = append(items, item)
	}
	message := "批量刷新执行成功"
	if summary.HasError() {
		message = "批量刷新存在失败项"
	}
	var responseErr error
	if err != nil {
		responseErr = errors.Tag(err)
	}
	return RefreshBatchAdminResponse{
		Success: !summary.HasError(),
		Message: message,
		Error:   responseErr,
		Summary: summary,
		Items:   items,
	}
}

// MarshalJSON 在真正输出管理页 JSON 时再把 error 转成字符串。
func (item RefreshBatchAdminItem) MarshalJSON() ([]byte, error) {
	type adminItem struct {
		Key     string `json:"key"`             // Key 是当前批量项的缓存 key
		Success bool   `json:"success"`         // Success 表示当前项是否成功
		Error   string `json:"error,omitempty"` // Error 是输出给管理页的错误文案
	}
	errorText := ""
	if item.Error != nil {
		errorText = item.Error.Error()
	}
	body, err := json.Marshal(adminItem{
		Key:     item.Key,
		Success: item.Success,
		Error:   errorText,
	})
	return body, errors.Tag(err)
}

// MarshalJSON 在真正输出管理页 JSON 时再把聚合 error 转成 message。
func (response RefreshBatchAdminResponse) MarshalJSON() ([]byte, error) {
	type adminResponse struct {
		Success bool                    `json:"success"` // Success 表示整批是否成功
		Message string                  `json:"message"` // Message 是管理页展示文案
		Summary RefreshBatchSummary     `json:"summary"` // Summary 是批量刷新汇总统计
		Items   []RefreshBatchAdminItem `json:"items"`   // Items 是批量刷新明细
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
