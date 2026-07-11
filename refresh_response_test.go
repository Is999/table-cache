package tablecache

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Is999/go-utils/errors"
)

// TestBuildRefreshBatchAdminResponse 验证批量刷新响应只输出稳定错误码和安全文案。
func TestBuildRefreshBatchAdminResponse(t *testing.T) {
	itemErr := errors.Wrapf(ErrTargetNotFound, "redis key=user:secret")
	response := BuildRefreshBatchAdminResponse([]RefreshBatchResult{
		{Key: "user:1"},
		{Key: "user:2", Error: itemErr},
	})
	if response.Success || response.Code != AdminCodePartialFailure || response.Message != "批量刷新存在失败项" {
		t.Fatalf("response status = success:%v code:%q message:%q, want partial failure", response.Success, response.Code, response.Message)
	}
	if response.Summary.Total != 2 || response.Summary.Success != 1 || response.Summary.Failed != 1 {
		t.Fatalf("response.Summary = %+v, want total=2 success=1 failed=1", response.Summary)
	}
	if len(response.Items) != 2 {
		t.Fatalf("response.Items len = %d, want 2", len(response.Items))
	}
	if !response.Items[0].Success || response.Items[0].Code != AdminCodeOK || response.Items[0].Message != "执行成功" {
		t.Fatalf("response.Items[0] = %+v, want success", response.Items[0])
	}
	failedItem := response.Items[1]
	if failedItem.Success || failedItem.Code != AdminCodeTargetNotFound || failedItem.Message != "缓存目标不存在" || !errors.Is(failedItem.Error, ErrTargetNotFound) {
		t.Fatalf("response.Items[1] = %+v, want safe target-not-found with raw error retained", failedItem)
	}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal(response) error = %v", err)
	}
	if strings.Contains(string(body), "user:secret") || strings.Contains(string(body), ErrTargetNotFound.Error()) {
		t.Fatalf("Marshal(response) leaked raw error: %s", string(body))
	}
	if strings.Contains(string(body), `"error":`) || !strings.Contains(string(body), `"code":"target_not_found"`) || !strings.Contains(string(body), `"message":"缓存目标不存在"`) {
		t.Fatalf("Marshal(response) = %s, want code/message without error field", string(body))
	}
}

// TestBuildRefreshBatchAdminResponseWithAggregateError 验证无失败项但存在聚合错误时不会返回成功状态。
func TestBuildRefreshBatchAdminResponseWithAggregateError(t *testing.T) {
	results := []RefreshBatchResult{{Key: "user:1"}}
	aggregateErr := errors.Errorf("database password=secret")
	response := BuildRefreshBatchAdminResponseWithSummary(results, RefreshBatchSummary{}, aggregateErr)
	if response.Success || response.Code != AdminCodeInternalError || response.Message != "操作失败" {
		t.Fatalf("response status = success:%v code:%q message:%q, want internal failure", response.Success, response.Code, response.Message)
	}
	if response.Error != aggregateErr {
		t.Fatalf("response.Error = %v, want original aggregate error", response.Error)
	}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal(response) error = %v", err)
	}
	if strings.Contains(string(body), "password=secret") {
		t.Fatalf("Marshal(response) leaked aggregate error: %s", string(body))
	}
}

// TestSummarizeRefreshBatchResults 验证批量刷新结果汇总函数可正确统计状态。
func TestSummarizeRefreshBatchResults(t *testing.T) {
	summary := SummarizeRefreshBatchResults([]RefreshBatchResult{
		{Key: "user:1"},
		{Key: "user:2", Error: ErrTargetNotFound},
		{Key: "user:3"},
	})
	if summary.Total != 3 || summary.Success != 2 || summary.Failed != 1 {
		t.Fatalf("summary = %+v, want total=3 success=2 failed=1", summary)
	}
	if !summary.HasError() {
		t.Fatalf("summary.HasError() = false, want true")
	}
	if len(summary.FailedKeys) != 1 || summary.FailedKeys[0] != "user:2" {
		t.Fatalf("summary.FailedKeys = %#v, want [user:2]", summary.FailedKeys)
	}
}
