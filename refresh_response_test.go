package tablecache

import "testing"

// TestBuildRefreshBatchAdminResponse 验证批量刷新结果可转换为管理页标准响应结构。
func TestBuildRefreshBatchAdminResponse(t *testing.T) {
	response := BuildRefreshBatchAdminResponse([]RefreshBatchResult{
		{Key: "user:1"},
		{Key: "user:2", Error: ErrTargetNotFound},
	})
	if response.Success {
		t.Fatalf("response.Success = true, want false")
	}
	if response.Message != "批量刷新存在失败项" {
		t.Fatalf("response.Message = %q, want 批量刷新存在失败项", response.Message)
	}
	if response.Summary.Total != 2 || response.Summary.Success != 1 || response.Summary.Failed != 1 {
		t.Fatalf("response.Summary = %+v, want total=2 success=1 failed=1", response.Summary)
	}
	if len(response.Items) != 2 {
		t.Fatalf("response.Items len = %d, want 2", len(response.Items))
	}
	if !response.Items[0].Success {
		t.Fatalf("response.Items[0] = %+v, want success", response.Items[0])
	}
	if response.Items[1].Success || response.Items[1].Error == "" {
		t.Fatalf("response.Items[1] = %+v, want failed with error", response.Items[1])
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
