package tablecache

import "testing"

// TestBuildLoadThroughBatchAdminResponse 验证批量读穿结果可转换为管理页标准响应结构。
func TestBuildLoadThroughBatchAdminResponse(t *testing.T) {
	response := BuildLoadThroughBatchAdminResponse([]LoadThroughBatchResult{
		{
			Key:          "user:1",
			LookupResult: LookupResult{State: LookupStateHit, Refreshed: true},
		},
		{
			Key:          "user:2",
			LookupResult: LookupResult{State: LookupStateEmpty},
		},
		{
			Key:   "user:3",
			Error: ErrTargetNotFound,
		},
	})
	if response.Success {
		t.Fatalf("response.Success = true, want false")
	}
	if response.Message != "批量读穿存在失败项" {
		t.Fatalf("response.Message = %q, want 批量读穿存在失败项", response.Message)
	}
	if response.Summary.Total != 3 || response.Summary.Success != 2 || response.Summary.Failed != 1 {
		t.Fatalf("response.Summary = %+v, want total=3 success=2 failed=1", response.Summary)
	}
	if len(response.Items) != 3 {
		t.Fatalf("response.Items len = %d, want 3", len(response.Items))
	}
	if !response.Items[0].Success || response.Items[0].State != LookupStateHit || !response.Items[0].Refreshed {
		t.Fatalf("response.Items[0] = %+v, want success hit refreshed", response.Items[0])
	}
	if response.Items[2].Success || response.Items[2].Error == "" {
		t.Fatalf("response.Items[2] = %+v, want failed with error", response.Items[2])
	}
}

// TestBuildLoadThroughBatchAdminResponseWithSummary 验证管理页响应可复用已有汇总和聚合错误。
func TestBuildLoadThroughBatchAdminResponseWithSummary(t *testing.T) {
	results := []LoadThroughBatchResult{
		{
			Key:          "user:1",
			LookupResult: LookupResult{State: LookupStateHit},
		},
	}
	summary := SummarizeLoadThroughBatchResults(results)
	response := BuildLoadThroughBatchAdminResponseWithSummary(results, summary, nil)
	if !response.Success {
		t.Fatalf("response.Success = false, want true")
	}
	if response.Message != "批量读穿执行成功" {
		t.Fatalf("response.Message = %q, want 批量读穿执行成功", response.Message)
	}
	if response.Summary.Total != 1 || len(response.Items) != 1 {
		t.Fatalf("response = %+v, want one success item", response)
	}
}
