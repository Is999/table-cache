package tablecache

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Is999/go-utils/errors"
)

// TestBuildLoadThroughBatchAdminResponse 验证批量读穿响应状态与逐项结果保持一致。
func TestBuildLoadThroughBatchAdminResponse(t *testing.T) {
	itemErr := errors.Wrapf(ErrTargetNotFound, "sql password=secret")
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
			Error: itemErr,
		},
	})
	if response.Success || response.Code != AdminCodePartialFailure || response.Message != "批量读穿存在失败项" {
		t.Fatalf("response status = success:%v code:%q message:%q, want partial failure", response.Success, response.Code, response.Message)
	}
	if response.Summary.Total != 3 || response.Summary.Success != 2 || response.Summary.Failed != 1 {
		t.Fatalf("response.Summary = %+v, want total=3 success=2 failed=1", response.Summary)
	}
	if len(response.Items) != 3 {
		t.Fatalf("response.Items len = %d, want 3", len(response.Items))
	}
	if !response.Items[0].Success || response.Items[0].Code != AdminCodeOK || response.Items[0].Message != "执行成功" || response.Items[0].State != LookupStateHit || !response.Items[0].Refreshed {
		t.Fatalf("response.Items[0] = %+v, want successful refreshed hit", response.Items[0])
	}
	failedItem := response.Items[2]
	if failedItem.Success || failedItem.Code != AdminCodeTargetNotFound || failedItem.Message != "缓存目标不存在" || !errors.Is(failedItem.Error, ErrTargetNotFound) {
		t.Fatalf("response.Items[2] = %+v, want safe target-not-found with raw error retained", failedItem)
	}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal(response) error = %v", err)
	}
	if strings.Contains(string(body), "secret") || strings.Contains(string(body), ErrTargetNotFound.Error()) {
		t.Fatalf("Marshal(response) leaked raw error: %s", string(body))
	}
	if strings.Contains(string(body), `"error":`) || !strings.Contains(string(body), `"code":"target_not_found"`) || !strings.Contains(string(body), `"message":"缓存目标不存在"`) {
		t.Fatalf("Marshal(response) = %s, want code/message without error field", string(body))
	}
}

// TestBuildLoadThroughBatchAdminResponseWithAggregateError 验证聚合错误会统一收敛成功标记和安全文案。
func TestBuildLoadThroughBatchAdminResponseWithAggregateError(t *testing.T) {
	results := []LoadThroughBatchResult{{
		Key:          "user:1",
		LookupResult: LookupResult{State: LookupStateHit},
	}}
	aggregateErr := errors.Errorf("database dsn=password-secret")
	response := BuildLoadThroughBatchAdminResponseWithSummary(results, LoadThroughBatchSummary{
		Total:  99,
		Failed: 99,
	}, aggregateErr)
	if response.Success || response.Code != AdminCodeInternalError || response.Message != "操作失败" {
		t.Fatalf("response status = success:%v code:%q message:%q, want internal failure", response.Success, response.Code, response.Message)
	}
	if response.Error != aggregateErr {
		t.Fatalf("response.Error = %v, want original aggregate error", response.Error)
	}
	if response.Summary.Total != 1 || response.Summary.Success != 1 || response.Summary.Failed != 0 {
		t.Fatalf("response.Summary = %+v, want summary rebuilt from results", response.Summary)
	}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal(response) error = %v", err)
	}
	if strings.Contains(string(body), "password-secret") || !strings.Contains(string(body), `"code":"internal_error"`) {
		t.Fatalf("Marshal(response) = %s, want safe internal error", string(body))
	}
}

// TestAdminErrorDetails 验证公开错误码稳定覆盖 tablecache 的可识别错误。
func TestAdminErrorDetails(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "success", code: AdminCodeOK},
		{name: "target", err: ErrTargetNotFound, code: AdminCodeTargetNotFound},
		{name: "loader", err: ErrLoaderRequired, code: AdminCodeLoaderRequired},
		{name: "wait", err: ErrWaitRebuildTimeout, code: AdminCodeWaitTimeout},
		{name: "lock lost", err: ErrRefreshLockLost, code: AdminCodeRefreshUnavailable},
		{name: "invalidated", err: ErrRefreshInvalidated, code: AdminCodeRefreshUnavailable},
		{name: "miss", err: ErrCacheMiss, code: AdminCodeCacheMiss},
		{name: "not found", err: ErrNotFound, code: AdminCodeDataNotFound},
		{name: "prefix required", err: ErrKeyPrefixRequired, code: AdminCodeInvalidKey},
		{name: "cluster hash tag", err: ErrInvalidClusterHashTag, code: AdminCodeInvalidKey},
		{name: "entry scope", err: ErrEntryKeyOutOfScope, code: AdminCodeInvalidKey},
		{name: "invalid prefix", err: ErrInvalidKeyPrefix, code: AdminCodeInvalidKey},
		{name: "scan disabled", err: ErrScanFallbackDisabled, code: AdminCodeScanDisabled},
		{name: "collection", err: ErrCollectionTooLarge, code: AdminCodeCollectionTooLarge},
		{name: "invalid config", err: ErrInvalidConfig, code: AdminCodeInvalidRequest},
		{name: "partial hash", err: ErrPartialHashUnsupported, code: AdminCodeInvalidRequest},
		{name: "prefix replace", err: ErrPrefixReplaceUnsupported, code: AdminCodeRefreshUnavailable},
		{name: "prefix delete", err: ErrPrefixDeleteUnsupported, code: AdminCodeRefreshUnavailable},
		{name: "redis topology", err: ErrRedisTopologyUnsupported, code: AdminCodeRefreshUnavailable},
		{name: "marker replace", err: ErrMarkerReplaceUnsupported, code: AdminCodeRefreshUnavailable},
		{name: "distributed prefix", err: ErrDistributedPrefixHashTagRequired, code: AdminCodeRefreshUnavailable},
		{name: "unknown", err: errors.Errorf("private detail"), code: AdminCodeInternalError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, message := adminErrorDetails(test.err)
			if code != test.code || strings.TrimSpace(message) == "" {
				t.Fatalf("adminErrorDetails() = (%q, %q), want code %q and safe message", code, message, test.code)
			}
		})
	}
}
