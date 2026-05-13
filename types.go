package tablecache

import (
	"context"
	"time"
)

const (
	// TypeString 表示 Redis String 缓存类型。
	TypeString CacheType = "string"
	// TypeHash 表示 Redis Hash 缓存类型。
	TypeHash CacheType = "hash"
	// TypeList 表示 Redis List 缓存类型。
	TypeList CacheType = "list"
	// TypeSet 表示 Redis Set 缓存类型。
	TypeSet CacheType = "set"
	// TypeZSet 表示 Redis ZSet 缓存类型。
	TypeZSet CacheType = "zset"
)

const (
	// DefaultEmptyMarker 表示空值缓存占位符，用于缓存穿透保护。
	DefaultEmptyMarker = "__empty__"
)

const (
	// RebuildContextInheritCancel 表示重建上下文继承调用方取消信号。
	RebuildContextInheritCancel RebuildContextPolicy = iota
	// RebuildContextIgnoreCancel 表示重建上下文保留调用方上下文值，但忽略调用方取消信号。
	RebuildContextIgnoreCancel
)

const (
	// LookupStateHit 表示缓存命中且已成功写入目标对象。
	LookupStateHit LookupState = "hit"
	// LookupStateMiss 表示缓存未命中，且当前没有空值占位。
	LookupStateMiss LookupState = "miss"
	// LookupStateEmpty 表示缓存已确认源数据为空，命中了空值占位。
	LookupStateEmpty LookupState = "empty"
)

// CacheType 表示 Redis 目标数据结构类型。
type CacheType string

// RebuildContextPolicy 表示缓存重建上下文的取消信号策略。
type RebuildContextPolicy int

// LookupState 表示缓存读取状态。
type LookupState string

// Loader 表示缓存目标的数据加载函数，由业务项目自行适配数据库、RPC 或其它数据源。
type Loader func(ctx context.Context, params LoadParams) ([]Entry, error)

// Target 描述一个可刷新、可展示、可复用的表数据缓存目标。
type Target struct {
	Index            string        // Index 是缓存目标索引，通常等于 Redis key 第一个冒号前的前缀
	Title            string        // Title 是缓存目标中文名称
	Key              string        // Key 是 Redis key 或 key 前缀，前缀型目标以冒号结尾
	KeyTitle         string        // KeyTitle 是缓存管理页展示用 key 模板
	Type             CacheType     // Type 是 Redis 数据结构类型
	Remark           string        // Remark 是缓存用途说明
	TTL              time.Duration // TTL 是业务缓存基础过期时间，0 表示不过期
	Jitter           time.Duration // Jitter 是 TTL 最大抖动范围，用于降低缓存雪崩风险
	EmptyTTL         time.Duration // EmptyTTL 是空值占位缓存过期时间，用于缓存穿透保护
	LoaderTimeout    time.Duration // LoaderTimeout 是当前目标回源加载超时时间，0 表示使用管理器默认值
	RefreshAll       bool          // RefreshAll 表示刷新全部缓存时是否包含该目标
	AllowEmptyMarker bool          // AllowEmptyMarker 表示加载空数据时是否写入空值占位
	VisibleEmptyMark bool          // VisibleEmptyMark 表示把空值占位直接写入业务 key；默认只写独立元信息
	Loader           Loader        // Loader 是缓存回源加载器
}

// LoadParams 表示缓存目标加载时的上下文参数。
type LoadParams struct {
	Target   Target   // Target 是当前命中的缓存目标配置
	Key      string   // Key 是本次请求刷新或读取的 Redis key
	Index    string   // Index 是缓存目标索引
	KeyParts []string // KeyParts 是去掉目标前缀后的 key 片段
	Fields   []string // Fields 是 Hash 等二级索引字段，用于局部刷新
}

// LookupResult 表示一次缓存读取或“读取后按需刷新”的结果。
type LookupResult struct {
	State     LookupState `json:"state"`     // State 是本次读取最终状态
	Refreshed bool        `json:"refreshed"` // Refreshed 表示本次流程中是否执行了回源刷新
}

// LoadThroughOptions 表示单次读穿缓存调用的可选参数。
type LoadThroughOptions struct {
	Fields           []string              // Fields 是本次读穿时透传给 Loader 的字段列表，适用于 Hash 等局部刷新场景
	Loader           Loader                // Loader 是本次调用临时覆盖的回源加载器，nil 表示继续使用 Target.Loader
	LoaderTimeout    time.Duration         // LoaderTimeout 是本次读穿回源超时时间，优先级高于 Target.LoaderTimeout
	AllowEmptyMarker *bool                 // AllowEmptyMarker 控制本次 miss 回源是否允许写入空值占位，nil 表示沿用目标配置
	ContextPolicy    *RebuildContextPolicy // ContextPolicy 控制本次读穿是否忽略调用方取消信号，nil 表示沿用管理器配置
}

// LoadThroughItem 表示一条批量读穿请求。
type LoadThroughItem struct {
	Key     string             // Key 是本次批量读穿的目标缓存 key
	Dest    any                // Dest 是当前 key 的反序列化目标对象
	Options LoadThroughOptions // Options 是当前 key 的读穿可选参数
}

// LoadThroughBatchOptions 表示一批读穿请求的批次级配置。
type LoadThroughBatchOptions struct {
	Concurrency    int                // Concurrency 是本批次读穿的并发度，0 表示沿用 Manager 默认配置
	DefaultOptions LoadThroughOptions // DefaultOptions 是本批次所有条目的默认读穿参数，条目级 Options 优先级更高
}

// LoadThroughBatchResult 表示单条批量读穿请求的执行结果。
type LoadThroughBatchResult struct {
	Key          string       `json:"key"`          // Key 是当前批量项的缓存 key
	LookupResult LookupResult `json:"lookupResult"` // LookupResult 是当前批量项的读取结果
	Error        error        `json:"-"`            // Error 是当前批量项的执行错误
}

// LoadThroughBatchSummary 表示一批读穿请求的汇总结果。
type LoadThroughBatchSummary struct {
	Total      int      `json:"total"`      // Total 是批量请求总数
	Success    int      `json:"success"`    // Success 是执行成功的条数
	Failed     int      `json:"failed"`     // Failed 是执行失败的条数
	Hit        int      `json:"hit"`        // Hit 是最终命中缓存数据的条数
	Miss       int      `json:"miss"`       // Miss 是最终仍未命中的条数
	Empty      int      `json:"empty"`      // Empty 是命中空值占位的条数
	Refreshed  int      `json:"refreshed"`  // Refreshed 是执行过程中触发回源刷新的条数
	FailedKeys []string `json:"failedKeys"` // FailedKeys 是执行失败的 key 列表
}

// LoadThroughBatchError 表示批量读穿存在失败项的聚合错误。
type LoadThroughBatchError struct {
	Summary LoadThroughBatchSummary // Summary 是当前批量结果的汇总信息
}

// Error 返回批量读穿聚合错误描述。
func (e *LoadThroughBatchError) Error() string {
	return "tablecache批量读穿存在失败项"
}

// HasError 返回当前批量汇总是否包含失败项。
func (s LoadThroughBatchSummary) HasError() bool {
	return s.Failed > 0
}

// RefreshBatchResult 表示单条批量刷新请求的执行结果。
type RefreshBatchResult struct {
	Key   string `json:"key"` // Key 是当前批量项的缓存 key
	Error error  `json:"-"`   // Error 是当前批量项的执行错误
}

// RefreshBatchSummary 表示一批刷新请求的汇总结果。
type RefreshBatchSummary struct {
	Total      int      `json:"total"`      // Total 是批量请求总数
	Success    int      `json:"success"`    // Success 是执行成功的条数
	Failed     int      `json:"failed"`     // Failed 是执行失败的条数
	FailedKeys []string `json:"failedKeys"` // FailedKeys 是执行失败的 key 列表
}

// RefreshBatchError 表示批量刷新存在失败项的聚合错误。
type RefreshBatchError struct {
	Summary RefreshBatchSummary // Summary 是当前批量刷新结果的汇总信息
}

// Error 返回批量刷新聚合错误描述。
func (e *RefreshBatchError) Error() string {
	return "tablecache批量刷新存在失败项"
}

// HasError 返回当前批量刷新汇总是否包含失败项。
func (s RefreshBatchSummary) HasError() bool {
	return s.Failed > 0
}

// SummarizeLoadThroughBatchResults 汇总批量读穿结果，便于任务调度和批处理统一判断。
func SummarizeLoadThroughBatchResults(results []LoadThroughBatchResult) LoadThroughBatchSummary {
	summary := LoadThroughBatchSummary{
		Total:      len(results),
		FailedKeys: make([]string, 0),
	}
	for _, result := range results {
		if result.Error != nil {
			summary.Failed++
			summary.FailedKeys = append(summary.FailedKeys, result.Key)
			continue
		}
		summary.Success++
		if result.LookupResult.Refreshed {
			summary.Refreshed++
		}
		switch result.LookupResult.State {
		case LookupStateHit:
			summary.Hit++
		case LookupStateMiss:
			summary.Miss++
		case LookupStateEmpty:
			summary.Empty++
		}
	}
	return summary
}

// SummarizeRefreshBatchResults 汇总批量刷新结果，便于管理页和预热任务统一判断。
func SummarizeRefreshBatchResults(results []RefreshBatchResult) RefreshBatchSummary {
	summary := RefreshBatchSummary{
		Total:      len(results),
		FailedKeys: make([]string, 0),
	}
	for _, result := range results {
		if result.Error != nil {
			summary.Failed++
			summary.FailedKeys = append(summary.FailedKeys, result.Key)
			continue
		}
		summary.Success++
	}
	return summary
}

// Entry 表示一次写入 Redis 的标准缓存数据。
type Entry struct {
	Key       string        // Key 是最终写入的 Redis key
	Type      CacheType     // Type 是最终写入的 Redis 数据结构类型
	Value     any           // Value 是写入值，类型由 Type 决定
	TTL       time.Duration // TTL 是当前 key 的基础过期时间
	Jitter    time.Duration // Jitter 是当前 key 的过期时间抖动范围
	Overwrite *bool         // Overwrite 控制写入前是否先删除旧 key；nil 表示默认覆盖写入
}

// ZMember 表示 Redis ZSet 成员与分数。
type ZMember struct {
	Member any     // Member 是有序集合成员
	Score  float64 // Score 是有序集合分数
}

// Bool 返回布尔指针，便于 Entry.Overwrite 显式设置 false。
func Bool(value bool) *bool {
	return &value
}

// RebuildPolicy 返回重建上下文策略指针，便于 LoadThroughOptions 显式设置单次调用策略。
func RebuildPolicy(value RebuildContextPolicy) *RebuildContextPolicy {
	return &value
}

// Item 表示缓存管理页可展示的缓存目标。
type Item struct {
	Index    string `json:"index"`    // Index 是缓存目标索引
	Key      string `json:"key"`      // Key 是 Redis key 或 key 模板
	KeyTitle string `json:"keyTitle"` // KeyTitle 是 Redis key 展示标题
	Type     string `json:"type"`     // Type 是 Redis 数据类型
	Remark   string `json:"remark"`   // Remark 是缓存说明
}

// item 把缓存目标转换成缓存管理页展示项。
func (t Target) item() Item {
	keyTitle := t.KeyTitle
	if keyTitle == "" {
		keyTitle = t.Key
	}
	return Item{
		Index:    t.Index,
		Key:      keyTitle,
		KeyTitle: keyTitle,
		Type:     string(t.Type),
		Remark:   t.Remark,
	}
}
