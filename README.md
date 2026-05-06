# tablecache

`pkg/tablecache` 是一个框架无关的表数据缓存管理扩展包，把“缓存目标声明、数据回源、Redis 写入、刷新锁、TTL 抖动、空值占位”收口到统一组件中。

## 设计要点

- `Target` 只描述缓存元信息和加载函数，不绑定 go-zero、Gin、Kratos、GORM 或具体数据表。
- `Store` 是缓存存储抽象，当前提供 `NewRedisStore` 适配 `go-redis`，其它框架可按接口自行适配。
- `RefreshByKey` 支持固定 key 与前缀型 key，例如 `role_status`、`config_uuid:{uuid}`。
- `RefreshByKeys` 支持批量刷新多个 key，并自动去重。
- `RefreshAll` 只刷新显式标记 `RefreshAll` 的目标，避免批量误刷参数化大 key，并支持有限并发控制。
- `SetNX` 重建锁避免热点 key 缓存击穿。
- 长耗时 Loader 运行期间会自动续期重建锁，降低锁过期后被其它实例重复重建的概率。
- 锁值使用随机持有者标识，并通过 Lua 脚本只释放自己的锁，避免超时后误删其它实例新锁。
- 同一进程内相同 key 的刷新请求会通过 `singleflight` 合并，减少突发流量下的 Redis 锁请求。
- 重建成功后会写入短 TTL 结果元信息，避免“空结果但无业务 key”时等待方误判超时。
- 多条 `Entry` 通过 `WriteBatch` 走 Redis Pipeline 批量写入，降低全量重建的网络 RTT。
- `TTL + Jitter` 降低同类 key 集中失效导致的缓存雪崩。
- `AllowEmptyMarker + EmptyTTL` 对不存在的数据写入短 TTL 空值占位，减少缓存穿透；默认写入独立元信息 key，避免与真实业务值碰撞，兼容旧行为时可开启 `VisibleEmptyMark`。
- `Get` 提供统一读取和反序列化入口；`GetState` 可明确区分 `hit/miss/empty`；`GetOrRefresh`、`GetOrRefreshWithLoader`、`GetOrRefreshWithOptions`、`LoadThrough`、`LoadThroughWithOptions` 可封装“缓存读取 + 回源 + 回填 + 返回”的读穿闭环。
- `LookupMetrics` 可补充 `lookup_state_hit/miss/empty` 与 `lookup_refresh_triggered` 等细分读取指标；`ExtendedMetrics` 还可补充 `prefix_wait`、`prefix_retry` 等前缀刷新排障指标。
- `DeleteByKey`、`DeleteByPrefix` 只允许操作缓存管理器中已注册的目标 key 或前缀，并同步清理隐藏空值元信息和短 TTL 重建结果元信息，避免误删任意 Redis key。
- Loader 返回的 `Entry.Key` 会在写回前校验作用域：固定目标只能写固定 key，前缀目标只能写注册前缀内 key，避免加载器误写非托管 Redis key。
- Hash/List/Set/ZSet 返回真实空集合时会写入隐藏空集合元信息，使后续 `GetState` 明确返回 `hit`，避免把“空集合”误判成 `miss` 并反复回源。
- `Entry.Overwrite` 可控制写入前是否先删除旧 key，默认覆盖写入；显式 `tablecache.Bool(false)` 时按 Redis 结构增量更新。
- `LoaderTimeout` 与 `WithRebuildContextPolicy(...)` 可限制单次回源时长，并控制是否继承调用方取消信号。
- 前缀刷新采用分层互斥：前缀全量刷新独占前缀锁，同前缀单 key 刷新使用 key 级锁；若遇到全量刷新，会先等待，必要时重试，兼顾一致性和并发度。
- 内置事件日志统一采用 `component=tablecache event=... index=... key=...` 风格，便于 gozero-admin 按字段检索 `lock_lost`、`prefix_wait`、`prefix_retry` 等场景。
- 启动时会校验重复 `Index`、重复 `Key` 与重叠前缀目标，避免线上匹配歧义。

## 接入方式

业务项目只需要：

1. 实现或复用 `Store`，gozero-admin 使用 `NewRedisStore(redis.UniversalClient)`。
2. 声明 `[]Target`，在 `Loader` 中读取数据库并返回标准 `[]Entry`。
3. 在缓存管理接口或业务 miss 回源位置调用 `RefreshByKey` / `RefreshAll` / `RefreshByKeys`。
4. 在业务读取路径调用 `Get` / `GetState` / `GetOrRefresh` / `GetOrRefreshWithLoader` / `GetOrRefreshWithOptions` / `LoadThrough` / `LoadThroughWithOptions` / `LoadThroughBatch`，按是否需要显式传入 Loader、透传 fields 或批量读取选择接入方式。

这样同一套缓存目标可以被管理页、业务读取 miss 重建、定时预热任务复用。

## gozero-admin 接入示例

推荐做法是把 `go-utils` 的日志初始化放到项目入口，只配置一次，然后缓存管理器直接复用：

```go
package main

import (
	"log/slog"
	"os"
)

func initLogger() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(handler))
}
```

```go
package cacheexample

import (
	"context"
	"strconv"
	"time"

	utils "github.com/Is999/go-utils"
	"cron_admin/pkg/tablecache"

	"github.com/redis/go-redis/v9"
)

// User 表示业务用户缓存示例结构。
type User struct {
	ID   int    `json:"id"`   // ID 是用户 ID
	Name string `json:"name"` // Name 是用户名称
}

// UserModel 表示示例数据库访问对象。
type UserModel interface {
	FindOne(ctx context.Context, id int64) (*User, error)
}

// UserCache 定义 gozero-admin 中可复用的用户缓存组件。
type UserCache struct {
	manager *tablecache.Manager // manager 是用户资料缓存管理器
	model   UserModel           // model 是数据库查询适配器
}

// NewUserCache 创建用户缓存管理器。
func NewUserCache(client redis.UniversalClient, model UserModel) (*UserCache, error) {
	store := tablecache.NewRedisStore(client)
	metrics, err := tablecache.NewPrometheusMetrics(
		tablecache.WithPrometheusNamespace("gozero_admin"),
		tablecache.WithPrometheusSubsystem("tablecache"),
	)
	if err != nil {
		return nil, err
	}
	manager, err := tablecache.NewManager(store, []tablecache.Target{
		{
			Index:            "user_profile",
			Title:            "用户资料",
			Key:              "user_profile:",
			KeyTitle:         "user_profile:{userID}",
			Type:             tablecache.TypeString,
			TTL:              time.Hour,
			Jitter:           10 * time.Minute,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params tablecache.LoadParams) ([]tablecache.Entry, error) {
				if len(params.KeyParts) == 0 || model == nil {
					return nil, tablecache.ErrNotFound
				}
				userID, err := strconv.ParseInt(params.KeyParts[0], 10, 64)
				if err != nil {
					return nil, err
				}
				user, err := model.FindOne(ctx, userID)
				if err != nil {
					return nil, err
				}
				if user == nil {
					return nil, tablecache.ErrNotFound
				}
				return []tablecache.Entry{{
					Key:   params.Key,
					Type:  tablecache.TypeString,
					Value: user,
				}}, nil
			},
		},
	}, tablecache.WithMetrics(metrics), tablecache.WithGoUtilsLogger(utils.Log()))
	if err != nil {
		return nil, err
	}
	return &UserCache{
		manager: manager,
		model:   model,
	}, nil
}

// key 根据用户 ID 生成已注册的缓存 key。
func (c *UserCache) key(userID int64) string {
	return "user_profile:" + strconv.FormatInt(userID, 10)
}

// Manager 返回底层缓存管理器，供管理页和任务调度复用。
func (c *UserCache) Manager() *tablecache.Manager {
	return c.manager
}

// GetForBiz 在业务读取链路中直接执行读穿缓存。
func (c *UserCache) GetForBiz(ctx context.Context, userID int64) (*User, error) {
	var user User
	result, err := c.manager.LoadThrough(ctx, c.key(userID), &user, nil)
	if err != nil {
		return nil, err
	}
	if result.State == tablecache.LookupStateHit {
		return &user, nil
	}
	if result.State == tablecache.LookupStateEmpty {
		return nil, nil
	}
	return nil, nil
}

// GetForBizWithOptions 演示完整读穿参数的接入方式，便于业务在局部场景透传 fields 并自定义回源逻辑。
func (c *UserCache) GetForBizWithOptions(ctx context.Context, userID int64, fields []string) (*User, error) {
	var user User
	result, err := c.manager.LoadThroughWithOptions(ctx, c.key(userID), &user, tablecache.LoadThroughOptions{
		Fields: fields,
		Loader: func(ctx context.Context, params tablecache.LoadParams) ([]tablecache.Entry, error) {
			if c.model == nil {
				return nil, tablecache.ErrNotFound
			}
			dbUser, err := c.model.FindOne(ctx, userID)
			if err != nil {
				return nil, err
			}
			if dbUser == nil {
				return nil, tablecache.ErrNotFound
			}
			return []tablecache.Entry{{
				Key:   params.Key,
				Type:  tablecache.TypeString,
				Value: dbUser,
			}}, nil
		},
	})
	if err != nil {
		return nil, err
	}
	if result.State == tablecache.LookupStateHit {
		return &user, nil
	}
	return nil, nil
}

// RefreshFromAdmin 表示 gozero-admin 管理页“手动刷新单个缓存”的链路。
func (c *UserCache) RefreshFromAdmin(ctx context.Context, userID int64) error {
	return c.manager.RefreshByKey(ctx, c.key(userID))
}

// WarmupByTask 表示定时预热任务链路，支持批量预热多个注册 key。
func (c *UserCache) WarmupByTask(ctx context.Context, userIDs []int64) error {
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, c.key(userID))
	}
	return c.manager.RefreshByKeys(ctx, keys)
}

// WarmupByTaskWithSummary 表示预热任务链路可直接返回标准批量刷新响应。
func (c *UserCache) WarmupByTaskWithSummary(ctx context.Context, userIDs []int64) (tablecache.RefreshBatchAdminResponse, error) {
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, c.key(userID))
	}
	results, summary, err := c.manager.RefreshByKeysWithSummary(ctx, keys)
	return tablecache.BuildRefreshBatchAdminResponseWithSummary(results, summary, err), nil
}

// RefreshAllFromAdmin 表示管理页“全量刷新所有允许全量刷新的目标”时可直接返回标准响应。
func (c *UserCache) RefreshAllFromAdmin(ctx context.Context) (tablecache.RefreshBatchAdminResponse, error) {
	results, summary, err := c.manager.RefreshAllWithSummary(ctx)
	return tablecache.BuildRefreshBatchAdminResponseWithSummary(results, summary, err), nil
}

// LoadThroughBatchForAdmin 表示管理页批量读取缓存状态时可直接返回的标准 JSON 结构。
func (c *UserCache) LoadThroughBatchForAdmin(ctx context.Context, userIDs []int64) (tablecache.LoadThroughBatchAdminResponse, error) {
	items := make([]tablecache.LoadThroughItem, 0, len(userIDs))
	users := make([]User, len(userIDs))
	for index, userID := range userIDs {
		items = append(items, tablecache.LoadThroughItem{
			Key:  c.key(userID),
			Dest: &users[index],
		})
	}
	results, summary, err := c.manager.LoadThroughBatchWithSummaryOptions(ctx, items, tablecache.LoadThroughBatchOptions{
		Concurrency: 4,
		DefaultOptions: tablecache.LoadThroughOptions{
			LoaderTimeout:    300 * time.Millisecond,
			AllowEmptyMarker: tablecache.Bool(true),
		},
	})
	return tablecache.BuildLoadThroughBatchAdminResponseWithSummary(results, summary, err), nil
}
```

## gozero-admin 可复制模板

如果你希望直接按 `gozero-admin` 常见分层落地，可以按下面这套方式组织：

1. `main` 或启动入口只做一次 `slog.SetDefault(...)`；如果项目已自行实现 `utils.Logger`，也可以在这里调用 `utils.Configure(utils.WithLogger(customLogger))`。
2. `svc.ServiceContext` 统一创建 `redis.UniversalClient` 和业务缓存组件，并把 `manager.Items()` 缓存成管理页列表。
3. `logic` 或 `service` 层封装业务读穿、管理页刷新、批量预热。
4. `handler` 只负责参数解析并直接返回 `BuildRefreshBatchAdminResponse...` / `BuildLoadThroughBatchAdminResponse...` 生成的标准 JSON。

`svc/servicecontext.go` 示例：

```go
package svc

import (
	"your_project/internal/cache"
	"your_project/internal/config"

	"cron_admin/pkg/tablecache"
	"github.com/redis/go-redis/v9"
)

// ServiceContext 汇总项目运行时依赖。
type ServiceContext struct {
	Config    config.Config         // Config 是项目配置
	Redis     redis.UniversalClient // Redis 是全局 Redis 客户端
	Cache     *cache.UserCache      // Cache 是业务缓存组件
	TableList []tablecache.Item     // TableList 是缓存管理页展示列表
}

// NewServiceContext 创建服务上下文并集中初始化缓存组件。
func NewServiceContext(c config.Config) (*ServiceContext, error) {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    c.Redis.Addrs,
		Password: c.Redis.Password,
		DB:       c.Redis.DB,
	})
	userCache, err := cache.NewUserCache(client, nil)
	if err != nil {
		return nil, err
	}
	return &ServiceContext{
		Config:    c,
		Redis:     client,
		Cache:     userCache,
		TableList: userCache.Manager().Items(),
	}, nil
}
```

`logic` 或 `service` 层示例：

```go
package logic

import (
	"context"

	"cron_admin/pkg/tablecache"
	"your_project/internal/svc"
)

// CacheLogic 封装缓存管理页和业务侧共用逻辑。
type CacheLogic struct {
	svcCtx *svc.ServiceContext // svcCtx 是服务上下文
}

// NewCacheLogic 创建缓存逻辑对象。
func NewCacheLogic(svcCtx *svc.ServiceContext) *CacheLogic {
	return &CacheLogic{svcCtx: svcCtx}
}

// ListItems 返回管理页需要展示的缓存目标列表。
func (l *CacheLogic) ListItems(ctx context.Context) []tablecache.Item {
	_ = ctx
	return l.svcCtx.TableList
}

// RefreshUserFromAdmin 处理管理页“刷新单个用户缓存”动作。
func (l *CacheLogic) RefreshUserFromAdmin(ctx context.Context, userID int64) error {
	return l.svcCtx.Cache.RefreshFromAdmin(ctx, userID)
}

// WarmupUsers 处理定时任务或后台批量预热，并直接返回标准 JSON 结构。
func (l *CacheLogic) WarmupUsers(ctx context.Context, userIDs []int64) tablecache.RefreshBatchAdminResponse {
	response, err := l.svcCtx.Cache.WarmupByTaskWithSummary(ctx, userIDs)
	if err != nil {
		return tablecache.RefreshBatchAdminResponse{
			Success: false,
			Message: err.Error(),
		}
	}
	return response
}

// ReadUsersForAdmin 处理管理页批量读穿演示，便于直接查看 hit/miss/empty/refreshed。
func (l *CacheLogic) ReadUsersForAdmin(ctx context.Context, userIDs []int64) tablecache.LoadThroughBatchAdminResponse {
	response, err := l.svcCtx.Cache.LoadThroughBatchForAdmin(ctx, userIDs)
	if err != nil {
		return tablecache.LoadThroughBatchAdminResponse{
			Success: false,
			Message: err.Error(),
		}
	}
	return response
}
```

`handler` 示例：

```go
package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"your_project/internal/logic"
	"your_project/internal/svc"
)

// RegisterCacheRoutes 注册缓存管理相关接口。
func RegisterCacheRoutes(router *gin.RouterGroup, svcCtx *svc.ServiceContext) {
	cacheLogic := logic.NewCacheLogic(svcCtx)
	router.GET("/tablecache/items", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"items":   cacheLogic.ListItems(c.Request.Context()),
		})
	})
	router.POST("/tablecache/user/refresh", func(c *gin.Context) {
		var request struct {
			UserID int64 `json:"userId"` // UserID 是待刷新的用户 ID
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": err.Error()})
			return
		}
		if err := cacheLogic.RefreshUserFromAdmin(c.Request.Context(), request.UserID); err != nil {
			c.JSON(http.StatusOK, gin.H{"success": false, "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "刷新成功"})
	})
	router.POST("/tablecache/user/warmup", func(c *gin.Context) {
		var request struct {
			UserIDs []int64 `json:"userIds"` // UserIDs 是待预热的用户 ID 列表
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, cacheLogic.WarmupUsers(c.Request.Context(), request.UserIDs))
	})
	router.POST("/tablecache/user/read", func(c *gin.Context) {
		var request struct {
			UserIDs []int64 `json:"userIds"` // UserIDs 是待读取的用户 ID 列表
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, cacheLogic.ReadUsersForAdmin(c.Request.Context(), request.UserIDs))
	})
}
```

推荐理解方式：

- `manager.Items()` 负责给管理页渲染缓存目标清单。
- `RefreshByKey / RefreshByKeysWithSummary / RefreshAllWithSummary` 负责刷新、预热和后台批处理。
- `LoadThrough / LoadThroughWithOptions / LoadThroughBatchWithSummaryOptions` 负责业务读穿和管理页批量读缓存状态。
- `BuildRefreshBatchAdminResponseWithSummary(...)`、`BuildLoadThroughBatchAdminResponseWithSummary(...)` 负责把执行结果直接转成后台接口标准 JSON。

## 三条链路怎么串

1. 管理页刷新链路：
   gozero-admin 的缓存管理页拿到 `manager.Items()` 渲染列表，点击“刷新用户资料缓存”时调用 `RefreshFromAdmin(ctx, userID)`，最终进入 `RefreshByKey`，只允许刷新已注册 key。
2. 业务读取链路：
   业务接口直接调用 `GetForBiz`，内部走 `LoadThrough`，先读缓存，只有真实 miss 才回源、回填并返回；如果命中空值占位则直接返回空对象，避免反复穿透数据库。
3. 预热任务链路：
   定时任务、启动预热或后台批处理调用 `WarmupByTask`，通过 `RefreshByKeys` 对一组注册 key 做有限并发预热。

## 指标建议

项目已内置 `PrometheusMetrics`，直接实现了 `Metrics`、`ExtendedMetrics`、`LookupMetrics`：

```go
metrics, err := tablecache.NewPrometheusMetrics(
	tablecache.WithPrometheusNamespace("gozero_admin"),
	tablecache.WithPrometheusSubsystem("tablecache"),
)
if err != nil {
	return err
}

manager, err := tablecache.NewManager(store, targets, tablecache.WithMetrics(metrics))
if err != nil {
	return err
}
_ = manager
```

- `refresh_total`、`refresh_duration_seconds` 记录刷新总次数和耗时。
- `cache_hit_total`、`cache_miss_total`、`lock_failed_total` 记录基础命中与锁竞争情况。
- `loader_error_total`、`empty_marker_write_total`、`wait_timeout_total`、`prefix_delete_total`、`prefix_delete_keys_total`、`refresh_entry_count` 记录扩展运行指标。
- `prefix_wait_total`、`prefix_retry_total` 可用于观察是否有大前缀全量刷新拖慢单 key 刷新。
- `refresh_batch_total`、`refresh_batch_size`、`refresh_batch_success_items_total`、`refresh_batch_failed_items_total` 可用于观察批量刷新和全量刷新任务的执行次数、任务规模与成功失败条目数。
- `lookup_state_total{state="hit|miss|empty"}`、`lookup_refresh_triggered_total` 记录读穿读取链路指标。
- `refresh_total{result="lock_lost"}` 可直接观察锁续期失败导致的刷新中止次数。
- `WithPrometheusRegisterer(...)` 支持注入独立注册器，适合测试、多实例组件或避免全局注册器重复注册冲突。

## 日志建议

接入 `WithLogger(...)` 后，tablecache 会输出统一字段风格日志，便于 gozero-admin 在 Loki、ELK、Datadog 中直接检索：

- `event="lock_lost"`：锁续期失败后刷新被中止。
- `event="prefix_wait"`：单 key 刷新被同前缀全量刷新阻塞。
- `event="prefix_retry"`：单 key 在写回前发现前缀全量刷新已开始，因此主动重试。
- `event="lock_renew_failed"`：底层锁续期失败，通常会紧接着出现 `lock_lost`。
- `event="refresh_batch_done"`、`event="refresh_all_done"`：批量刷新与全量刷新任务完成时输出汇总结果。
- 推荐直接使用 `WithGoUtilsLogger(utils.Log())`；如果更喜欢统一入口，也可以继续使用 `WithLogger(utils.Log())`，两种写法都兼容。
- 如果业务入口已经通过 `slog.SetDefault(...)` 完成统一日志初始化，`utils.Log()` 默认会复用这套 `slog` 能力；如果项目已自行实现 `utils.Logger`，也可以继续走 `utils.Configure(utils.WithLogger(customLogger))`，同样无需额外 `logxadapter`。

## LoadThrough 参数建议

- `LoadThrough(ctx, key, dest, nil)` 适合绝大多数标准读穿场景，直接复用目标上注册的 `Loader`。
- `GetOrRefreshWithLoader` 适合快速补一个局部临时回源逻辑，但参数能力相对精简。
- `LoadThroughWithOptions` 适合生产场景做统一封装，可同时传入 `Fields`、临时 `Loader`、`LoaderTimeout`、`AllowEmptyMarker`、`ContextPolicy`，便于 Hash 字段级刷新、定制回源策略、按调用收紧超时、临时关闭空值占位或忽略上游取消。
- `LoadThroughBatch` 适合列表页、后台批处理或批量业务读场景；它会逐项返回结果，单条失败不会中断整批处理。
- `LoadThroughBatchWithBatchOptions` 适合整批共享默认参数的场景，可统一设置批次并发度和默认 `LoadThroughOptions`，条目级参数仍可覆盖。
- `LoadThroughBatchWithSummary` 适合任务调度、预热任务或管理页批处理；它会额外返回汇总信息和聚合错误，便于直接判断整批状态。
- `RefreshByKeysWithSummary`、`RefreshAllWithSummary` 适合预热任务和管理页刷新接口；它们会返回逐项结果、汇总信息和聚合错误，便于直接构造标准 JSON 响应。

示例：

```go
var user User
result, err := manager.LoadThroughWithOptions(ctx, "user_profile:1", &user, tablecache.LoadThroughOptions{
	Fields:           []string{"name", "status"},
	LoaderTimeout:    200 * time.Millisecond,
	AllowEmptyMarker: tablecache.Bool(false),
	ContextPolicy:    tablecache.RebuildPolicy(tablecache.RebuildContextIgnoreCancel),
})
if err != nil {
	return err
}
_ = result
```

批量示例：

```go
var user1 User
var user2 User
results := manager.LoadThroughBatch(ctx, []tablecache.LoadThroughItem{
	{Key: "user_profile:1", Dest: &user1},
	{Key: "user_profile:2", Dest: &user2},
})
for _, result := range results {
	if result.Error != nil {
		continue
	}
	_ = result.LookupResult
}
```

批次级默认参数示例：

```go
results := manager.LoadThroughBatchWithBatchOptions(ctx, []tablecache.LoadThroughItem{
	{Key: "user_profile:1", Dest: &user1},
	{Key: "user_profile:2", Dest: &user2},
}, tablecache.LoadThroughBatchOptions{
	Concurrency: 4,
	DefaultOptions: tablecache.LoadThroughOptions{
		LoaderTimeout:    200 * time.Millisecond,
		AllowEmptyMarker: tablecache.Bool(true),
		ContextPolicy:    tablecache.RebuildPolicy(tablecache.RebuildContextIgnoreCancel),
	},
})
_ = results
```

汇总示例：

```go
results, summary, err := manager.LoadThroughBatchWithSummary(ctx, []tablecache.LoadThroughItem{
	{Key: "user_profile:1", Dest: &user1},
	{Key: "user_profile:2", Dest: &user2},
})
if err != nil {
	return err
}
_ = results
_ = summary
```

管理页标准 JSON 示例：

```go
response, err := userCache.LoadThroughBatchForAdmin(ctx, []int64{1, 2, 3})
if err != nil {
	return err
}
return response
```

批量刷新标准 JSON 示例：

```go
response, err := userCache.WarmupByTaskWithSummary(ctx, []int64{1, 2, 3})
if err != nil {
	return err
}
return response
```

全量刷新标准 JSON 示例：

```go
response, err := userCache.RefreshAllFromAdmin(ctx)
if err != nil {
	return err
}
return response
```
