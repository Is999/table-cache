# tablecache

`tablecache` 是一个框架无关的 Redis 表数据缓存管理库。它把缓存目标声明、读穿回源、批量写入、刷新锁、TTL 抖动、空值占位、前缀删除和管理页响应结构收口到一套稳定组件里，适合管理后台、定时预热任务和业务读取链路共同复用。

## 特性概览

| 能力 | 说明 |
| --- | --- |
| 读穿缓存 | `LoadThrough` / `GetOrRefresh` 先读缓存，miss 后按注册的 `Loader` 回源并写回。 |
| 防击穿 | 同进程使用 `singleflight` 合并同 key 刷新，多实例使用 Redis `SET NX` 分布式锁。 |
| 防穿透 | `AllowEmptyMarker` 对不存在数据写短 TTL 空值占位，默认不污染业务 key。 |
| 防雪崩 | `TTL + Jitter` 分散过期时间；RedisStore 默认按 TTL 追加 10% 抖动。 |
| 安全删除 | `DeleteByKey` / `DeleteByPrefix` 只允许操作已注册 key，刷新锁和 delete epoch 阻断旧刷新写回。 |
| 大前缀治理 | 前缀 key 索引优先删除，索引不可用时才降级 `SCAN + UNLINK`。 |
| Redis Cluster | 支持单节点和 Cluster；Cluster 降级扫描按 master 节点执行。 |
| 集合治理 | 支持集合分页读取和集合读写上限，避免大 Hash/List/Set/ZSet 一次性拉爆内存。 |
| 管理页响应 | 内置批量刷新、批量读穿的标准响应结构和汇总结果。 |
| 可观测性 | 支持 Prometheus 指标、结构化事件日志和 key 脱敏。 |

## 版本要求

- Go 1.26+
- Redis 6.2+，推荐 Redis 7+
- 依赖 `go-redis`、Prometheus client、`go-utils` 等基础库

## 安装

```bash
go get github.com/Is999/table-cache
```

## 快速开始

下面示例展示一个用户资料缓存：key 前缀为 `a:`，业务 key 为 `a:user:1`。

```go
package cache

import (
	"context"
	"strconv"
	"time"

	tablecache "github.com/Is999/table-cache"
	"github.com/redis/go-redis/v9"
)

type User struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type UserModel interface {
	FindOne(ctx context.Context, id int64) (*User, error)
}

func NewUserCache(client redis.UniversalClient, model UserModel) (*tablecache.Manager, error) {
	store := tablecache.NewRedisStore(client)

	return tablecache.NewManager(store, []tablecache.Target{
		{
			Index:            "user",
			Title:            "用户资料",
			Key:              "user:",
			KeyTitle:         "user:{id}",
			Type:             tablecache.TypeString,
			TTL:              time.Hour,
			Jitter:           10 * time.Minute,
			AllowEmptyMarker: true,
			Loader: func(ctx context.Context, params tablecache.LoadParams) ([]tablecache.Entry, error) {
				id, err := strconv.ParseInt(params.KeyParts[0], 10, 64)
				if err != nil {
					return nil, err
				}
				user, err := model.FindOne(ctx, id)
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
	}, tablecache.WithKeyPrefix("a:"))
}
```

业务读取：

```go
var user User
result, err := manager.LoadThrough(ctx, "a:user:1", &user, nil)
if err != nil {
	return err
}
if result.State == tablecache.LookupStateHit {
	// user 来自缓存命中或 miss 后回源写回。
}
```

管理页刷新：

```go
if err := manager.RefreshByKey(ctx, "a:user:1"); err != nil {
	return err
}
```

批量预热：

```go
results, summary, err := manager.RefreshByKeysWithSummary(ctx, []string{
	"a:user:1",
	"a:user:2",
})
response := tablecache.BuildRefreshBatchAdminResponseWithSummary(results, summary, err)
_ = response
```

完整可运行示例见 [example/admin_integration](./example/admin_integration)。

## 核心概念

### Target

`Target` 描述一个缓存目标。固定 key 用完整 key，前缀型 key 以冒号结尾。

| 字段 | 说明 |
| --- | --- |
| `Index` | 稳定索引名，用于管理页、指标和日志。 |
| `Title` | 展示名称。 |
| `Key` | 逻辑 key 或逻辑前缀，例如 `user:`。 |
| `KeyTitle` | 展示模板，例如 `user:{id}`。 |
| `Type` | Redis 类型：String、Hash、List、Set、ZSet。 |
| `TTL` / `Jitter` | 基础过期时间和最大抖动。 |
| `EmptyTTL` | 空值占位过期时间。 |
| `LoaderTimeout` | 当前目标回源超时时间。 |
| `RefreshAll` | 是否允许被 `RefreshAll` 批量刷新。 |
| `AllowEmptyMarker` | 查无数据时是否写空值占位。 |
| `VisibleEmptyMark` | 是否把空值占位写进业务 key；默认写独立元信息。 |
| `Loader` | 回源函数，返回要写入 Redis 的 `Entry` 列表。 |

### Key 前缀

默认业务 key 前缀是 `tc:`。生产项目建议显式传入短项目前缀，例如 `WithKeyPrefix("a:")`。

- `Get` / `GetState` 支持传逻辑 key，会自动补前缀后只读查询。
- `RefreshByKey`、`RefreshByKeys`、`DeleteByKey`、`DeleteByPrefix`、`LoadThrough` 和 `GetOrRefresh` 的写回路径必须传实际 key。
- 未带前缀的 miss 请求只做只读查询，不会触发回源写入，避免误写非托管 key。
- 可开启 `WithStrictKeyPrefix(true)`，启动期拒绝空前缀、通配符和空白字符。

### Entry

`Entry` 是 Loader 返回的写入单元。`Entry.Key` 必须落在当前 Target 管理范围内：

- 固定目标只能写固定 key。
- 前缀目标只能写注册前缀内的 key。
- Hash/List/Set/ZSet 的空集合会写内部元信息，使后续读取明确返回 hit，而不是反复 miss。

## 并发与一致性

当多个请求同时读取一个不存在的缓存：

1. 所有请求先查缓存。
2. 同一进程内相同 key 通过 `singleflight` 合并。
3. 多实例通过 Redis `SET NX` 重建锁竞争，只有持锁方执行 Loader。
4. 未抢到锁的请求等待重建结果 marker，然后重新读缓存。
5. Loader 查无数据时可写空值占位，避免热点空数据持续穿透数据库。
6. 长 Loader 会自动续期锁；写业务 key、空值 marker、前缀清理和完成 marker 前都会复核锁 owner。

删除和刷新交错时，Manager 会写 delete epoch，并在刷新写回前校验 epoch。前缀全量刷新还会使用前缀锁和前缀 epoch，避免旧单 key 刷新覆盖新全量结果。

## Redis 配置建议

### 单节点

```go
client := redis.NewClient(&redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "",
	DB:       0,
	PoolSize: 20,
})
store := tablecache.NewRedisStore(client)
```

### Cluster

```go
client := redis.NewClusterClient(&redis.ClusterOptions{
	Addrs: []string{
		"127.0.0.1:7001",
		"127.0.0.1:7002",
		"127.0.0.1:7003",
	},
	Password: "",
	PoolSize: 20,
})
store := tablecache.NewRedisStore(client)
```

如果 Redis Cluster 返回的是容器 hostname，需要在业务 Redis 客户端层处理地址改写；真实集成测试可参考 `redis_cluster_integration_test.go` 的 `TABLECACHE_REDIS_ADDR_MAP`。

## 生产配置

推荐起步配置：

```go
store := tablecache.NewRedisStore(
	client,
	tablecache.WithUnlinkChunkSize(512),
	tablecache.WithScanUnlinkConcurrency(1),
	tablecache.WithMaxCollectionReadCount(5000),
	tablecache.WithMaxCollectionWriteCount(5000),
)

manager, err := tablecache.NewManager(
	store,
	targets,
	tablecache.WithKeyPrefix("a:"),
	tablecache.WithStrictKeyPrefix(true),
	tablecache.WithPrefixKeyIndex(true),
	tablecache.WithPrefixKeyIndexTTL(30*24*time.Hour),
	tablecache.WithScanFallback(false),
	tablecache.WithScanCount(2000),
	tablecache.WithPrefixDeleteConcurrency(1),
	tablecache.WithRefreshConcurrency(4),
	tablecache.WithLogKeyRedaction(true),
)
```

| 配置 | 建议 |
| --- | --- |
| `WithKeyPrefix` | 使用短项目前缀，例如 `a:`、`crm:`，避免 Redis key 过长。 |
| `WithStrictKeyPrefix` | 生产建议开启，避免空前缀或通配符误删。 |
| `WithPrefixKeyIndex` | 默认开启，前缀删除优先走索引，减少全库 SCAN。 |
| `WithScanFallback(false)` | 线上大 keyspace 建议关闭降级扫描，把索引缺失暴露为错误。 |
| `WithScanCount` | 常见取值 1000 到 5000；越大单轮 Redis 压力越高。 |
| `WithUnlinkChunkSize` | 常见取值 500 到 1000，控制 Pipeline 大小。 |
| `WithPrefixDeleteConcurrency` | 高峰期保持 1 或 2；后台维护窗口可适当提高。 |
| `WithMaxCollectionReadCount` | 为集合全量读取设置上限，超限返回 `ErrCollectionTooLarge`。 |
| `WithLogKeyRedaction` | 日志含用户 ID、订单 ID 等高基数字段时建议开启。 |

## API 选择

| 场景 | 推荐 API |
| --- | --- |
| 只读缓存 | `Get` / `GetState` |
| 业务读穿 | `LoadThrough` |
| 读穿并临时覆盖 Loader、超时或 fields | `LoadThroughWithOptions` |
| 批量读穿 | `LoadThroughBatchWithSummaryOptions` |
| 管理页刷新单 key | `RefreshByKey` |
| 定时任务批量预热 | `RefreshByKeysWithSummary` |
| 管理页全量刷新 | `RefreshAllWithSummary` |
| 精确删除 | `DeleteByKey` |
| 前缀删除 | `DeleteByPrefix` |
| 集合局部读取 | `CollectionPageStore.ReadPage` |

批量读穿示例：

```go
var user1 User
var user2 User

results, summary, err := manager.LoadThroughBatchWithSummaryOptions(ctx, []tablecache.LoadThroughItem{
	{Key: "a:user:1", Dest: &user1},
	{Key: "a:user:2", Dest: &user2},
}, tablecache.LoadThroughBatchOptions{
	Concurrency: 4,
	DefaultOptions: tablecache.LoadThroughOptions{
		LoaderTimeout:    200 * time.Millisecond,
		AllowEmptyMarker: tablecache.Bool(true),
		ContextPolicy:    tablecache.RebuildPolicy(tablecache.RebuildContextIgnoreCancel),
	},
})
response := tablecache.BuildLoadThroughBatchAdminResponseWithSummary(results, summary, err)
_ = response
```

## 指标

`PrometheusMetrics` 同时实现基础指标、扩展指标、读穿指标、批量刷新指标和扫描降级指标。

```go
metrics, err := tablecache.NewPrometheusMetrics(
	tablecache.WithPrometheusNamespace("admin"),
	tablecache.WithPrometheusSubsystem("tablecache"),
)
if err != nil {
	return err
}

manager, err := tablecache.NewManager(store, targets, tablecache.WithMetrics(metrics))
```

| 指标 | 说明 |
| --- | --- |
| `refresh_total` / `refresh_duration_seconds` | 刷新次数和耗时。 |
| `cache_hit_total` / `cache_miss_total` | 基础命中和 miss。 |
| `lock_failed_total` | 重建锁竞争失败次数。 |
| `loader_error_total` | Loader 回源错误次数。 |
| `empty_marker_write_total` | 空值占位写入次数。 |
| `wait_timeout_total` | 等待其它实例重建超时次数。 |
| `prefix_wait_total` / `prefix_retry_total` | 前缀全量刷新对单 key 刷新的影响。 |
| `prefix_delete_total` / `prefix_delete_keys_total` | 前缀删除次数和删除 key 数。 |
| `lookup_state_total` | 读穿最终状态：hit、miss、empty。 |
| `lookup_refresh_triggered_total` | 读穿触发回源次数。 |
| `scan_fallback_total` | 前缀索引不可用时降级扫描次数。 |

## 日志

接入 `WithLogger(...)` 或 `WithGoUtilsLogger(...)` 后，日志统一输出稳定字段：

```text
component=tablecache event=prefix_wait index=user key=a:user:1
```

常见事件：

| 事件 | 含义 |
| --- | --- |
| `lock_lost` | 刷新过程中锁丢失，写回被中止。 |
| `lock_renew_failed` | 锁续期失败。 |
| `prefix_wait` | 单 key 刷新正在等待同前缀全量刷新。 |
| `prefix_retry` | 单 key 刷新发现前缀代际变化后主动重试。 |
| `prefix_scan_fallback` | 前缀索引不可用，降级为 SCAN。 |
| `refresh_batch_done` / `refresh_all_done` | 批量刷新或全量刷新完成。 |

## 管理后台接入

推荐按三层接入：

1. `svc` 层创建 `redis.UniversalClient`、`Store`、`Manager`，并缓存 `manager.Items()` 供页面展示。
2. `logic` 或 `service` 层封装 `RefreshByKey`、`RefreshByKeysWithSummary`、`LoadThroughBatchWithSummaryOptions`。
3. `handler` 层只做参数解析和响应输出，直接复用 `BuildRefreshBatchAdminResponse...` 或 `BuildLoadThroughBatchAdminResponse...`。

完整模板见 [example/admin_integration](./example/admin_integration)。

## 测试

基础验证：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

真实 Redis 集成测试：

```bash
TABLECACHE_REDIS_SINGLE_ADDR='127.0.0.1:6380' \
TABLECACHE_REDIS_SINGLE_DB='0' \
TABLECACHE_REDIS_CLUSTER_ADDRS='127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003' \
go test -count=1 -run 'TestRedis(Single|Cluster)Integration' -v ./...
```

可选环境变量：

| 变量 | 说明 |
| --- | --- |
| `TABLECACHE_REDIS_SINGLE_ADDR` | 单节点 Redis 地址。 |
| `TABLECACHE_REDIS_SINGLE_PASSWORD` | 单节点 Redis 密码。 |
| `TABLECACHE_REDIS_SINGLE_DB` | 单节点 Redis DB。 |
| `TABLECACHE_REDIS_CLUSTER_ADDRS` | Cluster 种子节点，逗号分隔。 |
| `TABLECACHE_REDIS_CLUSTER_PASSWORD` | Cluster 密码。 |
| `TABLECACHE_REDIS_ADDR_MAP` | Cluster 返回内网 hostname 时的地址改写，格式 `host=ip,host2=ip2`。 |

## 上线检查

- 所有 Target 的 `Index`、`Key` 不重复且不重叠。
- 生产项目显式配置短 `WithKeyPrefix`，并开启 `WithStrictKeyPrefix(true)`。
- 大前缀删除开启索引；如果不允许线上全库扫描，配置 `WithScanFallback(false)`。
- Loader 返回的 `Entry.Key` 使用 `params.Key` 或同 Target 范围内 key。
- 不存在数据的热点 key 开启 `AllowEmptyMarker`。
- 大集合配置读写上限，并优先使用 `ReadPage` 做局部读取。
- 指标、日志和告警至少覆盖 `loader_error_total`、`wait_timeout_total`、`lock_lost`、`prefix_scan_fallback`。
