# tablecache

`tablecache` 是一个框架无关的 Redis 表数据缓存管理库。它把缓存目标声明、读穿回源、批量写入、刷新锁、TTL 抖动、空值占位、前缀删除和管理页响应结构收口到一套稳定组件里，适合管理后台、定时预热任务和业务读取链路共同复用。

## 特性概览

| 能力 | 说明 |
| --- | --- |
| 读穿缓存 | `LoadThrough` / `GetOrRefresh` 先读缓存，miss 后按注册的 `Loader` 回源并写回。 |
| 防击穿 | 同进程使用 `singleflight` 合并同 key 刷新，多实例通过 Lua 原子获取刷新锁并观察当前 owner。 |
| 防穿透 | `AllowEmptyMarker` 对重复访问的不存在 key 写短 TTL 空值占位；随机高基数 miss 由 Loader 全局并发上限继续保护数据源。 |
| 防雪崩 | `TTL + Jitter` 分散过期时间；Loader 默认 30 秒超时、单 Manager 默认最多并发 32 个回源。 |
| 安全提交 | Manager 把当前锁的 `LockGuard` 传到底层提交；RedisStore 在 Lua 实际写删时原子校验 owner，锁已失效则不修改缓存。 |
| 前缀治理 | 单节点/哨兵支持普通前缀；Cluster 仅允许固定 hash tag 同槽前缀。 |
| Redis 部署 | 支持单节点、哨兵主节点和 Cluster master 读取；Ring 因故障重映射可能复活旧值，在构造期整体拒绝。 |
| 集合治理 | 集合分页读取及全量读写默认限制 5000 个成员，避免大 Hash/List/Set/ZSet 一次性拉爆内存。 |
| 管理页响应 | 内置批量刷新、批量读穿响应；JSON 只输出稳定 `code/message`，原始错误仅保留在 Go 字段中。 |
| 可观测性 | 支持 Prometheus 指标、结构化事件日志和 key 脱敏。 |

## 版本要求

- 源码兼容 Go 1.26+；生产构建使用 Go 1.26.5+
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
	"fmt"
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
				if len(params.KeyParts) != 1 {
					return nil, fmt.Errorf("user前缀全量刷新未实现")
				}
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

`Target` 描述一个缓存目标。`Key` / `KeyTitle` 必须是不含 `WithKeyPrefix` 的逻辑值，注册时始终拼接项目命名空间；预拼物理前缀会在构造期返回 `ErrInvalidConfig`。固定 key 用完整逻辑 key，前缀型 key 以冒号结尾。

| 字段 | 说明 |
| --- | --- |
| `Index` | 稳定索引名，用于管理页、指标和日志。 |
| `Title` | 展示名称。 |
| `Key` | 逻辑 key 或逻辑前缀，例如 `user:`。 |
| `KeyTitle` | 展示模板，例如 `user:{id}`。 |
| `Type` | Redis 类型：String、Hash、List、Set、ZSet。 |
| `TTL` / `Jitter` | 基础过期时间和最大抖动；`Jitter=0` 时 RedisStore 默认按 TTL 的 10% 抖动，只有 `WithDefaultJitterRatio(0)` 才会关闭。 |
| `EmptyTTL` | 空值占位过期时间。 |
| `LoaderTimeout` | 当前目标回源超时时间。 |
| `RefreshAll` | 是否允许被 `RefreshAll` 批量刷新。 |
| `AllowEmptyMarker` | 查无数据时是否写空值占位。 |
| `Loader` | 回源函数，返回要写入 Redis 的 `Entry` 列表。 |

`Manager.Items()` 返回管理列表展示契约，其中 `key` 是注册后真正用于操作的固定 key 或前缀，`keyTitle` 是展示模板；二者不会互相覆盖。结果还包含 `index`、`title`、`type` 和 `remark`。

### Key 前缀

默认业务 key 前缀是 `tc:`。生产项目建议显式传入以冒号结尾的短项目前缀，例如 `WithKeyPrefix("a:")`。

内部锁、空值占位、前缀索引等元信息 key 使用短根前缀 `tcm:`，与业务 key 前缀分开管理。

- `Get` / `GetState` 支持传逻辑 key，会自动补前缀后只读查询。
- `RefreshByKey`、`RefreshByKeys`、`DeleteByKey`、`DeleteByPrefix`、`LoadThrough` 和 `GetOrRefresh` 的写回路径必须传实际 key。
- 未带前缀的 miss 请求只做只读查询，不会触发回源写入，避免误写非托管 key。
- 构造期始终拒绝空前缀、未以冒号结尾的前缀、通配符、空白字符和内部保留根 `tcm:`；目标 key 会独立校验 Redis Cluster hash tag。
- 内部前缀删除通过 `RedisPrefixPattern` 转义 Redis glob 字符；自定义 `PrefixReplaceStore` 也必须使用已经转义的 `DeletePatterns`。

### Entry

`Entry` 是 Loader 返回的写入单元。Loader 的写入范围按本次请求收紧：

- 固定目标只能写固定 key。
- 前缀目标的非全量请求只能返回 `Entry.Key == params.Key`；不能借一次单 key 回源顺带写入同前缀其它 key。
- 只有 `params.Key == params.Target.Key` 的前缀全量请求可以返回该 Target 前缀内的多个 key，并且必须给出完整、权威快照。
- Hash/List/Set/ZSet 的空集合会写内部元信息，使后续读取明确返回 hit，而不是反复 miss。
- `Overwrite=nil/true` 使用可重试的覆盖写；`Overwrite=false` 只允许 Hash、Set、ZSet 的幂等增量写。String 没有独立增量语义，List 增量 `RPUSH` 在网络响应丢失重放时会重复成员，二者都会在构造写计划时返回 `ErrInvalidConfig`。

## 并发与一致性

当多个请求同时读取一个不存在的缓存：

1. 所有请求先查缓存。
2. 同一进程内相同 key 通过 `singleflight` 合并。
3. 多实例通过 Lua 原子竞争重建锁并观察 owner，只有持锁方执行 Loader。
4. 未抢到锁的请求等待同一语义范围的短结果 marker；拿锁方还会二次检查缓存和刷新 generation，关闭快速 owner 窗口。
5. Loader 查无数据时可写空值占位，避免热点空数据持续穿透数据库。
6. 长 Loader 会自动续期锁；锁续期和 Go 侧预检只用于尽早发现失锁，不是最终正确性边界。

请求型读穿、显式刷新和 `RefreshAll` 的本地 flight 与分布式结果 marker 相互隔离，不能用 scheduled 空结果替代请求型空值语义。未使用单次 Loader、超时、marker 或 context policy 覆盖时，即使 `AllowEmptyMarker=false`，一次成功但为空的默认读穿也会发布短完成 generation，只合并已经进入同一轮的 waiter，不会把后续新请求变成持久负缓存。带单次覆盖的调用无法证明跨实例语义相同，不共享 generation；热点空值仍应开启 `AllowEmptyMarker`。`LookupResult.Refreshed` 表示本次 miss 参与了执行、等待或复用刷新结果，不等同于“当前 goroutine 一定执行了 Loader”。

`DeleteByKey` 与同 key 刷新共用刷新锁，失锁后的旧请求再由 Lua fencing 阻断写回，不额外维护重复的 key epoch。跨 key 的前缀全量刷新和前缀删除仍使用前缀提交锁及必要的 prefix/member epoch，避免旧单 key 刷新覆盖新全量结果，或旧全量快照复活已删除成员。刷新起始会在短提交锁内确保代际非空；提交时代际缺失、被淘汰或变化都按失效处理，不能把“首次为空”和“删除后被淘汰为空”误判成相等。

所有 Manager 可见的业务写入、删除、marker 切换和 ready 状态提交都会携带当前刷新锁或前缀锁的 `LockGuard`。RedisStore 在每批真实 Redis 写删所用的 Lua 内先校验 `LockGuard.Key` 对应的值仍等于 `LockGuard.Owner`，并在同一个原子执行边界内完成该批变更；任一 guard 不匹配时整批不落数据。索引新成员可以在业务写入前先登记，因为它只会把索引扩大为安全超集，不会让删除漏掉已可见数据。前缀全量操作可能分成多批，它保证的是“每一批都在仍持锁时提交”，不是把整个快照伪装成单个超大事务。自定义 Store 也必须提供等价的原子 guard 语义，不能只在 Go 中先 `GET` 再写删。

### 回源并发边界

- Loader 默认超时为 30 秒，可用 `WithLoaderTimeout` 或 `Target.LoaderTimeout` 调整；`0` 表示沿用上级默认值，负数或超过 24 小时会在启动期返回 `ErrInvalidConfig`。
- Loader 超时只包围等待 Loader 槽和 Loader 本身；锁、代际与 Redis 提交受完整刷新总预算约束。即使使用 `RebuildContextIgnoreCancel`，完整刷新和最终回读也保留内部截止时间，不会永久挂起。
- Loader 及其 DB/RPC 调用必须透传并响应 `ctx`；Go context 不能强制终止忽略取消信号的业务函数，违反该约定仍会长期占用 goroutine 和刷新锁。
- 刷新锁 TTL 至少 30ms，显式续期间隔至少 10ms；显式等待步长至少 1ms、最多 100000 次且总窗口不超过 24 小时。
- 单个 Manager 默认最多同时执行 32 个 Loader，可用 `WithLoaderConcurrency` 调整；Loader、批量刷新和批量读穿并发硬上限均为 256，并按实际任务数分配并发槽。
- 该上限是单进程、单 Manager 的数据源保护边界；多实例部署仍应结合数据库/RPC 容量设置每实例值。
- 只有真实允许跨 key 前缀操作的目标，Loader 有效超时才必须不超过前缀代际 TTL 的一半；固定 key 和 Cluster 无 tag 的逐 key 路径不使用 prefix epoch，不受这项无关限制。

### Hash fields 局部刷新

`LoadThroughOptions.Fields` 只允许用于 `TypeHash`，并要求 Store 同时实现 `CollectionPageStore` 和 `HashFieldsStore`；开启字段级空值占位时还要实现 `FieldsEmptyStore`：

- HMGET 缺少任一请求字段时按 miss 处理，不把半份结果当成 hit。
- Loader 必须返回同一个 Hash key、恰好一条 Entry，并完整覆盖请求字段；返回额外字段或缺字段都会失败。
- 局部刷新强制增量写入，显式 `Overwrite=true` 会返回 `ErrPartialHashUnsupported`，避免误删未请求字段。
- Loader 返回不存在时始终通过同槽 Lua 原子删除本次请求字段；开启空值占位时再登记字段组合空值，关闭时同时清除该 Hash 的全部历史 fields registry。整 key 空刷新也会清除全部历史 registry。
- 字段组合使用同槽 ZSET registry 保存独立到期时间，并按 Redis 服务器时间清理；单次最多渐进移除 256 个过期成员，全部过期时使用 `UNLINK` 异步释放。任一字段正向写回会在同一原子提交中清除该 Hash 的整个 registry，避免重叠组合保留错误负缓存；代价只是其它组合下次可能多回源一次。

单 key 刷新中的隐藏空值和真实空集合通过 `MarkerReplaceStore.ReplaceWithMarker` 在同一个 Redis hash slot 内先写 marker、再删除旧状态。前缀全量快照的 marker 与旧数据切换由 `PrefixReplaceStore.ReplacePrefix` 统一提交。自定义 Store 不支持对应能力时直接返回错误，不会退回可能暴露中间空窗的多次命令。

### 前缀全量替换

前缀目标执行全量刷新时，Manager 要求 Store 实现 `PrefixReplaceStore`。实现必须先校验并写入新快照，再删除不在 `KeepKeys` 中的旧 key；失败时可以暂时保留冗余旧数据，但不能先清空旧前缀。自定义 Store 未实现该接口时会返回 `ErrPrefixReplaceUnsupported`，不会破坏性降级。

`DeleteByPrefix` 只有在可信索引可用，或 Store 实现 `GuardedPatternStore` 时才能执行；缺少带锁 SCAN 能力会返回专用的 `ErrPrefixDeleteUnsupported`，不会复用全量替换错误或执行无 guard 删除。

`WithScanFallback(false)` 只控制 `Manager.DeleteByPrefix` 的在线降级。首次前缀 full refresh、索引证据丢失后的 full refresh，仍必须通过 `ReplacePrefix` 执行带 guard 的 SCAN 对账，才能建立权威快照和可信 ready；这类操作应放在维护窗口，并按整个 Redis keyspace、Cluster master 数量和连接池容量评估压力。

前缀 Loader 收到 `params.Key == params.Target.Key` 时表示全量刷新，此时 `KeyParts` 为空且不能携带 `Fields`。Loader 必须返回该前缀当前完整、权威的 Entry 快照；返回空切片会被解释为“当前快照为空”并安全删除旧成员，不会为前缀根生成空值 marker。全量 Entry 必须使用默认覆盖语义或显式 `Overwrite=true`，`Overwrite=false` 会被拒绝，避免旧 Hash 字段或集合成员混入新快照。若业务不支持全量刷新，必须返回普通错误明确拒绝，不能访问 `KeyParts[0]`，也不能用 `ErrNotFound` 代替“不支持”。

前缀全量刷新和 `DeleteByPrefix` 还受 Redis 拓扑约束：

- 单节点或哨兵当前主节点没有跨 slot 问题，普通前缀可以执行全量刷新和前缀删除。
- Cluster 上不带固定 hash tag 的普通前缀会分散到多个 slot，无法让一个前缀锁与所有真实写删处于同一 Lua 原子边界，因此前缀全量刷新和 `DeleteByPrefix` 会快速失败（fail-fast），不执行 Loader、SCAN 或部分写删。
- Cluster 上只有包含固定非空 hash tag 的前缀才允许这两个操作，例如 `a:user:{all}:`。该前缀下的业务 key、锁、marker 和在线索引都会集中到同一个 slot；这换取了原子 guard 能力，也会集中容量与热点，必须按单 slot 预算评估。
- Ring 的故障摘除会重映射单 key，节点恢复后可能重新暴露刷新或删除前的旧值；该风险无法由逐 key 锁修复，因此 `RedisStore` 在构造校验期整体拒绝 Ring，而不只拒绝前缀操作。
- 普通无固定 hash tag 的分布式前缀应使用 `RefreshByKeys` 和 `DeleteByKey` 按实际 key 操作；每个 key 的业务值、锁和元信息会在自己的 slot 内安全提交。

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
	ReadOnly: false,
	RouteByLatency: false,
	RouteRandomly: false,
})
store := tablecache.NewRedisStore(client)
```

如果 Redis Cluster 返回的是容器 hostname，需要在业务 Redis 客户端层处理地址改写；真实集成测试可参考 `redis_cluster_integration_test.go` 的 `TABLECACHE_REDIS_ADDR_MAP`。Redis 6.2 不支持 `CLUSTER SHARDS` 时，示例会回退到 `CLUSTER SLOTS`。

`RedisStore` 接受 `redis.UniversalClient`，但生产支持范围只包括单节点、哨兵当前主节点和 Cluster master 路由：

- 单节点和哨兵客户端按当前主节点执行。
- Cluster 普通无固定 hash tag 的前缀全量刷新和 `DeleteByPrefix` 始终 fail-fast；即使客户端能遍历全部节点，也不会放宽原子提交边界。
- Cluster 带固定 hash tag 的前缀允许执行，但全部相关 key 集中在同一个 slot；需要 SCAN 对账时，wrapper 仍须暴露可靠的 `ForEachMaster` 能力。
- Cluster 的 `ReadOnly`、`RouteByLatency`、`RouteRandomly` 必须全部为 `false`。锁、marker、代际和业务值必须从 master 读取；副本延迟会破坏写后读与击穿判断，构造校验会直接拒绝这三种配置。
- `redis.NewClient` 若实际连接到 Cluster seed，会在任何前缀 Loader、SCAN 或写入前拒绝，必须改用 `redis.NewClusterClient`，避免只处理一个 master。
- 单节点/哨兵客户端首次执行前缀操作会通过 `CLUSTER INFO` 确认拓扑并缓存结果；ACL、网络错误或无法识别的响应都会 fail-close，生产账号需允许该探测命令。
- Ring 当前版本完全不支持；请改用单节点、哨兵或 Cluster master 路由。
- 无法确认拓扑或无法保证同槽时返回错误，不会扫描部分节点或提交部分结果后伪装成功。

## 生产配置

推荐起步配置：

```go
store := tablecache.NewRedisStore(
	client,
	tablecache.WithUnlinkChunkSize(512),
	tablecache.WithMaxCollectionReadCount(5000),
	tablecache.WithMaxCollectionWriteCount(5000),
)

manager, err := tablecache.NewManager(
	store,
	targets,
	tablecache.WithKeyPrefix("a:"),
	tablecache.WithPrefixKeyIndex(true),
	tablecache.WithPrefixKeyIndexTTL(30*24*time.Hour),
	tablecache.WithScanFallback(false),
	tablecache.WithScanCount(2000),
	tablecache.WithPrefixDeleteConcurrency(1),
	tablecache.WithRefreshConcurrency(4),
	tablecache.WithLoaderTimeout(30*time.Second),
	tablecache.WithLoaderConcurrency(32),
	tablecache.WithLogKeyRedaction(true),
)
```

| 配置 | 建议 |
| --- | --- |
| `WithKeyPrefix` | 使用以冒号结尾的短项目前缀，例如 `a:`、`crm:`，避免 Redis key 过长。 |
| `WithPrefixKeyIndex` | 默认开启，为允许执行前缀操作的 Target 维护安全 superset 索引，减少 SCAN。 |
| `WithScanFallback(false)` | 默认值；索引不可信时显式失败。仅维护窗口在容量评审后传 `true`，且不能绕过 Cluster 同槽要求。 |
| `WithScanCount` | 常见取值 1000 到 5000，硬上限 100000；越大单轮 Redis 压力越高。 |
| `WithUnlinkChunkSize` | 常见取值 500 到 1000，硬上限 10000，控制 Pipeline 大小。 |
| `WithPipelineRetries` | 默认 1、硬上限 10；只重试可证明幂等的 Pipeline。 |
| `WithPrefixDeleteConcurrency` | 高峰期保持 1 或 2；后台维护窗口可适当提高。 |
| `WithMaxCollectionReadCount` | 默认 5000 个成员；超限返回 `ErrCollectionTooLarge`，显式 `0` 才关闭。 |
| `WithMaxCollectionWriteCount` | 默认 5000 个成员；限制单 Entry 集合成员数，显式 `0` 才关闭。 |
| `WithLoaderTimeout` | 默认 30 秒；按数据源超时预算调小，`0` 保持默认，负数非法。 |
| `WithLoaderConcurrency` | 默认 32、硬上限 256；按单实例可分配的数据源并发预算设置。 |
| `WithDefaultJitterRatio` | 默认 0.1，只接受 0 到 1；`0` 关闭默认抖动。 |
| `WithLogKeyRedaction` | 默认开启；只有受控本地诊断才显式关闭。 |

集合上限只按成员数计算，不是序列化字节上限。不要把超大 blob 当表缓存；业务必须按 Redis `proto-max-bulk-len`、`maxmemory`、网络和 Loader 超时控制单 Entry 字节数，以及一次 full Loader 的总条目和总字节。大对象应拆分、分页或放到对象存储；只有完成容量评审后才能显式传 `0` 关闭成员上限。

### 前缀索引

在线前缀索引是安全 superset，不承诺与当前业务 key 集合时刻完全相等。写入新业务 key 时先登记索引、再写业务值；在线删除不破坏性移除成员，陈旧成员由成员时间和 TTL 自然收敛。因此超时、进程退出或 Redis 部分失败最多留下可清理的陈旧成员，不会让一次成功可见的 Manager 提交产生“业务 key 已存在但索引漏记”的结果。按可信索引删除时遇到陈旧成员是安全的，因为删除不存在的 key 不会扩大操作范围。索引成员每 256 个组成一条 Lua 命令，每个 Pipeline 最多 64 条命令；过期成员每次最多渐进清理 256 个，避免大快照或过期积压制造客户端和 Redis 主线程尖峰。

`DeleteByKey` 和 `DeleteByPrefix` 成功后会删除真实业务状态，但保留索引成员、manifest 与 ready 证据；后续删除仍可继续走索引快路径，不会因为一次在线清理强制退化为全节点 SCAN。

索引的 manifest、ready 和完整性证据只用于证明这个 superset 可以安全作为删除候选集。重建过程的位图初始化、历史哨兵恢复、权威成员写入和 manifest 提交都受当前锁 owner 栅栏保护；首次成功且拓扑允许的前缀全量刷新会完成旧数据对账后建立 ready。证据缺失或不可信时立即撤销 ready，并按 `WithScanFallback` 选择 SCAN 或显式报错。Cluster 普通无固定 hash tag 仍直接 fail-fast，不能借索引或 SCAN 绕过拓扑边界；Ring 在 Store 校验期已整体拒绝。索引的内部拆分方式属于实现细节，不构成跨 slot 全量写删能力。

本版本不读取、迁移或清理旧格式索引，也不支持新旧实例混跑。升级或回滚时必须先停止全部实例，按 Target 清理原缓存与内部元信息或切换到新的 Redis namespace；启动目标版本后，只对单节点/哨兵普通前缀或 Cluster 固定 hash tag 前缀执行一次全量刷新。

## API 选择

| 场景 | 推荐 API |
| --- | --- |
| 只读缓存 | `Get` / `GetState` |
| 业务读穿 | `LoadThrough` |
| 读穿并临时覆盖 Loader、超时或 fields | `LoadThroughWithOptions` |
| 批量读穿 | `LoadThroughBatchWithSummaryOptions` |
| 管理页刷新单 key | `RefreshByKey` |
| 定时任务批量预热 | `RefreshByKeysWithSummary` |
| 管理页全量刷新 | 对拓扑允许的 Target 使用 `RefreshAllWithSummary` |
| 精确删除 | `DeleteByKey` |
| 前缀删除 | 单节点/哨兵普通前缀或 Cluster 固定 hash tag 前缀使用 `DeleteByPrefix`；其它分布式场景使用 `DeleteByKey` |
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

自定义 `WithPrometheusRefreshBuckets` 或 `WithPrometheusRefreshEntryBuckets` 时，桶必须严格递增且不能包含 NaN；非法配置会由 `NewPrometheusMetrics` 直接返回错误，不会延迟到首次记录时 panic。

| 指标 | 说明 |
| --- | --- |
| `refresh_total` / `refresh_duration_seconds` | 刷新次数和耗时。 |
| `cache_hit_total` / `cache_miss_total` | 基础命中和 miss。 |
| `lock_failed_total` | 重建锁竞争失败次数。 |
| `loader_error_total` | Loader 回源错误次数。 |
| `empty_marker_write_total` | 空值占位写入次数。 |
| `wait_timeout_total` | 等待其它实例重建超时次数。 |
| `prefix_wait_total` / `prefix_retry_total` | 单 key 仅在 Redis 提交阶段等待前缀提交锁，以及因全量代际推进而重试的次数；Loader 本身不被前缀提交锁串行化。 |
| `prefix_delete_total` / `prefix_delete_keys_total` | 前缀删除次数和 Redis 实际删除 key 数；已经不存在的 key 不计数。 |
| `lookup_state_total` | 每次逻辑读取只记录一个最终状态：hit、miss、empty；miss 后回源成功不会再同时计一次 miss。 |
| `lookup_refresh_triggered_total` | 读穿触发回源次数。 |
| `scan_fallback_total` | 实际执行前缀降级扫描的次数；被 `WithScanFallback(false)` 拒绝时不计数。 |

## 日志

自定义 printf 风格日志使用 `WithLogger(...)`，go-utils 日志使用 `WithGoUtilsLogger(...)`；接入后统一输出稳定字段：

```text
component=tablecache event="prefix_wait" index="user" key="sha1:7f7f...:len:8"
```

业务 key 默认通过 `sha1:<摘要>:len:<长度>` 脱敏；只有显式设置 `WithLogKeyRedaction(false)` 才会输出原始 key。

常见事件：

| 事件 | 含义 |
| --- | --- |
| `lock_lost` | 刷新过程中锁丢失，写回被中止。 |
| `lock_renew_failed` | 锁续期失败。 |
| `prefix_wait` | 单 key 已完成 Loader，正在等待同前缀的短提交锁。 |
| `prefix_retry` | 单 key 刷新发现前缀代际变化后主动重试。 |
| `prefix_scan_fallback` | 前缀索引不可用，降级为 SCAN。 |
| `refresh_batch_done` / `refresh_all_done` | 批量刷新或全量刷新完成。 |

## 管理后台接入

推荐按三层接入：

1. `svc` 层创建 `redis.UniversalClient`、`Store`、`Manager`，并缓存 `manager.Items()` 供页面展示。
2. `logic` 或 `service` 层封装 `RefreshByKey`、`RefreshByKeysWithSummary`、`LoadThroughBatchWithSummaryOptions`。
3. `handler` 层只做参数解析和响应输出，直接复用 `BuildRefreshBatchAdminResponse...` 或 `BuildLoadThroughBatchAdminResponse...`。

批量响应的顶层和逐项结果统一包含 `success/code/message`。`code/message` 是稳定、安全的公开契约；`Error` 字段标记为 `json:"-"`，只供调用方在受控日志中使用，未知下游错误统一输出 `internal_error / 操作失败`。不要把 `Error.Error()` 直接写入 HTTP JSON 或未脱敏日志。

公开 code 包括：`ok`、`partial_failure`、`invalid_request`、`target_not_found`、`loader_required`、`wait_timeout`、`refresh_unavailable`、`cache_miss`、`data_not_found`、`invalid_key`、`scan_fallback_disabled`、`collection_too_large` 和兜底 `internal_error`。配置或 fields 请求不符合约束时返回 `invalid_request`；Store 缺少安全刷新能力时返回 `refresh_unavailable`。调用方应按 code 分支，message 只用于展示。

HTTP JSON 不输出 `error` 字段，调用方只按稳定的 `code` 分支并展示 `message`。Go 调用方仍可通过顶层和逐项 `Error` 字段取得原始错误链。

完整接入示例见 [example/admin_integration](./example/admin_integration)。它为了本地验证提供 HTTP 路由和内存数据源，并具备以下边界：

- 默认仅监听 `127.0.0.1:8080`，显式使用 `tc:` key 前缀和严格前缀校验。
- JSON 请求体最多 64 KiB，拒绝未知字段和多个 JSON 对象；批量用户 ID 最多 100 个并按顺序去重。
- HTTP 配置了 header/read/write/idle 超时，日志不输出 Redis 地址、业务 key 或下游原始错误。
- 示例没有业务鉴权、权限码、MFA、审计和限流契约，`/metrics` 也没有访问控制；**不能直接绑定公网或作为生产服务部署**。接入真实项目时必须复用项目现有鉴权和审计中间件。

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

- 所有 Target 的 `Index`、逻辑 `Key` 不重复且不重叠，`Key` / `KeyTitle` 没有预拼 `WithKeyPrefix`。
- 生产项目显式配置以冒号结尾、短且不以 `tcm:` 开头的 `WithKeyPrefix`；危险前缀会在构造期直接失败。
- 单节点/哨兵普通前缀可以执行前缀全量刷新 / `DeleteByPrefix`；Cluster 仅对固定 hash tag 前缀开放，并已评估单 slot 容量与热点。
- Cluster 普通无固定 hash tag 使用 `RefreshByKeys` / `DeleteByKey`，没有尝试用索引或全节点 SCAN 绕过 fail-fast；Ring 已改用受支持拓扑。
- 允许执行前缀操作的 Target 开启索引；生产默认 `WithScanFallback(false)`，只有受控维护窗口才显式开启 SCAN。
- 未混跑不同版本；升级或回滚前已停止全部实例，并清理旧缓存 namespace 或切换到新 namespace。
- 自定义 Store 按使用能力实现 `MarkerReplaceStore`、`HashFieldsStore`、`FieldsEmptyStore`、`PrefixReplaceStore` 和 `GuardedPatternStore`，并遵守 marker 状态切换与“先写新快照、后删旧 key”的失败语义；`DeleteByPrefix` 需要按可信索引走快路径时还要实现 `PrefixIndexStore`，否则只能通过 `GuardedPatternStore` 执行带 guard 的 SCAN。需要支持前缀操作时还应在 Loader 前完成等价的拓扑校验。
- 自定义 Store 的 `ApplyMutation` 必须先登记新索引、再提交业务状态；在线删除可以保留陈旧索引成员，不能让旧 owner 延迟移除新成员。收到 `LockGuard` 的可见写删必须在实际变更的同一原子边界内校验 owner；需要构造期校验时实现专用 `StoreValidator`。
- 自定义 Store 的 `AcquireRefreshLock` 必须原子返回锁竞争时的当前 owner，避免快 Loader 在抢锁与读取 owner 之间完成所造成的重复回源窗口。
- 需要前缀 SCAN 对账的 Cluster wrapper 暴露完整 `ForEachMaster` 能力，并已在真实环境验证所有 master 可达；该能力不放宽同槽限制。
- 非 full Loader 只返回 `Entry.Key == params.Key`；只有前缀 full Loader 可以返回同 Target 前缀下的完整快照。
- 根据单实例数据源预算设置 `WithLoaderTimeout` 和 `WithLoaderConcurrency`。
- 不存在数据的热点 key 开启 `AllowEmptyMarker`。
- 随机高基数 miss 另有网关限流、业务参数校验或数据源熔断，不能只依赖空值占位。
- 大集合配置读写上限，并优先使用 `ReadPage` 做局部读取。
- 指标、日志和告警至少覆盖 `loader_error_total`、`wait_timeout_total`、`lock_lost`、`prefix_scan_fallback`。
- 管理路由已接入业务鉴权、权限、审计和限流；示例路由未直接部署。

以上配置都在 `NewManager` / `NewRedisStore` 构造期生效，发布新版本或修改这些 Option 后需要重启进程。不需要 MySQL/业务数据迁移、回填或补偿；但本版本取消旧缓存兼容，升级和回滚都必须清理旧缓存 namespace，并仅对拓扑允许的前缀 Target 执行一次全量刷新建立可信 ready。Cluster 普通无固定 hash tag 继续使用逐 key API；Ring 必须先迁移到受支持拓扑。关闭索引后再次开启时，同样只对允许执行前缀操作的 Target 重建可信 ready。
