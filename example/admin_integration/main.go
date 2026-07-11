package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	tablecache "github.com/Is999/table-cache"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	// defaultHTTPAddr 表示示例服务默认监听地址。
	defaultHTTPAddr = "127.0.0.1:8080"
	// defaultRedisAddr 表示示例 Redis 默认地址。
	defaultRedisAddr = "127.0.0.1:6379"
	// defaultRedisType 表示示例 Redis 默认模式。
	defaultRedisType = "single"
	// exampleKeyPrefix 表示示例显式使用的业务缓存前缀。
	exampleKeyPrefix = "tc:"
	// maxRequestBodyBytes 表示示例 JSON 请求体上限。
	maxRequestBodyBytes int64 = 64 << 10
	// maxBatchUserIDs 表示单次批量操作允许的用户 ID 上限。
	maxBatchUserIDs = 100
	// apiCodeMethodNotAllowed 表示请求方法不受支持。
	apiCodeMethodNotAllowed = "method_not_allowed"
)

// Config 表示示例服务配置。
type Config struct {
	HTTPAddr                   string            // HTTPAddr 是示例 HTTP 服务监听地址
	RedisType                  string            // RedisType 是 Redis 模式，single 表示单节点，cluster 表示集群
	RedisAddrs                 []string          // RedisAddrs 是 Redis 地址列表，支持单节点或集群
	RedisAddrMap               map[string]string // RedisAddrMap 用于把容器内集群地址改写为宿主机可访问地址
	RedisPassword              string            // RedisPassword 是 Redis 密码
	RedisDB                    int               // RedisDB 是 Redis 单节点模式下的逻辑库编号
	RedisPoolSize              int               // RedisPoolSize 是 Redis 连接池大小
	RedisTLS                   bool              // RedisTLS 表示是否启用 TLS
	RedisTLSInsecureSkipVerify bool              // RedisTLSInsecureSkipVerify 表示是否跳过 TLS 证书校验
}

// clusterSlotsClient 表示执行 CLUSTER SLOTS 的最小客户端能力。
type clusterSlotsClient interface {
	ClusterSlots(ctx context.Context) *redis.ClusterSlotsCmd
}

// User 表示用户缓存对象。
type User struct {
	ID   int64  `json:"id"`   // ID 是用户 ID
	Name string `json:"name"` // Name 是用户名称
}

// userModel 定义示例用户数据源接口。
type userModel interface {
	FindOne(ctx context.Context, id int64) (*User, error)
	FindAll(ctx context.Context) ([]User, error)
}

// memoryUserModel 表示示例内存用户数据源。
type memoryUserModel struct {
	users map[int64]User // users 保存示例用户数据
}

// NewMemoryUserModel 创建示例内存用户数据源。
func NewMemoryUserModel() *memoryUserModel {
	return &memoryUserModel{
		users: map[int64]User{
			1: {
				ID:   1,
				Name: "alpha",
			},
			2: {
				ID:   2,
				Name: "beta",
			},
			3: {
				ID:   3,
				Name: "gamma",
			},
		},
	}
}

// FindOne 根据用户 ID 查询单条示例数据。
func (m *memoryUserModel) FindOne(ctx context.Context, id int64) (*User, error) {
	// ctx 预留给真实数据库或 RPC 场景复用，这里仅保持接口一致。
	_ = ctx

	// user 是当前命中的示例用户数据。
	user, ok := m.users[id]
	if !ok {
		return nil, nil
	}
	// copied 是返回给调用方的副本，避免外部修改内存源数据。
	copied := user
	return &copied, nil
}

// FindAll 返回完整用户快照，供前缀目标执行全量刷新。
func (m *memoryUserModel) FindAll(ctx context.Context) ([]User, error) {
	// ctx 预留给真实数据库或 RPC 场景复用，这里仅保持接口一致。
	_ = ctx
	users := make([]User, 0, len(m.users))
	for _, user := range m.users {
		users = append(users, user)
	}
	sort.Slice(users, func(left int, right int) bool {
		return users[left].ID < users[right].ID
	})
	return users, nil
}

// UserCache 定义业务侧可复用的用户缓存组件。
type UserCache struct {
	manager *tablecache.Manager // manager 是用户缓存管理器
}

// NewUserCache 创建示例用户缓存组件。
func NewUserCache(client redis.UniversalClient, model userModel) (*UserCache, error) {
	if model == nil {
		return nil, errors.Errorf("示例用户数据源不能为空")
	}
	// store 是基于 go-redis 的缓存存储实现。
	store := tablecache.NewRedisStore(client)
	// refreshAll 只在底层拓扑能提供强一致前缀写删时开启；Cluster 继续使用按用户ID预热，避免把普通前缀集中到单槽。
	refreshAll := store.AllowsPrefixMutation(exampleKeyPrefix + "user_profile:")
	// metrics 是示例 Prometheus 指标实现。
	metrics, err := tablecache.NewPrometheusMetrics(
		tablecache.WithPrometheusNamespace("admin"),
		tablecache.WithPrometheusSubsystem("tablecache"),
	)
	if err != nil {
		return nil, errors.Tag(err)
	}
	// manager 是统一缓存管理器，负责读穿、刷新、日志和指标。
	manager, err := tablecache.NewManager(store, []tablecache.Target{
		{
			Index:            "user_profile",
			Title:            "用户资料",
			Key:              "user_profile:",
			KeyTitle:         "user_profile:{userID}",
			Type:             tablecache.TypeString,
			TTL:              time.Hour,
			Jitter:           10 * time.Minute,
			RefreshAll:       refreshAll,
			AllowEmptyMarker: true,
			LoaderTimeout:    500 * time.Millisecond,
			Loader: func(ctx context.Context, params tablecache.LoadParams) ([]tablecache.Entry, error) {
				if params.Key == params.Target.Key {
					users, err := model.FindAll(ctx)
					if err != nil {
						return nil, errors.Tag(err)
					}
					entries := make([]tablecache.Entry, 0, len(users))
					for _, user := range users {
						entries = append(entries, tablecache.Entry{
							Key:   params.Target.Key + strconv.FormatInt(user.ID, 10),
							Type:  tablecache.TypeString,
							Value: user,
						})
					}
					return entries, nil
				}
				// keyParts 必须只包含一个用户ID，拒绝空段或别名后缀触发错误查询。
				keyParts := params.KeyParts
				if len(keyParts) != 1 {
					return nil, errors.Errorf("示例用户缓存key参数数量非法")
				}
				// userID 是当前请求命中的用户 ID。
				userID, err := strconv.ParseInt(keyParts[0], 10, 64)
				if err != nil {
					return nil, errors.Tag(err)
				}
				// user 是当前回源查到的用户数据。
				user, err := model.FindOne(ctx, userID)
				if err != nil {
					return nil, errors.Tag(err)
				}
				if user == nil {
					return nil, errors.Tag(tablecache.ErrNotFound)
				}
				return []tablecache.Entry{{
					Key:   params.Key,
					Type:  tablecache.TypeString,
					Value: user,
				}}, nil
			},
		},
	},
		tablecache.WithMetrics(metrics),
		tablecache.WithGoUtilsLogger(utils.Log()),
		tablecache.WithKeyPrefix(exampleKeyPrefix),
		tablecache.WithRefreshConcurrency(4),
		tablecache.WithLoaderTimeout(500*time.Millisecond),
		tablecache.WithLogKeyRedaction(true),
	)
	if err != nil {
		return nil, errors.Tag(err)
	}
	return &UserCache{
		manager: manager,
	}, nil
}

// key 根据用户 ID 生成已注册缓存 key。
func (c *UserCache) key(userID int64) string {
	return exampleKeyPrefix + "user_profile:" + strconv.FormatInt(userID, 10)
}

// Manager 返回底层缓存管理器，供管理页展示和任务调度复用。
func (c *UserCache) Manager() *tablecache.Manager {
	return c.manager
}

// GetForBiz 在业务读取链路中直接执行读穿缓存。
func (c *UserCache) GetForBiz(ctx context.Context, userID int64) (*User, error) {
	// user 是当前请求的反序列化目标对象。
	var user User

	// result 是当前读穿流程的最终读取状态。
	result, err := c.manager.LoadThrough(ctx, c.key(userID), &user, nil)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if result.State == tablecache.LookupStateHit {
		return &user, nil
	}
	if result.State == tablecache.LookupStateEmpty {
		return nil, nil
	}
	return nil, nil
}

// RefreshFromAdmin 处理管理页手动刷新单个缓存。
func (c *UserCache) RefreshFromAdmin(ctx context.Context, userID int64) error {
	return c.manager.RefreshByKey(ctx, c.key(userID))
}

// WarmupByTaskWithSummary 处理定时预热任务并返回标准批量刷新响应。
func (c *UserCache) WarmupByTaskWithSummary(ctx context.Context, userIDs []int64) (tablecache.RefreshBatchAdminResponse, error) {
	// keys 保存待预热的缓存 key 列表。
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, c.key(userID))
	}
	// results 和 summary 用于直接构造后台标准 JSON 响应。
	results, summary, err := c.manager.RefreshByKeysWithSummary(ctx, keys)
	return tablecache.BuildRefreshBatchAdminResponseWithSummary(results, summary, err), errors.Tag(err)
}

// LoadThroughBatchForAdmin 处理管理页批量读缓存状态。
func (c *UserCache) LoadThroughBatchForAdmin(ctx context.Context, userIDs []int64) (tablecache.LoadThroughBatchAdminResponse, error) {
	// items 是批量读穿请求列表。
	items := make([]tablecache.LoadThroughItem, 0, len(userIDs))
	// users 是批量反序列化承载对象。
	users := make([]User, len(userIDs))
	for index, userID := range userIDs {
		items = append(items, tablecache.LoadThroughItem{
			Key:  c.key(userID),
			Dest: &users[index],
		})
	}
	// results 和 summary 用于直接构造后台标准 JSON 响应。
	results, summary, err := c.manager.LoadThroughBatchWithSummaryOptions(ctx, items, tablecache.LoadThroughBatchOptions{
		Concurrency: 4,
		DefaultOptions: tablecache.LoadThroughOptions{
			LoaderTimeout:    300 * time.Millisecond,
			AllowEmptyMarker: tablecache.Bool(true),
		},
	})
	return tablecache.BuildLoadThroughBatchAdminResponseWithSummary(results, summary, err), errors.Tag(err)
}

// ServiceContext 汇总示例服务运行时依赖。
type ServiceContext struct {
	Redis     redis.UniversalClient // Redis 是全局 Redis 客户端
	Cache     *UserCache            // Cache 是用户缓存组件
	TableList []tablecache.Item     // TableList 是缓存管理页展示列表
}

// NewServiceContext 创建示例服务上下文。
func NewServiceContext(config Config) (*ServiceContext, error) {
	// client 是示例 Redis 通用客户端，支持单节点和集群地址改写。
	client, err := newRedisClient(config)
	if err != nil {
		return nil, errors.Tag(err)
	}
	// model 是示例内存数据源，真实项目可替换成 GORM、RPC 或其它仓储实现。
	model := NewMemoryUserModel()
	// cache 是业务侧统一复用的用户缓存组件。
	cache, err := NewUserCache(client, model)
	if err != nil {
		_ = client.Close()
		return nil, errors.Tag(err)
	}
	return &ServiceContext{
		Redis:     client,
		Cache:     cache,
		TableList: cache.Manager().Items(),
	}, nil
}

// CacheLogic 封装管理页和业务侧共用逻辑。
type CacheLogic struct {
	svcCtx *ServiceContext // svcCtx 是服务上下文
}

// NewCacheLogic 创建缓存逻辑对象。
func NewCacheLogic(svcCtx *ServiceContext) *CacheLogic {
	return &CacheLogic{svcCtx: svcCtx}
}

// ListItems 返回管理页缓存目标列表。
func (l *CacheLogic) ListItems(ctx context.Context) []tablecache.Item {
	// ctx 预留给真实鉴权、审计和链路追踪场景复用。
	_ = ctx
	return l.svcCtx.TableList
}

// GetBizUser 返回业务接口读取到的用户对象。
func (l *CacheLogic) GetBizUser(ctx context.Context, userID int64) (*User, error) {
	return l.svcCtx.Cache.GetForBiz(ctx, userID)
}

// RefreshUserFromAdmin 处理管理页刷新单个用户缓存。
func (l *CacheLogic) RefreshUserFromAdmin(ctx context.Context, userID int64) error {
	return l.svcCtx.Cache.RefreshFromAdmin(ctx, userID)
}

// WarmupUsers 处理定时预热任务并返回标准批量刷新响应。
func (l *CacheLogic) WarmupUsers(ctx context.Context, userIDs []int64) (tablecache.RefreshBatchAdminResponse, error) {
	// response 是当前预热任务的标准响应结构。
	response, err := l.svcCtx.Cache.WarmupByTaskWithSummary(ctx, userIDs)
	return response, errors.Tag(err)
}

// ReadUsersForAdmin 处理管理页批量读取缓存状态。
func (l *CacheLogic) ReadUsersForAdmin(ctx context.Context, userIDs []int64) (tablecache.LoadThroughBatchAdminResponse, error) {
	// response 是当前批量读穿的标准响应结构。
	response, err := l.svcCtx.Cache.LoadThroughBatchForAdmin(ctx, userIDs)
	return response, errors.Tag(err)
}

// userIDRequest 表示单个用户操作请求。
type userIDRequest struct {
	UserID int64 `json:"userId"` // UserID 是单个用户 ID
}

// userIDsRequest 表示多个用户操作请求。
type userIDsRequest struct {
	UserIDs []int64 `json:"userIds"` // UserIDs 是多个用户 ID
}

// initLogger 初始化示例统一日志实现。
func initLogger() {
	// handler 是 slog JSON 输出处理器。
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	// 直接设置 slog 默认 logger，go-utils 默认会跟随 slog.Default()。
	slog.SetDefault(slog.New(handler))
}

// loadConfig 加载并校验示例配置，非法环境变量直接返回错误。
func loadConfig() (Config, error) {
	// redisAddrsText 是环境变量中的 Redis 地址文本。
	redisAddrsText := strings.TrimSpace(os.Getenv("TABLECACHE_REDIS_ADDRS"))
	if redisAddrsText == "" {
		redisAddrsText = defaultRedisAddr
	}
	// redisAddrs 是当前生效的 Redis 地址列表。
	redisAddrs := make([]string, 0)
	for _, addr := range strings.Split(redisAddrsText, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			redisAddrs = append(redisAddrs, addr)
		}
	}
	if len(redisAddrs) == 0 {
		redisAddrs = []string{defaultRedisAddr}
	}
	// redisType 是当前示例使用的 Redis 模式。
	redisType := strings.ToLower(getenvDefault("TABLECACHE_REDIS_TYPE", defaultRedisType))
	if redisType != "single" && redisType != "cluster" {
		return Config{}, errors.Errorf("TABLECACHE_REDIS_TYPE只支持single或cluster")
	}
	// redisDB 是当前生效的 Redis DB。
	redisDB, err := strconv.Atoi(getenvDefault("TABLECACHE_REDIS_DB", "0"))
	if err != nil || redisDB < 0 {
		return Config{}, errors.Errorf("TABLECACHE_REDIS_DB必须是非负整数")
	}
	// redisPoolSize 是当前生效的连接池大小。
	redisPoolSize, err := strconv.Atoi(getenvDefault("TABLECACHE_REDIS_POOL_SIZE", "20"))
	if err != nil || redisPoolSize <= 0 {
		return Config{}, errors.Errorf("TABLECACHE_REDIS_POOL_SIZE必须是正整数")
	}
	redisTLS, err := parseBoolEnv("TABLECACHE_REDIS_TLS")
	if err != nil {
		return Config{}, errors.Tag(err)
	}
	redisTLSInsecure, err := parseBoolEnv("TABLECACHE_REDIS_TLS_INSECURE_SKIP_VERIFY")
	if err != nil {
		return Config{}, errors.Tag(err)
	}
	addrMap, err := parseAddrMap(os.Getenv("TABLECACHE_REDIS_ADDR_MAP"))
	if err != nil {
		return Config{}, errors.Tag(err)
	}
	config := Config{
		HTTPAddr:                   getenvDefault("TABLECACHE_HTTP_ADDR", defaultHTTPAddr),
		RedisType:                  redisType,
		RedisAddrs:                 redisAddrs,
		RedisAddrMap:               addrMap,
		RedisPassword:              os.Getenv("TABLECACHE_REDIS_PASSWORD"),
		RedisDB:                    redisDB,
		RedisPoolSize:              redisPoolSize,
		RedisTLS:                   redisTLS,
		RedisTLSInsecureSkipVerify: redisTLSInsecure,
	}
	if err := validateConfig(config); err != nil {
		return Config{}, errors.Tag(err)
	}
	return config, nil
}

// getenvDefault 返回环境变量值，若为空则回退到默认值。
func getenvDefault(key string, defaultValue string) string {
	// value 是当前环境变量值。
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

// parseBoolEnv 严格解析布尔环境变量，空值表示 false。
func parseBoolEnv(key string) (bool, error) {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch value {
	case "":
		return false, nil
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return false, errors.Errorf("%s必须是布尔值", key)
}

// parseAddrMap 解析形如 host=ip,host2=ip2 的地址改写配置。
func parseAddrMap(text string) (map[string]string, error) {
	result := make(map[string]string)
	for _, item := range strings.Split(text, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		host, mapped, ok := strings.Cut(item, "=")
		if !ok {
			return nil, errors.Errorf("TABLECACHE_REDIS_ADDR_MAP条目[%s]缺少等号", item)
		}
		host = strings.TrimSpace(host)
		mapped = strings.TrimSpace(mapped)
		if host == "" || mapped == "" {
			return nil, errors.Errorf("TABLECACHE_REDIS_ADDR_MAP条目不能为空")
		}
		if _, exists := result[host]; exists {
			return nil, errors.Errorf("TABLECACHE_REDIS_ADDR_MAP主机[%s]重复", host)
		}
		result[host] = mapped
	}
	return result, nil
}

// validateConfig 校验单节点与集群配置不会被静默互相降级。
func validateConfig(config Config) error {
	if strings.TrimSpace(config.HTTPAddr) == "" {
		return errors.Errorf("TABLECACHE_HTTP_ADDR不能为空")
	}
	if config.RedisType != "single" && config.RedisType != "cluster" {
		return errors.Errorf("RedisType只支持single或cluster")
	}
	if len(config.RedisAddrs) == 0 {
		return errors.Errorf("RedisAddrs不能为空")
	}
	if config.RedisType == "single" && len(config.RedisAddrs) != 1 {
		return errors.Errorf("single模式只能配置一个Redis地址")
	}
	if config.RedisType == "cluster" && config.RedisDB != 0 {
		return errors.Errorf("cluster模式不支持非零Redis DB")
	}
	if config.RedisDB < 0 || config.RedisPoolSize <= 0 {
		return errors.Errorf("Redis DB和连接池配置非法")
	}
	if config.RedisTLSInsecureSkipVerify && !config.RedisTLS {
		return errors.Errorf("跳过TLS证书校验前必须启用Redis TLS")
	}
	return nil
}

// buildRedisTLSConfig 根据配置构建 Redis TLS 连接配置。
func buildRedisTLSConfig(config Config) *tls.Config {
	if !config.RedisTLS {
		return nil
	}
	return &tls.Config{
		InsecureSkipVerify: config.RedisTLSInsecureSkipVerify,
	}
}

// newRedisClient 根据配置创建单节点或集群 Redis 客户端，并在集群模式下支持地址改写。
func newRedisClient(config Config) (redis.UniversalClient, error) {
	if err := validateConfig(config); err != nil {
		return nil, errors.Tag(err)
	}
	tlsConfig := buildRedisTLSConfig(config)
	if config.RedisType == "cluster" {
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     config.RedisAddrs,
			Password:  config.RedisPassword,
			PoolSize:  config.RedisPoolSize,
			TLSConfig: tlsConfig,
			ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
				return loadClusterSlotsWithAddrMap(ctx, config.RedisAddrs, config.RedisPassword, tlsConfig, config.RedisAddrMap)
			},
		})
		if err := client.Ping(context.Background()).Err(); err != nil {
			_ = client.Close()
			return nil, errors.Tag(err)
		}
		return client, nil
	}
	// client 是单节点 Redis 客户端。
	client := redis.NewClient(&redis.Options{
		Addr:      config.RedisAddrs[0],
		Password:  config.RedisPassword,
		DB:        config.RedisDB,
		PoolSize:  config.RedisPoolSize,
		TLSConfig: tlsConfig,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, errors.Tag(err)
	}
	return client, nil
}

// loadClusterSlotsWithAddrMap 从任一可用种子节点读取槽位信息，并把容器 hostname 改写为宿主机地址。
// Redis 7 及以上优先使用 ClusterShards；Redis 6.2 使用 ClusterSlots。
func loadClusterSlotsWithAddrMap(ctx context.Context, addrs []string, password string, tlsConfig *tls.Config, addrMap map[string]string) ([]redis.ClusterSlot, error) {
	var lastErr error
	for _, addr := range addrs {
		seedClient := redis.NewClient(&redis.Options{
			Addr:      addr,
			Password:  password,
			TLSConfig: tlsConfig,
		})
		slots, err := loadClusterTopologySlots(ctx, seedClient, addrMap)
		_ = seedClient.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return slots, nil
	}
	if lastErr == nil {
		lastErr = errors.New("没有可用的 Redis Cluster 种子节点")
	}
	return nil, errors.Tag(lastErr)
}

// loadClusterTopologySlots 按 Redis 服务端能力选择 ClusterShards 或 ClusterSlots。
func loadClusterTopologySlots(ctx context.Context, seedClient *redis.Client, addrMap map[string]string) ([]redis.ClusterSlot, error) {
	shards, err := seedClient.ClusterShards(ctx).Result()
	if err == nil {
		return clusterShardsToSlots(shards, addrMap), nil
	}
	if !isClusterShardsUnsupported(err) {
		return nil, errors.Tag(err)
	}
	slots, err := loadClusterSlots(ctx, seedClient)
	if err != nil {
		return nil, errors.Tag(err)
	}
	for slotIndex, slot := range slots {
		for nodeIndex, node := range slot.Nodes {
			slots[slotIndex].Nodes[nodeIndex].Addr = rewriteClusterAddr(node.Addr, addrMap)
		}
	}
	return slots, nil
}

// loadClusterSlots 通过 ClusterSlots 命令读取槽位信息。
func loadClusterSlots(ctx context.Context, client clusterSlotsClient) ([]redis.ClusterSlot, error) {
	slots, err := client.ClusterSlots(ctx).Result()
	return slots, errors.Tag(err)
}

// clusterShardsToSlots 把 Redis 7 的 ClusterShards 结果转换成 go-redis ClusterSlots 回调需要的结构。
func clusterShardsToSlots(shards []redis.ClusterShard, addrMap map[string]string) []redis.ClusterSlot {
	slots := make([]redis.ClusterSlot, 0, len(shards))
	for _, shard := range shards {
		masters := make([]redis.ClusterNode, 0, 1)
		replicas := make([]redis.ClusterNode, 0, len(shard.Nodes))
		for _, node := range shard.Nodes {
			addr := rewriteClusterAddr(clusterShardNodeAddr(node), addrMap)
			if strings.TrimSpace(addr) == "" {
				continue
			}
			clusterNode := redis.ClusterNode{
				ID:   node.ID,
				Addr: addr,
			}
			if strings.EqualFold(node.Role, "master") {
				masters = append(masters, clusterNode)
			} else {
				replicas = append(replicas, clusterNode)
			}
		}
		nodes := append(masters, replicas...)
		if len(nodes) == 0 {
			continue
		}
		for _, slotRange := range shard.Slots {
			slots = append(slots, redis.ClusterSlot{
				Start: int(slotRange.Start),
				End:   int(slotRange.End),
				Nodes: nodes,
			})
		}
	}
	return slots
}

// clusterShardNodeAddr 返回 ClusterShards 节点的可连接地址。
func clusterShardNodeAddr(node redis.Node) string {
	host := strings.TrimSpace(node.Endpoint)
	if host == "" {
		host = strings.TrimSpace(node.Hostname)
	}
	if host == "" {
		host = strings.TrimSpace(node.IP)
	}
	if host == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	port := node.Port
	if port <= 0 {
		port = node.TLSPort
	}
	if port <= 0 {
		return host
	}
	return net.JoinHostPort(host, strconv.FormatInt(port, 10))
}

// isClusterShardsUnsupported 判断当前错误是否表示 Redis 服务端不支持 ClusterShards。
func isClusterShardsUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	if !strings.Contains(message, "shards") {
		return false
	}
	return strings.Contains(message, "unknown command") || strings.Contains(message, "unknown subcommand")
}

// rewriteClusterAddr 按 addr_map 把集群返回的 hostname 改写为宿主机可访问地址。
func rewriteClusterAddr(addr string, addrMap map[string]string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return addr
	}
	mappedHost, ok := addrMap[host]
	if !ok || strings.TrimSpace(mappedHost) == "" {
		return addr
	}
	return net.JoinHostPort(mappedHost, port)
}

// decodeJSONBody 限制请求体大小、拒绝未知字段，并确保请求体只包含一个 JSON 对象。
func decodeJSONBody(writer http.ResponseWriter, request *http.Request, dest any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dest); err != nil {
		return errors.Tag(err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.Errorf("请求体只能包含一个JSON对象")
		}
		return errors.Tag(err)
	}
	return nil
}

// validateUserID 校验示例用户 ID 的业务边界。
func validateUserID(userID int64) error {
	if userID <= 0 {
		return errors.Errorf("userId必须大于0")
	}
	return nil
}

// normalizeUserIDs 校验批量上限并去重，避免重复请求放大缓存和回源压力。
func normalizeUserIDs(userIDs []int64) ([]int64, error) {
	if len(userIDs) == 0 {
		return nil, errors.Errorf("userIds不能为空")
	}
	if len(userIDs) > maxBatchUserIDs {
		return nil, errors.Errorf("userIds不能超过%d项", maxBatchUserIDs)
	}
	seen := make(map[int64]struct{}, len(userIDs))
	result := make([]int64, 0, len(userIDs))
	for _, userID := range userIDs {
		if err := validateUserID(userID); err != nil {
			return nil, errors.Tag(err)
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		result = append(result, userID)
	}
	return result, nil
}

// requireMethod 校验路由方法并输出稳定错误响应。
func requireMethod(writer http.ResponseWriter, request *http.Request, method string) bool {
	if request.Method == method {
		return true
	}
	writer.Header().Set("Allow", method)
	writeAPIError(writer, http.StatusMethodNotAllowed, apiCodeMethodNotAllowed, "请求方法不支持")
	return false
}

// writeAPIError 输出不包含内部错误细节的失败响应。
func writeAPIError(writer http.ResponseWriter, statusCode int, code string, message string) {
	writeJSON(writer, statusCode, map[string]any{
		"success": false,
		"code":    code,
		"message": message,
	})
}

// logSafeError 只记录错误类型，避免示例把 Redis key、下游地址或密钥写入日志。
func logSafeError(event string, err error) {
	if err == nil {
		return
	}
	slog.Error(event, "error_type", fmt.Sprintf("%T", err))
}

// registerRoutes 注册示例 HTTP 路由。
func registerRoutes(mux *http.ServeMux, svcCtx *ServiceContext) {
	// cacheLogic 是缓存相关业务逻辑对象。
	cacheLogic := NewCacheLogic(svcCtx)

	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/tablecache/items", func(writer http.ResponseWriter, request *http.Request) {
		if !requireMethod(writer, request, http.MethodGet) {
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"success": true,
			"code":    tablecache.AdminCodeOK,
			"message": "查询成功",
			"items":   cacheLogic.ListItems(request.Context()),
		})
	})
	mux.HandleFunc("/tablecache/user/refresh", func(writer http.ResponseWriter, request *http.Request) {
		if !requireMethod(writer, request, http.MethodPost) {
			return
		}
		// payload 是当前请求体。
		var payload userIDRequest
		if err := decodeJSONBody(writer, request, &payload); err != nil {
			logSafeError("tablecache_request_invalid", err)
			writeAPIError(writer, http.StatusBadRequest, tablecache.AdminCodeInvalidRequest, "请求参数错误")
			return
		}
		if err := validateUserID(payload.UserID); err != nil {
			writeAPIError(writer, http.StatusBadRequest, tablecache.AdminCodeInvalidRequest, "请求参数错误")
			return
		}
		if err := cacheLogic.RefreshUserFromAdmin(request.Context(), payload.UserID); err != nil {
			logSafeError("tablecache_refresh_failed", err)
			writeAPIError(writer, http.StatusInternalServerError, tablecache.AdminCodeInternalError, "刷新失败")
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"success": true,
			"code":    tablecache.AdminCodeOK,
			"message": "刷新成功",
		})
	})
	mux.HandleFunc("/tablecache/user/warmup", func(writer http.ResponseWriter, request *http.Request) {
		if !requireMethod(writer, request, http.MethodPost) {
			return
		}
		// payload 是当前批量预热请求体。
		var payload userIDsRequest
		if err := decodeJSONBody(writer, request, &payload); err != nil {
			logSafeError("tablecache_request_invalid", err)
			writeAPIError(writer, http.StatusBadRequest, tablecache.AdminCodeInvalidRequest, "请求参数错误")
			return
		}
		userIDs, err := normalizeUserIDs(payload.UserIDs)
		if err != nil {
			writeAPIError(writer, http.StatusBadRequest, tablecache.AdminCodeInvalidRequest, "请求参数错误")
			return
		}
		response, err := cacheLogic.WarmupUsers(request.Context(), userIDs)
		if err != nil {
			logSafeError("tablecache_warmup_failed", err)
		}
		writeJSON(writer, http.StatusOK, response)
	})
	mux.HandleFunc("/tablecache/user/read", func(writer http.ResponseWriter, request *http.Request) {
		if !requireMethod(writer, request, http.MethodPost) {
			return
		}
		// payload 是当前批量读缓存请求体。
		var payload userIDsRequest
		if err := decodeJSONBody(writer, request, &payload); err != nil {
			logSafeError("tablecache_request_invalid", err)
			writeAPIError(writer, http.StatusBadRequest, tablecache.AdminCodeInvalidRequest, "请求参数错误")
			return
		}
		userIDs, err := normalizeUserIDs(payload.UserIDs)
		if err != nil {
			writeAPIError(writer, http.StatusBadRequest, tablecache.AdminCodeInvalidRequest, "请求参数错误")
			return
		}
		response, err := cacheLogic.ReadUsersForAdmin(request.Context(), userIDs)
		if err != nil {
			logSafeError("tablecache_read_failed", err)
		}
		writeJSON(writer, http.StatusOK, response)
	})
	mux.HandleFunc("/biz/user", func(writer http.ResponseWriter, request *http.Request) {
		if !requireMethod(writer, request, http.MethodGet) {
			return
		}
		// userIDText 是 URL 查询参数中的用户 ID 文本。
		userIDText := strings.TrimSpace(request.URL.Query().Get("userId"))
		// userID 是解析后的用户 ID。
		userID, err := strconv.ParseInt(userIDText, 10, 64)
		if err != nil || validateUserID(userID) != nil {
			writeAPIError(writer, http.StatusBadRequest, tablecache.AdminCodeInvalidRequest, "请求参数错误")
			return
		}
		// user 是业务接口读取到的用户对象。
		user, err := cacheLogic.GetBizUser(request.Context(), userID)
		if err != nil {
			logSafeError("tablecache_biz_read_failed", err)
			writeAPIError(writer, http.StatusInternalServerError, tablecache.AdminCodeInternalError, "查询失败")
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"success": true,
			"code":    tablecache.AdminCodeOK,
			"message": "查询成功",
			"data":    user,
		})
	})
}

// writeJSON 统一输出 JSON 响应。
func writeJSON(writer http.ResponseWriter, statusCode int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(statusCode)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		logSafeError("tablecache_response_encode_failed", err)
	}
}

// run 启动示例并在初始化或监听失败时返回错误。
func run() error {
	// config 是示例服务配置。
	config, err := loadConfig()
	if err != nil {
		return errors.Tag(err)
	}
	// svcCtx 是示例服务上下文。
	svcCtx, err := NewServiceContext(config)
	if err != nil {
		return errors.Tag(err)
	}
	defer func() {
		if err := svcCtx.Redis.Close(); err != nil {
			logSafeError("admin_integration_example_redis_close_failed", err)
		}
	}()

	// mux 是示例 HTTP 路由复用器。
	mux := http.NewServeMux()
	registerRoutes(mux, svcCtx)

	// server 是示例 HTTP 服务实例。
	server := &http.Server{
		Addr:              config.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	// 启动日志只记录模式和节点数，不输出 Redis 地址或认证信息。
	utils.Log().Info("admin_integration_example_started", "addr", config.HTTPAddr, "redis_type", config.RedisType, "redis_nodes", len(config.RedisAddrs))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return errors.Tag(err)
	}
	return nil
}

// main 启动管理后台接入的最小可运行示例。
func main() {
	// 统一初始化项目日志。
	initLogger()
	if err := run(); err != nil {
		logSafeError("admin_integration_example_failed", err)
		os.Exit(1)
	}
}
