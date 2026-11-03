[English](README.md) | 简体中文

# dtm-driver-kitex

[DTM](https://github.com/dtm-labs/dtm) 的 Kitex 驱动，面向使用标准 gRPC over HTTP/2 的 Kitex protobuf 服务。它让 DTM 能够通过 Kitex 兼容的 etcd、Consul 或 Nacos 注册中心完成自身注册与分支 URL 解析。

不支持 Kitex TTHeader、Thrift 和 Hertz 传输方式。

## 安装与启用

本包在被导入时自动完成注册。应用通常使用匿名导入，并通过 DTM 标准驱动 API 选择该驱动：

```go
import (
    "github.com/dtm-labs/dtmdriver"
    _ "github.com/dtm-labs/dtmdriver-kitex"
)

if err := dtmdriver.Use("dtm-driver-kitex"); err != nil {
    panic(err)
}
```

DTM Server 的构建产物必须包含同样的匿名导入。其配置与其他 DTM 微服务驱动完全一致：

```yaml
MicroService:
  Driver: 'dtm-driver-kitex'
  Target: 'kitex-etcd://localhost:2379/dtm-server'
  EndPoint: 'localhost:36790'
```

公开的包 API 刻意保持精简：`DriverName`、`SchemeEtcd`、`SchemeConsul`、`SchemeNacos` 和 `ServerMetadataMiddleware`。

## 支持范围

| 能力 | 支持情况 |
| --- | --- |
| 注册中心 | etcd（`registry-etcd v0.2.6`）、Consul（`registry-consul v0.2.0`）、Nacos（`registry-nacos v0.1.3`） |
| 传输方式 | 标准 gRPC / HTTP/2 + protobuf |
| 载荷类型 | protobuf 类型化载荷与 DTM 原始（`isRaw`）载荷 |
| 负载均衡 | grpc-go 的 `pick_first` 与 `round_robin` |
| 直连模式 | `host:port/<grpc.Service>/<Method>` |
| 权重路由 | 权重以 `kitex.weight` 保留；标准 grpc-go balancer 会忽略它 |

## URI 格式

分支 URL 的格式为 `<discovery-service>/<grpc-service>/<method>`：

```text
# 直连
127.0.0.1:8080/order.OrderService/Create

# 经注册中心解析
kitex-etcd://127.0.0.1:2379/order-svc/order.OrderService/Create
kitex-consul://127.0.0.1:8500/order-svc/order.OrderService/Create
kitex-nacos://127.0.0.1:8848/order-svc/order.OrderService/Create
```

etcd 支持多个以逗号分隔的 endpoint；Consul 和 Nacos 只支持一个。

通用查询参数：

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `refresh_interval` | `5s` | 服务发现轮询间隔 |
| `empty_grace` | `30s` | 空结果持续多长时间后才会清空地址列表 |
| `default_weight` | `10` | 注册权重 |

各后端专属参数：

- etcd：`prefix`、`user`、`password`、`dial_timeout`
- Consul：`datacenter`、`token`、`scheme`（`http` 或 `https`）
- Nacos：`group`、`cluster`、`namespace`、`username`、`password`

未知参数、userinfo、fragment、非法 endpoint 以及不合法的服务名或方法路径，都会在创建任何 provider 之前被拒绝。

### 凭据

凭据直接写在注册中心 URI 中：

```text
kitex-etcd://127.0.0.1:2379/order-svc?user=dtm&password=secret
kitex-consul://127.0.0.1:8500/order-svc?token=acl-token
kitex-nacos://127.0.0.1:8848/order-svc?username=dtm&password=secret&namespace=prod
```

DTM 会持久化分支 URL，因此 URI 中的凭据也可能被持久化到事务数据里。请在注册中心侧使用网络边界或专用最小权限账号，控制对 DTM 存储和日志的访问，并按部署策略定期轮换凭据。原始凭据值不会进入 provider 缓存键，也不会出现在解析器错误信息中；缓存键使用由凭据派生的 SHA-256 标识。

自定义 CA 文件、客户端证书和凭据配置文件不属于本最小 API。当上游 SDK 支持时，请在主机/容器层面配置注册中心的 TLS 信任。

## Kitex 服务端必需的中间件

DTM 分支辅助函数从 grpc-go 的 incoming metadata 中读取事务字段，而 Kitex 把 HTTP/2 metadata 存放在自己的 context 里，因此每个作为分支服务的 Kitex 服务端都必须安装这个桥接中间件：

```go
import (
    "github.com/cloudwego/kitex/server"
    dtmkitex "github.com/dtm-labs/dtmdriver-kitex"
)

srv := order.NewServer(handler,
    server.WithMiddleware(dtmkitex.ServerMetadataMiddleware),
)
```

该中间件只负责把 metadata 复制进 grpc-go 的 incoming context，不执行屏障 SQL，不修改结果，也不记录 metadata 的值。

## 运行时行为

- Resolver 的 scheme 在包初始化时注册一次，因此 `RegisterAddrResolver` 是一个刻意的空操作。
- 每个 target 使用单 goroutine 的 resolver 状态机，具备周期性刷新、指数错误退避、保留最后一次可用状态、空结果宽限期，以及合并 `ResolveNow` 请求的能力。
- Provider 客户端在进程生命周期内共享。`RegisterService` 将每次调用委托给所选的 Kitex registry。没有导出的关闭 API；registry 租约、健康检查或临时实例会按各后端自身的行为处理进程崩溃后的清理。
- Consul 注册使用 agent 侧 TCP 健康检查。Nacos 注册为临时实例，由 SDK 心跳维持。

## 失败语义

Kitex handler 必须使用 `github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status` 创建 status 错误；普通的 grpc-go status 错误可能被 Kitex 传输层降级为 `codes.Internal`。

- `OK`：分支成功
- `codes.Aborted`：确定性失败，DTM 回滚
- `codes.FailedPrecondition`：进行中，DTM 重试
- 其他错误：基础设施故障，DTM 重试

## 常见问题排查

- `no instance remains for <service>`：注册中心中没有健康实例。对 Consul 而言，请确认注册的地址从 Consul agent 可达。
- `BarrierFromGrpc` 失败：请在该分支服务端安装 `ServerMetadataMiddleware`。
- DTM 忽略 `conf.yaml`：请用 `-c <path>` 启动 DTM。
- 业务错误变成带 `[biz error]` 后缀的 `Internal`：请返回携带预期错误码的 nphttp2 status 错误。

## 兼容性

- Go 1.22 及以上
- Kitex v0.12.x、grpc-go v1.59.x、dtmdriver v0.0.6、DTM client v1.18.x
- `registry-etcd` 保持在 v0.2.6，因为 v0.3.0 需要 Go 1.23

## 许可证

MIT
