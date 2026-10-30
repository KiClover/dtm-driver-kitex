# DTM Kitex 插件调研报告

> 状态：调研完成，尚未开始实现  
> 调研日期：2026-10-29（Asia/Hong_Kong）  
> DTM 本地基线：`18146ee53bafbf094b1a5f12ca7e8a29bdb57edd`（`main`）  
> 目标：为 DTM 增加调用 Kitex gRPC 服务、接入 Kitex 服务发现，以及在 Kitex 业务服务中复用 DTM 子事务屏障的能力。

## 1. 结论摘要

DTM Kitex 插件在技术上可行，协议层风险较低。DTM 的 gRPC 分支由 grpc-go 动态调用，不依赖业务生成的 client stub；Kitex 的 gRPC transport 可以接收标准 grpc-go 请求。因此不需要改造 DTM 的事务调度模型，也不需要把 DTM Server 改造成 Kitex Server。

插件的核心不是重新实现事务协议，而是补齐三个适配层：

1. **DTM Driver**：实现 `dtmdriver.Driver`，负责 URI 解析、grpc-go resolver 注册和 DTM Server 的服务注册。
2. **Discovery Bridge**：把 Kitex discovery 结果适配为 grpc-go `resolver.Builder` / `resolver.Resolver`。
3. **Metadata Bridge**：把 Kitex context 中的 incoming metadata 复制到 grpc-go metadata context，使现有 `dtmgrpc.BarrierFromGrpc(ctx)` 可以正常读取事务信息。

推荐先完成“直连 + metadata bridge + Saga barrier”的最小纵向切片，再接入一个实际使用的注册中心。第一版不应同时抽象 etcd、Nacos、Consul，也不应顺带实现 Hertz。

## 2. 调研范围与证据等级

本文使用以下标记：

- **已证实**：由当前 DTM 本地源码、上游源码或官方交叉框架测试直接支持。
- **建议设计**：基于现有接口和风险分析给出的实现建议，尚不是已发布契约。
- **待实测**：需要在目标 Kitex、注册中心和部署环境中运行集成测试后确认。

本地 DTM 基线的关键依赖为：

| 项目 | 当前值 |
| --- | --- |
| Go | `1.22` |
| `github.com/dtm-labs/dtmdriver` | `v0.0.6` |
| `google.golang.org/grpc` | `v1.56.3` |
| `google.golang.org/protobuf` | `v1.34.1` |

DTM 当前没有直接依赖 Kitex。插件适合做成独立 Go module，避免将 Kitex及具体注册中心依赖引入 DTM 核心。

## 3. DTM 当前如何调用 gRPC 分支

### 3.1 动态调用，不依赖业务 client stub

**已证实**：`client/dtmgrpc/dtmgimp/types.go` 中的 `InvokeBranch(...)` 会：

1. 调用当前 driver 的 `ParseServerMethod(url)`；
2. 得到 grpc-go Dial target 和完整方法名；
3. 注入 DTM metadata；
4. 调用 `ClientConn.Invoke(ctx, method, msg, reply, opts...)`。

因此，Kitex 插件只需让 grpc-go 能够解析目标地址，并保证完整方法名与 Kitex IDL 一致。推荐 URI 形式为：

```text
kitex:///order-service/order.v1.OrderService/Submit
```

建议解析为：

```text
server = kitex:///order-service
method = /order.v1.OrderService/Submit
```

`ParseServerMethod` 必须在边界处严格校验：

- scheme 必须为 `kitex`；
- 服务名不能为空；
- 方法必须符合 `/package.Service/Method`；
- 不接受多余 path segment；
- query、fragment 和 URL 转义的支持范围必须明确；
- 错误应稳定、可判定，不依赖 panic 或字符串下标越界。

上述 URI 是**建议契约**，应在实现前结合目标注册中心最终确认。一旦发布，就不应随意改变其可观察解析行为。

### 3.2 raw codec 是必须覆盖的真实路径

这是本次调研最容易误判的地方。

DTM 有两条不同的 gRPC 调用路径：

1. 业务代码主动调用 `InvokeBranch(...)` 时，由调用方传入 `isRaw`；typed protobuf 一般使用 `isRaw=false`。
2. DTM Server 调度已持久化的 Saga、Msg 等事务分支时，`dtmsvr/trans_status.go` 的 `getGrpcResult(...)` 固定调用 `MustGetGrpcConn(server, true)`，再把 `branchPayload []byte` 传给 `ClientConn.Invoke()`。

`isRaw=true` 时，DTM 的 `rawCodec.Marshal` 直接返回传入的 `[]byte`。这不是一种非标准的线上消息格式，而是避免 DTM 在不知道业务请求类型时对已经序列化的 protobuf 再次 marshal。线路上的 payload 仍是业务 protobuf bytes，Kitex 服务端应按目标方法的请求类型正常解码。

因此，不能只用 typed client 调用证明插件可用。首个集成测试必须覆盖：

```text
DTM Saga
  -> getGrpcResult(isRaw=true)
  -> grpc-go ClientConn.Invoke
  -> Kitex protobuf gRPC server
  -> metadata bridge
  -> BarrierFromGrpc
  -> action / compensate
```

## 4. grpc-go 与 Kitex gRPC 的协议兼容性

**已证实**：Kitex gRPC transport 基于标准 HTTP/2，服务端会从 gRPC path 中解析 package、service 和 method，按 protobuf codec 解码请求并返回标准 gRPC status。

CloudWeGo 官方测试仓库包含 grpc-go client 调用 Kitex server 的交叉框架测试，覆盖 unary、client streaming、server streaming 和 bidirectional streaming。这足以支持“grpc-go `ClientConn.Invoke` 可以调用普通 Kitex protobuf unary handler”的判断。

成立条件：

- 服务由兼容的 proto3 IDL 生成；
- Kitex Server 启用 gRPC transport；
- proto package、service 和 method 全名一致；
- DTM raw 路径传入的是该方法请求类型已经序列化后的 protobuf bytes。

**待实测**：

- deadline 和 cancellation；
- gzip；
- 最大消息大小；
- TLS、authority 和 SNI；
- status details；
- DTM 当前 grpc-go `v1.56.3` 与选定 Kitex 版本的组合。

## 5. Metadata Bridge：必要组件

### 5.1 DTM 使用的 metadata

**已证实**：DTM 通过 grpc-go `metadata.AppendToOutgoingContext` 写入以下键：

```text
dtm-gid
dtm-trans_type
dtm-branch_id
dtm-op
dtm-dtm
```

`client/dtmgrpc/dtmgimp/utils.go` 的 `TransBaseFromGrpc(ctx)` 只使用 grpc-go 的 `metadata.FromIncomingContext(ctx)` 读取这些值。`BarrierFromGrpc(ctx)` 随后要求 `trans type`、`gid`、`branch ID`、`op` 四项非空，否则返回 `invalid trans info`。

### 5.2 为什么默认读取不到

Kitex 与 grpc-go 在线路上的 metadata 兼容，但它们在 Go 进程内：

- 使用不同 package 中定义的 `MD` 类型；
- 使用各自私有的 context key。

所以，即使 header 已到达 Kitex handler，grpc-go 的 `metadata.FromIncomingContext(ctx)` 也看不到 Kitex 放入 context 的值。没有 bridge 时，`BarrierFromGrpc(ctx)` 会得到空事务信息。

### 5.3 推荐行为

**建议设计**：提供 Kitex server middleware：

1. 使用 `github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/metadata` 读取 incoming metadata；
2. 复制为 `google.golang.org/grpc/metadata.MD`；
3. 使用 `grpcmetadata.NewIncomingContext(ctx, md)` 创建新 context；
4. 将新 context 交给后续 Kitex handler。

bridge 应保留所有 metadata，而不只复制五个 DTM 键，以便现有 branch headers 继续工作；同时不得修改 gid、branch ID、op 或多值顺序。

建议对外暴露一个语义明确的 Kitex middleware，例如 `DTMMetadataBridge`。具体 Go 名称和签名应以目标 Kitex 版本的 `endpoint.Middleware` 类型为准，避免在调研阶段过早锁死 API。

可以附带 `BarrierFromKitex(ctx)` helper，直接读取 Kitex metadata 后调用 `dtmcli.BarrierFrom(...)`。但它应是显式替代方案，而不是唯一方案；middleware bridge 对已有大量 `BarrierFromGrpc` 业务代码的迁移成本更低。

## 6. Discovery Bridge：必要组件

### 6.1 两套 Resolver 接口不能直接互换

**已证实**：Kitex `discovery.Resolver` 的核心行为是：

```go
Target(ctx context.Context, target rpcinfo.EndpointInfo) string
Resolve(ctx context.Context, desc string) (Result, error)
Diff(cacheKey string, prev, next Result) (Change, bool)
Name() string
```

grpc-go `v1.56.3` 需要 `resolver.Builder` 和 `resolver.Resolver`，并由 adapter 主动调用：

```go
ClientConn.UpdateState(resolver.State{...})
ClientConn.ReportError(err)
```

两者在目标类型、实例类型、更新方向和生命周期上都不兼容，不能直接注册。

### 6.2 推荐 adapter 行为

**建议设计**：实现 grpc-go resolver builder/resolver：

- `Scheme()` 返回 `kitex`；
- `Build()` 从 `target.Endpoint()` 提取逻辑服务名；
- 调用 Kitex resolver 获取实例；
- 将 `Instance.Address().String()` 转换为 `resolver.Address.Addr`；
- 地址去重并对稳定状态做一致性比较；
- 通过 `ClientConn.UpdateState` 推送地址；
- 发现失败时调用 `ClientConn.ReportError`；
- `ResolveNow()` 触发重新发现；
- `Close()` 停止 watch、轮询和后台 goroutine，且应幂等。

Kitex 通用 discovery 接口没有统一的长期 watch 契约：

- 如果选定的注册中心扩展暴露 watch，应使用其原生 watch；
- 如果只有 `Resolve`，可响应 grpc-go `ResolveNow`，必要时增加有界轮询；
- 轮询周期、退避、抖动和关闭行为必须可测试，不能产生 goroutine 泄漏。

Kitex 的 weight/tag 可以映射到 `resolver.Address.Attributes`，但 grpc-go 不会自动采用相同路由语义。MVP 建议使用 DTM 已配置的 `round_robin`，并明确“不保证保留 Kitex 权重和标签路由”。如果将来需要权重路由，应单独设计 grpc-go balancer，而不是悄悄改变 resolver 行为。

## 7. Driver 职责与模块边界

`dtmdriver.Driver` 当前契约为：

```go
type Driver interface {
    GetName() string
    RegisterAddrResolver()
    RegisterService(target string, endpoint string) error
    ParseServerMethod(uri string) (server string, method string, err error)
}
```

建议建立独立模块，暂称 `dtmdriver-kitex`。推荐内部边界：

```text
dtmdriver-kitex/
  driver/           # dtmdriver.Driver、URI 解析、注册入口
  resolverbridge/   # Kitex discovery -> grpc-go resolver
  metabridge/       # Kitex incoming metadata -> grpc-go incoming metadata
  barrier/          # 可选的 BarrierFromKitex 等便捷 API
  integrationtest/  # 真实 DTM Server + Kitex Server + 注册中心
```

公开 API 应保持最小：

- driver name 常量或注册入口；
- Kitex metadata middleware；
- 必要的、稳定的构造配置；
- 可选的 `BarrierFromKitex` helper。

grpc-go resolver 的具体实现类型、缓存、轮询器和地址比较逻辑应优先保持内部可见，避免将实现细节变成长期兼容负担。

### 7.1 `RegisterService` 的含义

DTM Server 本身当前仍是 grpc-go Server，默认监听 `36790`。Kitex driver 的 `RegisterService(target, endpoint)` 是把该 grpc-go endpoint 按目标注册中心所需的服务名和元数据注册出去；它不会把 DTM Server 改造成 Kitex Server。

这里需要在选定首个注册中心后明确：

- `target` 的 URI/JSON 格式；
- 注册的 service name；
- endpoint 的 host/port 和 IPv6 表达；
- protocol/tag 元数据；
- 重复注册语义；
- 进程退出时的反注册机制。

`dtmdriver.Driver` 没有 `Close()`，因此反注册和后台任务生命周期是实际风险。第一版不能只实现 `Register` 而忽略退出行为；至少应定义可测试的 cleanup 机制，并确认 DTM 进程如何调用它。如果上游接口无法承载，应把限制写入文档，不要伪装成完整生命周期支持。

### 7.2 全局单 driver 限制

`dtmdriver.Use(name)` 是进程级全局选择，第一次使用时调用 `RegisterAddrResolver()`，之后不能自由切换。影响包括：

- 单元测试不能无约束并行切换 driver；
- 同一 DTM 进程不能直接同时 `Use` 两个独立 driver；
- resolver scheme 的全局注册必须避免重复注册 panic；
- 配置和测试需要控制进程级状态。

MVP 只支持 Kitex gRPC driver 即可，不需要为未来双栈提前改 DTM 核心。

## 8. 错误语义

插件必须保留 DTM 已有 gRPC 错误契约，不能自行发明新的重试规则。

当前 DTM 映射为：

- `codes.Aborted` -> 事务失败；旧兼容条件下，message 为 `ONGOING` 时仍视为进行中；
- `codes.FailedPrecondition` -> 事务进行中，DTM 后续重试；
- 其他错误 -> 保留原错误，通常进入 DTM 未知错误重试路径。

业务 Kitex handler 应继续使用 DTM 提供的错误转换语义，集成测试至少覆盖：

- 成功；
- 明确失败；
- 明确进行中；
- 未知 gRPC error；
- deadline exceeded；
- 方法不存在时的 `Unimplemented`。

resolver/discovery 错误不能伪装成业务失败，否则可能错误终止 Saga；应作为连接/发现错误进入可重试路径。

## 9. 推荐实施阶段

### 阶段 1：最小纵向切片

目标是证明真实 DTM 调度链路，而不是只证明 grpc-go 能调用 Kitex。

1. 启动真实 Kitex proto3 unary gRPC Server。
2. 先用 grpc-go `ClientConn.Invoke` 直连验证协议互操作。
3. 实现并安装 metadata bridge。
4. 实现最小 driver URI 解析；服务地址先直连，不接注册中心。
5. 运行真实 DTM Saga action/compensate。
6. 在 Kitex handler 中调用 `BarrierFromGrpc`，验证幂等、空补偿和悬挂处理。
7. 同时覆盖 typed `isRaw=false` 和 DTM Server persisted payload `isRaw=true`。

阶段 1 的完成标准：Saga action/compensate 经真实 DTM Server 调度成功，事务 metadata 正确，重复请求不重复执行业务，补偿语义符合 barrier 预期。

### 阶段 2：一个真实注册中心

只选择项目实际需要的一个注册中心：

1. 实现 discovery -> grpc-go resolver adapter；
2. 实现 DTM Server 的注册和反注册；
3. 测试实例新增、删除、服务重启、注册中心断连和恢复；
4. 验证空实例窗口与 `ResolveNow`；
5. 跑 race test 和 goroutine 泄漏检查。

### 阶段 3：扩展事务和生产能力

在前两阶段稳定后再考虑：

- TCC confirm/cancel；
- Msg 与 Workflow；
- TLS、authority、SNI；
- gzip 和大消息；
- 权重/标签路由；
- 其他注册中心；
- 更完整的可观测性。

## 10. 测试清单

### URI 与边界校验

- 合法 URI 与完整 gRPC method；
- 空 scheme、错误 scheme、空服务名、空方法；
- 缺少 package/service/method；
- 多余 segment、query、fragment、URL 转义；
- IPv4、IPv6；
- 错误不 panic。

### 协议互操作

- grpc-go `Invoke` -> Kitex unary handler；
- typed protobuf 与 raw persisted payload；
- `Unimplemented`、deadline、cancel；
- gzip、最大消息、TLS、authority、SNI。

### Metadata 与 Barrier

- 五个 DTM metadata 键全部到达；
- 自定义 branch headers 被保留；
- bridge 后 grpc-go metadata 可读取同值；
- 大小写、多值、空值；
- 缺少四个必要字段中的任一项时明确失败；
- 并发请求之间不存在 metadata 串号；
- Saga 幂等、空补偿、悬挂处理。

### Resolver 与生命周期

- 首次解析、多实例、地址去重；
- 实例新增、删除、地址更新；
- 空实例和发现错误；
- 注册中心断连与恢复；
- `ResolveNow` / `Close` 并发；
- 多次 `Close`；
- DTM Server 反注册；
- round-robin 基本分布；
- race test 和 goroutine 泄漏检查。

### 版本矩阵

- 本报告记录的 DTM main 基线；
- 一个计划支持的 DTM release；
- `dtmdriver v0.0.6`；
- grpc-go `v1.56.3`；
- 选定 Kitex 版本；
- 选定注册中心插件版本。

## 11. 主要风险与未决项

| 风险/未决项 | 影响 | 建议 |
| --- | --- | --- |
| Kitex 与 grpc-go 使用不同 metadata context key | Barrier 读不到事务信息 | metadata bridge 必须进入 MVP |
| DTM Server persisted branch 固定使用 raw codec | 仅 typed 测试会产生假阳性 | 集成测试覆盖 `isRaw=true` |
| Kitex discovery 没有统一 watch 契约 | 实例更新可能滞后 | 首个注册中心使用原生 watch；否则有界轮询 |
| weight/tag 与 grpc-go balancer 语义不同 | 路由行为不一致 | MVP 明确只保证 round-robin |
| `dtmdriver.Driver` 没有 `Close` | 反注册或 goroutine 泄漏 | 明确定义 cleanup，必要时提出上游扩展 |
| `dtmdriver.Use` 是全局单例 | 并行测试和多 driver 受限 | 串行隔离测试，不在 MVP 改核心 |
| grpc-go 当前默认 insecure | 生产安全配置不足 | TLS 放入后续独立设计，不静默改变默认值 |
| driver URI 一旦发布即形成契约 | 后续兼容成本 | 在首个 registry 确认后冻结 URI |

进入实现前仍需做两个选择：

1. 第一阶段是否只支持直连，还是同时选定一个注册中心；本报告推荐先直连。
2. 第二阶段第一个注册中心是哪一个；应由实际部署环境决定，而不是为了“通用”同时实现多个。

## 12. Hertz 的范围边界

`dtmdriver-gozero` 当前是纯 gRPC driver，没有 HTTP resolver 或 Resty middleware，不能作为 Hertz HTTP driver 的现成实现。

DTM 的通用 SPI 确实支持 HTTP：driver 可以向 `dtmdriver.Middlewares.HTTP` 添加 Resty `OnBeforeRequest` middleware，Spring Cloud 和 Dapr driver 已采用该模式。但是这不属于 Kitex MVP。

只有在 DTM HTTP 分支需要通过 CloudWeGo 注册中心按逻辑服务名发现 Hertz 实例时，才需要独立的 `dtmdriver-hertz`：

- 直连 URL、Kubernetes Service/DNS 或网关地址：不需要 Hertz driver；
- 逻辑服务名、客户端实例发现和负载均衡：需要 Hertz driver。

Kitex 与 Hertz 应保持独立 module/package，因为两者分别适配 grpc-go resolver 与 Resty HTTP middleware。若未来同一 DTM 进程必须同时使用两者，可增加薄的组合 facade；不要在当前 Kitex driver 中硬耦合 Hertz。

另外，DTM 的配置结构仍保留 `HTTPMicroService`，但当前 `StartSvr()` 只读取 `MicroService` 并调用一次 `RegisterService`，没有接线 `HTTPMicroService`。因此“双端口同时注册”不能仅靠把 Kitex/Hertz 放进同一仓库解决，未来需要组合配置或修改 DTM 启动 wiring。

## 13. 建议的下一次对话起点

下一次在本项目继续时，建议先确认以下一句话范围：

> 实现 DTM Kitex 插件第一阶段：独立 Go module，先做直连 URI、Kitex metadata bridge 和真实 Saga action/compensate 集成测试；暂不接注册中心，不做 Hertz，不修改 DTM 核心事务逻辑。

随后按以下顺序工作：

1. 选定插件 module/repository 名称和目标 Kitex 版本；
2. 固化第一阶段 URI 契约；
3. 先写 raw/typed 两条失败的集成测试；
4. 实现 metadata bridge；
5. 实现最小 driver；
6. 跑通真实 Saga barrier；
7. 再决定第二阶段注册中心。

## 14. 关键资料

DTM：

- [DTM 当前仓库](https://github.com/dtm-labs/dtm)
- [dtmdriver 接口](https://github.com/dtm-labs/dtmdriver/blob/main/driver.go)
- [go-zero driver](https://github.com/dtm-labs/dtmdriver-gozero/blob/main/driver.go)
- [Kratos driver](https://github.com/dtm-labs/dtmdriver-kratos/blob/main/driver.go)
- [Polaris driver](https://github.com/dtm-labs/dtmdriver-polaris/blob/main/driver.go)
- [Spring Cloud HTTP driver](https://github.com/dtm-labs/dtmdriver-springcloud/blob/master/driver.go)
- [Dapr 双栈 driver](https://github.com/dtm-labs/dtmdriver-dapr/blob/main/driver.go)

Kitex / grpc-go：

- [Kitex gRPC transport 文档](https://www.cloudwego.io/docs/kitex/tutorials/basic-feature/protocol/transport_protocol/)
- [Kitex gRPC server handler](https://github.com/cloudwego/kitex/blob/develop/pkg/remote/trans/nphttp2/server_handler.go)
- [Kitex metadata 实现](https://github.com/cloudwego/kitex/blob/develop/pkg/remote/trans/nphttp2/metadata/metadata.go)
- [Kitex discovery 接口](https://github.com/cloudwego/kitex/blob/develop/pkg/discovery/discovery.go)
- [Kitex registry 接口](https://github.com/cloudwego/kitex/blob/develop/pkg/registry/registry.go)
- [grpc-go resolver v1.56.3](https://github.com/grpc/grpc-go/blob/v1.56.3/resolver/resolver.go)
- [grpc-go client 调用 Kitex server 的官方测试](https://github.com/cloudwego/kitex-tests/blob/main/kitexgrpc/compressor/grpc_compressor_test.go)

## 15. 最终建议

可以立项实现 `dtmdriver-kitex`。最小可行版本应聚焦：

```text
直连 URI
  + grpc-go -> Kitex 协议互操作
  + Kitex metadata bridge
  + DTM Saga action/compensate
  + BranchBarrier
  + typed/raw 两条调用路径
```

完成上述纵向切片后，再加入一个真实注册中心的 discovery/resolver bridge。Hertz、多个注册中心、权重路由、TLS 和 DTM 核心双栈 wiring 都应后置，避免在协议和 barrier 尚未被真实链路验证前扩大范围。
