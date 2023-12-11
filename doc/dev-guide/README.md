
## client修改顺序

client/v3/schedule.go --添加client
client/v3/retry.go -- 添加重试逻辑
client/v3/client.go -- 添加新的业务逻辑参数


## server 修改顺序

### 相关代理逻辑
server/proxy/grpcproxy/adapter 增加adpater用于组装server端的client
server/proxy/grpcproxy/schedule.go 增加新业务的proxy逻辑
server/etcdmain/grpc_proxy.go 增加对应的proxy
server/etcdserver/api/v3client/v3client.go 增加客户端业务

### raft逻辑


#### 涉及到存储相关 mvcc业务
server/mvcc/buckets/bucket.go 增加bucket
server/auth/store_schedule.go 这里增加具体数据库操作逻辑（TODO索引部分尚未实现cache）

#### server相关内容
server/etcdserver/server.go 增加对应业务的store

#### 网络相关[添加后http访问才有反应]
server/etcdserver/api/v3rpc/grpc.go 注册地址
server/embed/serve.go 注册handler


#### raft提交相关
server/etcdserver/apply.go 相关的应用