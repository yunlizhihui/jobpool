# 运行效果
    doc/*.sh
# K8S上运行配置相关
    doc/k8s_yaml
# ETCD架构
    见文章

# server 主逻辑

## server/main.go
    etcdmain.Main(os.Args)

## server/etcdmain/main.go
    startEtcdOrProxyV2(args)

## server/etcdmain/etcd.go
    startEtcdOrProxyV2(args []string)
    startEtcd(cfg *embed.Config)

## server/embed/etcd.go
    e.serveClients()

## server/embed/serve.go
    func (sctx *serveCtx) serve(....)
        grpc+http
        v3client.New(s)
            server/etcdserver/api/v3client/v3client.go
                adapter.ScheduleServerToScheduleClient(v3rpc.NewScheduleServer(s))
        gwmux, err = sctx.registerGateway(grpcDialForRestGatewayBackends)

# 请求后的逻辑
    以计划列表查询为例：
## server/etcdserver/v3_server.go
    func (s *EtcdServer) PlanList
        s.raftRequest
## server/etcdserver/apply.go
    进入到raft逻辑
    func (a *applierV3backend) Apply(....
        case r.SchedulePlanList != nil:
        a.s.applyV3.PlanList(r.SchedulePlanList)
    func (a *applierV3backend) PlanList(
        resp, err := a.s.ScheduleStore().PlanList(r)
## server/scheduler/store_schedule.go
    func (as *scheduleStore) PlanList(
        。。 读取boltdb获取数据

# 调度器与执行器
    见代码
    [api]-domain、rpc、gateway及生成(scripts)
    [server]-v3_server、apply、store
    [agent]-心跳、执行逻辑


# 遗留问题
    1、分页查询、排序
    2、通用的过滤机制
    3、垃圾回收机制（保留7日内数据，执行器下线后清理）
    4、执行器获取资源情况（节点资源、yarn资源等），以便后续做优化调度
    5、任务地图
    7、多租户及默认租户
    8、大数据量的性能测试（如百万级任务后内存占用、CPU占用）
    9、其他
    10、验证角色用户权限相关