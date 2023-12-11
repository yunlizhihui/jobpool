# 安全性

## 启用Auth方式认证（用户名、密码）
```shell
# 添加用户，同时设置密码(如123456）
./jobpoolctl user add root
# 启用auth
./jobpoolctl auth enable
# 设置内容
./jobpoolctl --user='root' --password='123456'  put name "zhangsan"
# 获取内容
./jobpoolctl --user='root' --password='123456'  get name
# 获取节点这种不需要auth认证
./jobpoolctl member list
# 调度逻辑目前不需要进行auth认证
./jobpoolctl plan list
```