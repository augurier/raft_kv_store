# go-raft-kv

---

基于go语言实现分布式kv数据库  

# 框架
每个运行main.go进程作为一个节点  
``` 
---internal  
    ---client 客户端使用节点提供的读写功能  
    ---logprovider 封装了简单的日志打印，方便调试  
    ---nodes 分布式核心代码
        init.go 节点在main中的调用初始化，和大循环启动  
        log.go 节点存储的entry相关数据结构  
        node_storage.go 抽象了节点数据持久化方法，序列化后存到leveldb里  
        node.go 节点的相关数据结构  
        replica.go 日志复制相关逻辑  
        server_node.go 节点作为server为  client提供的功能（读写）  
        vote.go 选主相关逻辑
```

# 环境与运行
使用环境是wsl+ubuntu  
go mod download安装依赖  
./scripts/build.sh 会在根目录下编译出main

# 注意
脚本第一次运行需要权限获取 chmod +x <脚本>  
如果出现tcp listen error可能是因为之前的进程没用正常退出，占用了端口  
lsof -i :9091查看pid  
kill -9 <pid>杀死进程  

## 关于测试
test/ 测试真实rpc，通过新开进程（main）的方式创建节点（参考test/common.go中executeNodeI函数） 
threadTest/ 测试线程模拟，参考threadTest/common.go中executeNodeI函数启动node


## 客户端工作原理
客户端每次会随机连上集群中一个节点，此时有四种情况：  
a 节点认为自己是leader，直接处理请求  
b 节点认为自己是follower，且有知道的leader，返回leader的id。客户端再连接这个新的id，新节点重新分析四种情况。  
c 节点认为自己是follower，但不知道leader是谁，返回空的id。客户端再随机连一个节点  
d 连接超时，客户端重新随机连一个节点  
