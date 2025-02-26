# go-raft-kv

---

基于go语言实现分布式kv数据库  

# 环境与运行
使用环境是wsl+ubuntu  
go mod download安装依赖  
./scripts/build.sh 会在根目录下编译出raftnode
./scripts/run.sh 运行三个节点，目前能在终端进行读入，leader（n1）节点输出send log，其余节点输出receive log。终端输入后如果超时就退出（脚本运行时间可以在其中调整）。

# 注意
脚本第一次运行需要权限获取 chmod +x <脚本>  
如果出现tcp listen error可能是因为之前的进程没用正常退出，占用了端口  
lsof -i :9091查看pid  
kill -9 <pid>杀死进程  

# todo list
消息通讯异常的处理  
kv本地持久化  
崩溃与恢复（以及对应的测试）