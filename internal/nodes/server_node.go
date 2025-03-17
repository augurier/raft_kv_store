package nodes

import (
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
)

// leader node作为server为client注册的方法
type ServerReply struct{
	Isleader bool
	LeaderAddress string // 自己不是leader则返回leader地址
	HaveValue bool
	Value string
}
// RPC call
func (node *Node) WriteKV(kvCall LogEntryCall, reply *ServerReply) error {
	log.Info(node.selfId + "收到客户端write请求")	
	if node.state != Leader {
		reply.Isleader = false
		if (node.leaderId == "") {
			log.Fatal("还没选出第一个leader")
			return nil
		}
		reply.LeaderAddress = node.nodes[node.leaderId].address
		log.Info(node.selfId + "转交给" + node.leaderId)
		return nil
	}

	node.maxLogId++
	logId := node.maxLogId
	rLogE := RaftLogEntry{kvCall.LogE, logId, node.currTerm}
	node.log = append(node.log, rLogE)
	node.storage.AppendLog(rLogE)
	log.Info("leader" + node.selfId + "处理请求 : " + kvCall.LogE.print() + ", 模拟方式 : " + strconv.Itoa(int(kvCall.CallState)))
	// 广播给其它节点	
	node.BroadCastKV(kvCall.CallState)
	reply.Isleader = true
	return nil
}
// RPC call
func (node *Node) ReadKey(key string, reply *ServerReply) error {
	log.Info("server read : " + key)
	// 先只读自己(无论自己是不是leader)，也方便测试
	value, err := node.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		reply.HaveValue = false
	} else {
		reply.HaveValue = true
		reply.Value = string(value)
	}
	reply.Isleader = true
	return nil
}

