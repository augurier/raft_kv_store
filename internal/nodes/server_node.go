package nodes

import (
	"github.com/syndtr/goleveldb/leveldb"
)

// leader node作为server为client注册的方法
type ServerReply struct{
	Isleader bool
	LeaderId string // 自己不是leader则返回leader
	HaveValue bool
	Value string
}
// RPC call
func (node *Node) WriteKV(kvCall *LogEntryCall, reply *ServerReply) error {
	log.Sugar().Infof("[%s]收到客户端write请求", node.SelfId)

	// 自己不是leader，转交leader地址回复	
	if node.State != Leader {
		reply.Isleader = false
		reply.LeaderId = node.LeaderId // 可能是空，那client就随机再找一个节点
		log.Sugar().Infof("[%s]转交给[%s]", node.SelfId, node.LeaderId)
		return nil
	}

	// 自己是leader，修改自己的记录并广播
	node.MaxLogId++
	logId := node.MaxLogId
	rLogE := RaftLogEntry{kvCall.LogE, logId, node.CurrTerm}
	node.Log = append(node.Log, rLogE)
	node.Storage.AppendLog(rLogE)
	log.Info("leader[" + node.SelfId + "]处理请求 : " + kvCall.LogE.print())
	// 广播给其它节点	
	node.BroadCastKV()
	reply.Isleader = true
	return nil
}

// RPC call
func (node *Node) ReadKey(key *string, reply *ServerReply) error {
	log.Sugar().Infof("[%s]收到客户端read请求", node.SelfId)
	// 先只读自己(无论自己是不是leader)，也方便测试
	value, err := node.Db.Get([]byte(*key), nil)
	if err == leveldb.ErrNotFound {
		reply.HaveValue = false
	} else {
		reply.HaveValue = true
		reply.Value = string(value)
	}
	reply.Isleader = true
	return nil
}

// RPC call 测试中寻找当前leader
type FindLeaderReply struct{
	Isleader bool
	LeaderId string
}
func (node *Node) FindLeader(_ struct{}, reply *FindLeaderReply) error {
	// 自己不是leader，转交leader地址回复	
	if node.State != Leader {
		reply.Isleader = false
		if (node.LeaderId == "") {
			log.Fatal("还没选出第一个leader")
			return nil
		}
		reply.LeaderId = node.LeaderId
		return nil
	}

	reply.LeaderId = node.SelfId
	reply.Isleader = true
	return nil
}
