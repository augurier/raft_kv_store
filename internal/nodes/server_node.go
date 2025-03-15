package nodes

import (
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
)

// leader node作为server为client注册的方法
type ServerReply struct{
	Isconnect bool
	HaveValue bool
	Value string
}
// RPC call
func (node *Node) WriteKV(kvCall LogEntryCall, reply *ServerReply) error {	
	node.maxLogId++
	logId := node.maxLogId
	node.log = append(node.log, RaftLogEntry{kvCall.LogE, logId, node.currTerm})
	// node.db.Put([]byte(kvCall.LogE.Key), []byte(kvCall.LogE.Value), nil)
	log.Info("server write request : " + kvCall.LogE.print() + ", 模拟方式 : " + strconv.Itoa(int(kvCall.CallState)))
	// 广播给其它节点	
	node.BroadCastKV(kvCall.CallState)
	reply.Isconnect = true
	return nil
}
// RPC call
func (node *Node) ReadKey(key string, reply *ServerReply) error {
	log.Info("server read : " + key)
	// 先只读leader自己
	value, err := node.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		reply.HaveValue = false
	} else {
		reply.HaveValue = true
		reply.Value = string(value)
	}
	reply.Isconnect = true
	return nil
}

