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
func (node *Node) WriteKV(kv LogEntry, reply *ServerReply) error {
	
	logId := node.maxLogId
	node.maxLogId++
	node.log[logId] = kv
	// 广播给其它节点
	node.db.Put([]byte(kv.Key), []byte(kv.Value), nil)
	log.Info("server write : logId = " + strconv.Itoa(logId) + ", key = " + kv.Key)
	node.BroadCastKV(logId, kv)
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

