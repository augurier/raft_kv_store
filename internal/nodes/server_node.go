package nodes

import "strconv"

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
	log.Info("server write : logId = " + strconv.Itoa(logId) + ", key = " + kv.Key)
	node.BroadCastKV(logId, kv)
	reply.Isconnect = true
	return nil
}
// RPC call
func (node *Node) ReadKey(key string, reply *ServerReply) error {
	log.Info("server read : " + key)
	// 先只读leader自己
	for _, kv := range node.log {
		if kv.Key == key {
			reply.Value = kv.Value
			reply.HaveValue = true
			reply.Isconnect = true
			return nil
		}
	}
	reply.HaveValue = false
	reply.Isconnect = true
	return nil
}

