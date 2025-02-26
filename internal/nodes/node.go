package nodes

import (
	"net/rpc"
	"go.uber.org/zap"
)

type State = uint8

const (
	Follower State = iota + 1
	Candidate
	Leader
)

type Public_node_info struct {
	connect bool
	address string
}

type Node struct {
	// 当前节点id
	self int

	// 除当前节点外其他节点信息
	nodes map[int]*Public_node_info

	//管道名
	pipeAddr string

	// 当前节点状态
	state State

	// 简单的kv存储
	log []LogEntry
}

func (node *Node) BroadCastKV(kv LogEntry) {
	// 遍历所有节点
	for i := range node.nodes {
		go func (index int, kv LogEntry)  {
			var reply KVReply
			node.sendKV(index, kv, &reply)
		} (i, kv)
	}
}

func (node *Node) sendKV(index int, kv LogEntry, reply *KVReply) {
	client, err := rpc.DialHTTP("tcp", node.nodes[index].address)
	if err != nil {
		log.Error("dialing: ", zap.Error(err))
		return
	}

	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Error("client close err: ", zap.Error(err))
		}
	}(client)

	callErr := client.Call("Node.ReceiveKV", kv, reply) // RPC
	if callErr != nil {
		log.Error("dialing: ", zap.Error(callErr))
	}

	if reply.Reply { // 发送成功

	} else { // 失败

	}
}

// RPC call
func (node *Node) ReceiveKV(kv LogEntry, reply *KVReply) error {
	log.Info("receive: " + kv.Key)
	reply.Reply = true;
	return nil
}
// func (node *Node) broadcastHeartbeat() {
// 	// 遍历所有节点
// 	for i := range raft.nodes {
// 		// request 参数
// 		hb := Heartbeat{
// 			Term:        raft.currTerm,
// 			LeaderId:    raft.self,
// 			CommitIndex: raft.commitIndex,
// 		}

// 		prevLogIndex := raft.nextIndex[i] - 1

// 		// 如果有日志未同步则发送
// 		if raft.getLastIndex() > prevLogIndex {
// 			hb.PrevLogIndex = prevLogIndex
// 			hb.PrevLogTerm = raft.log[prevLogIndex].CurrTerm
// 			hb.Entries = raft.log[prevLogIndex:]
// 			// log.Info("will send log entries", zap.Any("logEntries", hb.Entries))
// 		}

// 		go func(index int, hb Heartbeat) {
// 			var reply HeartbeatReply
// 			// 向某一个节点发送 heartbeat
// 			raft.sendHeartbeat(index, hb, &reply)
// 		}(i, hb)
// 	}
// }

