package nodes

import (
	"math/rand"
	"net/rpc"
	"strconv"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
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
	selfId string

	// 除当前节点外其他节点信息
	nodes map[string]*Public_node_info

	//管道名
	pipeAddr string

	// 当前节点状态
	state State

	// 简单的kv存储
	log map[int]LogEntry

	// leader用来标记新log
	maxLogId int

	db *leveldb.DB
}

func (node *Node) BroadCastKV(logId int, kvCall LogEntryCall) {
	// 遍历所有节点
	for id, _ := range node.nodes {
		go func(id string, kv LogEntryCall) {
			var reply KVReply
			node.sendKV(id, logId, kvCall, &reply)
		}(id, kvCall)
	}
}

func (node *Node) sendKV(id string, logId int, kvCall LogEntryCall, reply *KVReply) {
	switch kvCall.CallState {
	case Fail:
		log.Info("模拟发送失败")
		// 这么写向所有的node发送都失败，也可以随机数确定是否失败
	case Delay:
		log.Info("模拟发送延迟")
		// 随机延迟0-5ms
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)))
	default:
	}

	client, err := rpc.DialHTTP("tcp", node.nodes[id].address)
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

	arg := LogIdAndEntry{logId, kvCall.LogE}
	callErr := client.Call("Node.ReceiveKV", arg, reply) // RPC
	if callErr != nil {
		log.Error("dialing node_"+id+"fail: ", zap.Error(callErr))
	}
}

// RPC call
func (node *Node) ReceiveKV(arg LogIdAndEntry, reply *KVReply) error {
	log.Info("node_" + node.selfId + " receive: logId = " + strconv.Itoa(arg.LogId) + ", key = " + arg.Entry.Key)
	entry, ok := node.log[arg.LogId]
	if !ok {
		node.log[arg.LogId] = entry
	}
	// 持久化
	node.db.Put([]byte(arg.Entry.Key), []byte(arg.Entry.Value), nil)
	reply.Reply = true // rpc call需要有reply，但实际上调用是否成功是error返回值决定
	return nil
}
