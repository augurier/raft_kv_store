package nodes

import (
	// "context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"simple-kv-store/internal/logprovider"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

func newNode(address string) *Public_node_info {
	return &Public_node_info{
		connect: false,
		address: address,
	}
}

func Init(selfId string, nodeAddr map[string]string, db *leveldb.DB, rstorage *RaftStorage, isRestart bool) *Node {
	ns := make(map[string]*Public_node_info)
	for id, addr := range nodeAddr {
		ns[id] = newNode(addr)
	}

	// 创建节点
	node := &Node{
		selfId:      selfId,
		leaderId:    "",
		nodes:       ns,
		maxLogId:    -1, // 后来发现论文中是从1开始的（初始0），但不想改了
		currTerm:    1,
		log:         make([]RaftLogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		db:          db,
		storage:     rstorage,
	}
	node.initLeaderState()
	if isRestart {
		node.currTerm = rstorage.GetCurrentTerm()
		node.votedFor = rstorage.GetVotedFor()
		node.log = rstorage.GetLogEntries()
		log.Sugar().Infof("[%s]从重启中恢复log数量: %d", selfId, len(node.log))
	}

	return node
}

func (n *Node) initLeaderState() {
	for peerId := range n.nodes {
		n.nextIndex[peerId] = len(n.log) // 发送日志的下一个索引
		n.matchIndex[peerId] = 0         // 复制日志的最新匹配索引
	}
}

func Start(node *Node) {
	node.state = Follower     // 所有节点以 Follower 状态启动
	node.resetElectionTimer() // 启动选举超时定时器

	go func() {
		for {
			switch node.state {
			case Follower:
				// 监听心跳超时
				fmt.Printf("[%s] is a follower, 监听中...\n", node.selfId)

			case Leader:
				// 发送心跳
				fmt.Printf("[%s] is the leader, 发送心跳...\n", node.selfId)
				node.resetElectionTimer() // leader不主动触发选举
				node.BroadCastKV(Normal)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
}

// 初始时注册rpc方法
func (node *Node) Rpc(port string) {

	err := rpc.Register(node)
	if err != nil {
		log.Fatal("rpc register failed", zap.Error(err))
	}
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", port)
	if e != nil {
		log.Fatal("listen error:", zap.Error(e))
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatal("http server error:", zap.Error(err))
		}
	}()
}

// 封装有超时的dial
func DialHTTPWithTimeout(network, address string) (*rpc.Client, error) {
	done := make(chan struct{})
	var client *rpc.Client
	var err error

	go func() {
		client, err = rpc.DialHTTP(network, address)
		close(done)
	}()

	select {
	case <-done:
		return client, err
	case <-time.After(50 * time.Millisecond):
		return nil, fmt.Errorf("dial timeout: %s", address)
	}
}

// 封装有超时的call
func CallWithTimeout[T1 any, T2 any](client *rpc.Client, serviceMethod string, args *T1, reply *T2) error {
	done := make(chan error, 1)

	go func() {
		done <- client.Call(serviceMethod, args, reply)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(50 * time.Millisecond):
		return fmt.Errorf("call timeout: %s", serviceMethod)
	}
}
