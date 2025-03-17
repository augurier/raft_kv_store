package nodes

import (
	"fmt"
	"math/rand"
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

func Init(selfId string, nodeAddr map[string]string, pipe string, db *leveldb.DB, rstorage *RaftStorage) *Node {
	ns := make(map[string]*Public_node_info)
	for id, addr := range nodeAddr {
		ns[id] = newNode(addr)
	}

	// 创建节点
	node := &Node{
		selfId:      selfId,
		leaderId: 	 "",
		nodes:       ns,
		pipeAddr:    pipe,
		maxLogId:    -1, // 后来发现论文中是从1开始的（初始0），但不想改了
		currTerm:    1,
		log:         make([]RaftLogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		db:          db,
		storage: rstorage,
	}
	node.initLeaderState()
	return node
}

// func Start(node *Node, isLeader bool) {
// 	if isLeader {
// 		node.state = Candidate // 需要身份转变
// 	} else {
// 		node.state = Follower
// 	}

// 	go func() {
// 		for {
// 			switch node.state {
// 			case Follower:

// 			case Candidate:
// 				// todo 成为leader的初始化
// 				// node.currTerm = 1

// 				// candidate发布一个监听输入线程后，变成leader
// 				node.state = Leader
// 				go func() {
// 					if node.pipeAddr == "" { // 客户端远程调用server_node方法
// 						log.Info("请运行客户端进程进行读写")
// 					} else { // 命令行提供了管道，支持管道（键盘）输入
// 						pipe, err := os.Open(node.pipeAddr)
// 						if err != nil {
// 							log.Error("Failed to open pipe")
// 						}
// 						defer pipe.Close()

// 						// 不断读取管道中的输入
// 						buffer := make([]byte, 256)
// 						for {
// 							n, err := pipe.Read(buffer)
// 							if err != nil && err != io.EOF {
// 								log.Error("Error reading from pipe")
// 							}
// 							if n > 0 {
// 								input := string(buffer[:n])
// 								// 将用户输入封装成一个 LogEntry
// 								kv := LogEntry{input, ""} // 目前键盘输入key，value 0
// 								logId := node.maxLogId
// 								node.maxLogId++
// 								node.log[logId] = RaftLogEntry{kv, logId, node.currTerm}

//									log.Info("send : logId = " + strconv.Itoa(logId) + ", key = " + input)
//									// 广播给其它节点
//									node.BroadCastKV(Normal)
//									// 持久化
//									node.db.Put([]byte(kv.Key), []byte(kv.Value), nil)
//								}
//							}
//						}
//					}()
//				case Leader:
//					time.Sleep(50 * time.Millisecond)
//				}
//			}
//		}()
//	}
func (n *Node) initLeaderState() {
	for peerId := range n.nodes {
		n.nextIndex[peerId] = len(n.log) // 发送日志的下一个索引
		n.matchIndex[peerId] = 0         // 复制日志的最新匹配索引
	}
	n.storage.SetTermAndVote(n.currTerm, n.votedFor)
}

func Start(node *Node) {
	node.state = Follower     // 所有节点以 Follower 状态启动
	node.resetElectionTimer() // 启动选举超时定时器

	go func() {
		for {
			switch node.state {
			case Follower:
				// 监听心跳超时
				fmt.Printf("Node %s is a follower, 监听中...\n", node.selfId)

			case Leader:
				// 发送心跳
				fmt.Printf("Node %s is the leader, 发送心跳...\n", node.selfId)
				node.BroadCastKV(Normal)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
}

// follower 500-1000ms内没收到appendentries心跳，就变成candidate发起选举
func (node *Node) resetElectionTimer() {
	if node.electionTimer == nil {
		node.electionTimer = time.NewTimer(time.Duration(500+rand.Intn(500)) * time.Millisecond)
		go func() {
			for {
				<-node.electionTimer.C
				node.startElection()
			}
		}()
	} else {
		node.electionTimer.Stop()
		node.electionTimer.Reset(time.Duration(500+rand.Intn(500)) * time.Millisecond)
	}
}

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
