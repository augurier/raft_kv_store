package nodes

import (
	"errors"
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

// 运行在进程上的初始化 + rpc注册
func InitRPCNode(SelfId string, port string, nodeAddr map[string]string, db *leveldb.DB, rstorage *RaftStorage, isRestart bool) *Node {
	var nodeIds []string 
	for id := range nodeAddr {
		nodeIds = append(nodeIds, id)
	}

	// 创建节点
	node := &Node{
		SelfId:      SelfId,
		LeaderId:    "",
		Nodes:       nodeIds,
		MaxLogId:    -1, // 后来发现论文中是从1开始的（初始0），但不想改了
		CurrTerm:    1,
		Log:         make([]RaftLogEntry, 0),
		CommitIndex: -1,
		LastApplied: -1,
		NextIndex:   make(map[string]int),
		MatchIndex:  make(map[string]int),
		Db:          db,
		Storage:     rstorage,
		Transport:   &HTTPTransport{NodeMap:  nodeAddr},
	}
	node.initLeaderState()
	if isRestart {
		node.CurrTerm = rstorage.GetCurrentTerm()
		node.VotedFor = rstorage.GetVotedFor()
		node.Log = rstorage.GetLogEntries()
		log.Sugar().Infof("[%s]从重启中恢复log数量: %d", SelfId, len(node.Log))
	}

	log.Sugar().Infof("[%s]开始监听" + port + "端口", SelfId)
	node.ListenPort(port)

	return node
}

func (node *Node) ListenPort(port string) {

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

// 线程模拟的初始化
func InitThreadNode(SelfId string, peerIds []string, db *leveldb.DB, rstorage *RaftStorage, isRestart bool, threadTransport *ThreadTransport) (*Node, chan struct{}) {
	rpcChan := make(chan RPCRequest, 100) // 要监听的chan
	// 创建节点
	node := &Node{
		SelfId:      SelfId,
		LeaderId:    "",
		Nodes:       peerIds,
		MaxLogId:    -1, // 后来发现论文中是从1开始的（初始0），但不想改了
		CurrTerm:    1,
		Log:         make([]RaftLogEntry, 0),
		CommitIndex: -1,
		LastApplied: -1,
		NextIndex:   make(map[string]int),
		MatchIndex:  make(map[string]int),
		Db:          db,
		Storage:     rstorage,
		Transport:   threadTransport,
	}
	node.initLeaderState()
	if isRestart {
		node.CurrTerm = rstorage.GetCurrentTerm()
		node.VotedFor = rstorage.GetVotedFor()
		node.Log = rstorage.GetLogEntries()
		log.Sugar().Infof("[%s]从重启中恢复log数量: %d", SelfId, len(node.Log))
	}

	threadTransport.RegisterNodeChan(SelfId, rpcChan)
	quitChan := make(chan struct{}, 1)
	go node.listenForChan(rpcChan, quitChan)

	return node, quitChan
}

func (node *Node) listenForChan(rpcChan chan RPCRequest, quitChan chan struct{}) {
	defer node.Db.Close()

    for {
		select {
        case req := <-rpcChan:
			switch req.Behavior {
			case DelayRpc:
				threadTran, ok := node.Transport.(*ThreadTransport)
				if !ok {
					log.Fatal("无效的delayRpc模式")
				}
				duration, ok2 := threadTran.Ctx.GetDelay(req.SourceId, node.SelfId)
				if !ok2 {
					log.Fatal("没有设置对应的delay时间")
				}
				time.Sleep(duration)

			case FailRpc:
				continue
			}

			switch req.ServiceMethod {
			case "Node.AppendEntries":
				arg, ok := req.Args.(*AppendEntriesArg)
				resp, ok2 := req.Reply.(*AppendEntriesReply)
				if !ok || !ok2 {
					req.Done <- errors.New("type assertion failed for AppendEntries")
				} else {
					req.Done <- node.AppendEntries(arg, resp)
				}

			case "Node.RequestVote":
				arg, ok := req.Args.(*RequestVoteArgs)
				resp, ok2 := req.Reply.(*RequestVoteReply)
				if !ok || !ok2 {
					req.Done <- errors.New("type assertion failed for RequestVote")
				} else {
					req.Done <- node.RequestVote(arg, resp)
				}

			case "Node.WriteKV":
				arg, ok := req.Args.(*LogEntryCall)
				resp, ok2 := req.Reply.(*ServerReply)
				if !ok || !ok2 {
					req.Done <- errors.New("type assertion failed for WriteKV")
				} else {
					req.Done <- node.WriteKV(arg, resp)
				}

			case "Node.ReadKey":
				arg, ok := req.Args.(*string)
				resp, ok2 := req.Reply.(*ServerReply)
				if !ok || !ok2 {
					req.Done <- errors.New("type assertion failed for ReadKey")
				} else {
					req.Done <- node.ReadKey(arg, resp)
				}

			case "Node.FindLeader":
				arg, ok := req.Args.(struct{})
				resp, ok2 := req.Reply.(*FindLeaderReply)
				if !ok || !ok2 {
					req.Done <- errors.New("type assertion failed for FindLeader")
				} else {
					req.Done <- node.FindLeader(arg, resp)
				}

			default:
				req.Done <- fmt.Errorf("未知方法: %s", req.ServiceMethod)
			}
		case <-quitChan:
			log.Sugar().Infof("[%s] 监听线程收到退出信号", node.SelfId)
			node.Db.Close()
			node.Storage.Close()
            return
		}
    }
}

// 共同部分和启动
func (n *Node) initLeaderState() {
	for _, peerId := range n.Nodes {
		n.NextIndex[peerId] = len(n.Log) // 发送日志的下一个索引
		n.MatchIndex[peerId] = 0         // 复制日志的最新匹配索引
	}
}

func Start(node *Node, quitChan chan struct{}) {
	node.State = Follower     // 所有节点以 Follower 状态启动
	node.ResetElectionTimer() // 启动选举超时定时器

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-quitChan:
				fmt.Printf("[%s] Raft start 退出...\n", node.SelfId)
				return // 退出 goroutine

			case <-ticker.C:
				switch node.State {
				case Follower:
					// 监听心跳超时
					// fmt.Printf("[%s] is a follower, 监听中...\n", node.SelfId)

				case Leader:
					// 发送心跳
					// fmt.Printf("[%s] is the leader, 发送心跳...\n", node.SelfId)
					node.ResetElectionTimer() // leader 不主动触发选举
					node.BroadCastKV()
				}
			}
		}
	}()
}



