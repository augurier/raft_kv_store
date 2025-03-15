package nodes

import (
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"simple-kv-store/internal/logprovider"
	"strconv"
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

func Init(id string, nodeAddr map[string]string, pipe string, db *leveldb.DB) *Node {
	ns := make(map[string]*Public_node_info)
	for id, addr := range nodeAddr {
		ns[id] = newNode(addr)
	}

	// 创建节点
	node := &Node{
		selfId:   id,
		nodes:    ns,
		pipeAddr: pipe,
		maxLogId: -1,
		currTerm: 1,
		log:      make([]RaftLogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex: make(map[string]int),
		matchIndex: make(map[string]int),
		db:       db,
	}
	for nodeId := range nodeAddr {
        if nodeId != id { // 不初始化自身
            node.nextIndex[nodeId] = node.maxLogId + 1
            node.matchIndex[nodeId] = 0
        }
    }
	return node
}

func Start(node *Node, isLeader bool) {
	if isLeader {
		node.state = Candidate // 需要身份转变
	} else {
		node.state = Follower
	}

	go func() {
		for {
			switch node.state {
			case Follower:

			case Candidate:
				// todo 成为leader的初始化
				// node.currTerm = 1

				// candidate发布一个监听输入线程后，变成leader
				node.state = Leader
				go func() {
					if node.pipeAddr == "" { // 客户端远程调用server_node方法
						log.Info("请运行客户端进程进行读写")
					} else { // 命令行提供了管道，支持管道（键盘）输入
						pipe, err := os.Open(node.pipeAddr)
						if err != nil {
							log.Error("Failed to open pipe")
						}
						defer pipe.Close()

						// 不断读取管道中的输入
						buffer := make([]byte, 256)
						for {
							n, err := pipe.Read(buffer)
							if err != nil && err != io.EOF {
								log.Error("Error reading from pipe")
							}
							if n > 0 {
								input := string(buffer[:n])
								// 将用户输入封装成一个 LogEntry
								kv := LogEntry{input, ""} // 目前键盘输入key，value 0
								logId := node.maxLogId
								node.maxLogId++
								node.log[logId] = RaftLogEntry{kv, logId, node.currTerm}

								log.Info("send : logId = " + strconv.Itoa(logId) + ", key = " + input)
								// 广播给其它节点
								node.BroadCastKV(Normal)
								// 持久化
								node.db.Put([]byte(kv.Key), []byte(kv.Value), nil)
							}
						}
					}
				}()
			case Leader:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
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
