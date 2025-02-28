package nodes

import (
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"simple-kv-store/internal/logprovider"
	"time"

	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

func newNode(address string) *Public_node_info {
	return &Public_node_info{
		connect: false,
		address: address,
	}
}

func Init(id string, nodeAddr map[string]string, pipe string) *Node {
	ns := make(map[string]*Public_node_info)
	for id, addr := range nodeAddr {
		ns[id] = newNode(addr)
	}

	// 创建节点
	return &Node{
		selfId:  id,
		nodes: ns,
		pipeAddr: pipe,
	}
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
								log.Info("send : " + input)
								// 将用户输入封装成一个 LogEntry
								kv := LogEntry{input, ""}
								node.log = append(node.log, kv)
								// 广播给其它节点
								node.BroadCastKV(kv)
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
