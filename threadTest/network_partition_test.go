package threadTest

import (
	"fmt"
	clientPkg "simple-kv-store/internal/client"
	"simple-kv-store/internal/nodes"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBasicConnectivity(t *testing.T) {
    transport := nodes.NewThreadTransport()

    transport.RegisterNodeChan("1", make(chan nodes.RPCRequest, 10))
    transport.RegisterNodeChan("2", make(chan nodes.RPCRequest, 10))

    // 断开 A 和 B
    transport.SetConnectivity("1", "2", false)

    err := transport.CallWithTimeout(&nodes.ThreadClient{SourceId: "1", TargetId: "2"}, "Node.AppendEntries", &nodes.AppendEntriesArg{}, &nodes.AppendEntriesReply{})
    if err == nil {
        t.Errorf("Expected network partition error, but got nil")
    }

    // 恢复连接
    transport.SetConnectivity("1", "2", true)

    err = transport.CallWithTimeout(&nodes.ThreadClient{SourceId: "1", TargetId: "2"}, "Node.AppendEntries", &nodes.AppendEntriesArg{}, &nodes.AppendEntriesReply{})
    if !strings.Contains(err.Error(), "RPC 调用超时") {
        t.Errorf("Expected success, but got error: %v", err)
    }
}

func TestElectionWithPartition(t *testing.T) {
	// 登记结点信息
	n := 3
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport()
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}

	// 通知所有node结束
	defer func(){
		for _, quitChan := range quitCollections {
			close(quitChan)
		}
	}()

	time.Sleep(2 * time.Second) // 等待启动完毕
	fmt.Println("开始分区模拟1")
	var leaderNo int
	for i := 0; i < n; i++ {
		if nodeCollections[i].State == nodes.Leader {
			leaderNo = i
			for j := 0; j < n; j++ {
				if i != j { // 切断其它节点到leader的消息
					threadTransport.SetConnectivity(nodeCollections[j].SelfId, nodeCollections[i].SelfId, false)
				}				
			}
		}
	}
    time.Sleep(2 * time.Second)

    if nodeCollections[leaderNo].State == nodes.Leader {
        t.Errorf("分区退选失败")
    }

    // 恢复网络
	for j := 0; j < n; j++ {
		if leaderNo != j { // 恢复其它节点到leader的消息
			threadTransport.SetConnectivity(nodeCollections[j].SelfId, nodeCollections[leaderNo].SelfId, true)
		}				
	}

    time.Sleep(1 * time.Second)

	var leaderCnt int
	for i := 0; i < n; i++ {
		if nodeCollections[i].State == nodes.Leader {
			leaderCnt++
			leaderNo = i
		}
	}
	if leaderCnt != 1 {
		t.Errorf("多leader产生")
	}

	fmt.Println("开始分区模拟2")
	for j := 0; j < n; j++ {
		if leaderNo != j { // 切断leader到其它节点的消息
			threadTransport.SetConnectivity(nodeCollections[leaderNo].SelfId, nodeCollections[j].SelfId, false)
		}				
	}
	time.Sleep(1 * time.Second)

	if nodeCollections[leaderNo].State == nodes.Leader {
        t.Errorf("分区退选失败")
    }

	leaderCnt = 0
	for j := 0; j < n; j++ {
		if nodeCollections[j].State == nodes.Leader {
			leaderCnt++
		}			
	}
	if leaderCnt != 1 {
		t.Errorf("多leader产生")
	}

	// client启动
	c := clientPkg.Client{PeerIds: peerIds, Transport: threadTransport}
	var s clientPkg.Status
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		s = c.Write(nodes.LogEntryCall{LogE: newlog})
		if s != clientPkg.Ok {
			t.Errorf("write test fail")
		}		
	}

	time.Sleep(time.Second) // 等待写入完毕

	// 恢复网络
	for j := 0; j < n; j++ {
		if leaderNo != j {
			threadTransport.SetConnectivity(nodeCollections[leaderNo].SelfId, nodeCollections[j].SelfId, true)
		}				
	}

	time.Sleep(time.Second)
	// 日志一致性检查
	for i := 0; i < n; i++ {
		if len(nodeCollections[i].Log) != 5 {
			t.Errorf("日志数量不一致:" + strconv.Itoa(len(nodeCollections[i].Log)))
		}
	}
}