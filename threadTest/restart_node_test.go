package threadTest

import (
	"simple-kv-store/internal/client"
	"simple-kv-store/internal/nodes"
	"strconv"
	"testing"
	"time"
)

func TestNodeRestart(t *testing.T) {
	// 登记结点信息
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
	for i := 0; i < n; i++ {
		_, quitChan := ExecuteNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
	}

	// 通知所有node结束
	defer func(){
		for _, quitChan := range quitCollections {
			close(quitChan)
		}
	}()

	time.Sleep(time.Second) // 等待启动完毕
	// client启动, 连接任意节点
	cWrite := clientPkg.NewClient("0", peerIds, threadTransport)
	// 写入
	ClientWriteLog(t, 0, 5, cWrite)
	time.Sleep(time.Second) // 等待写入完毕

	// 模拟结点轮流崩溃
	for i := 0; i < n; i++ {
		close(quitCollections[i])

		time.Sleep(time.Second)
		_, quitChan := ExecuteNodeI(strconv.Itoa(i + 1), true, peerIds, threadTransport)
		quitCollections[i] = quitChan
		time.Sleep(time.Second) // 等待启动完毕
	}


	// client启动
	cRead := clientPkg.NewClient("0", peerIds, threadTransport)
	// 读写入数据
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		var value string
		s := cRead.Read(key, &value)
		if s != clientPkg.Ok {
			t.Errorf("Read test1 fail")
		}
	}

	// 读未写入数据
	for i := 5; i < 15; i++ {
		key := strconv.Itoa(i)
		var value string
		s := cRead.Read(key, &value)
		if s != clientPkg.NotFound {
			t.Errorf("Read test2 fail")
		}
	}
}


func TestRestartWhileWriting(t *testing.T) {
	// 登记结点信息
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
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

	time.Sleep(time.Second) // 等待启动完毕
	leaderIdx := FindLeader(t, nodeCollections)
	// client启动, 连接任意节点
	cWrite := clientPkg.NewClient("0", peerIds, threadTransport)
	// 写入
	go ClientWriteLog(t, 0, 5, cWrite)

	go func() {
		close(quitCollections[leaderIdx])

		n, quitChan := ExecuteNodeI(strconv.Itoa(leaderIdx + 1), true, peerIds, threadTransport)
		quitCollections[leaderIdx] = quitChan
		nodeCollections[leaderIdx] = n
	}()

	time.Sleep(time.Second) // 等待启动完毕	
	// client启动
	cRead := clientPkg.NewClient("0", peerIds, threadTransport)
	// 读写入数据
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		var value string
		s := cRead.Read(key, &value)
		if s != clientPkg.Ok {
			t.Errorf("Read test1 fail")
		}
	}
	CheckLogNum(t, nodeCollections[leaderIdx], 5)
}
