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
	threadTransport := nodes.NewThreadTransport()
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
	cWrite := clientPkg.Client{PeerIds: peerIds, Transport: threadTransport}
	// 写入
	var s clientPkg.Status
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		s := cWrite.Write(nodes.LogEntryCall{LogE: newlog, CallState: nodes.Normal})
		if s != clientPkg.Ok {
			t.Errorf("write test fail")
		}		
	}
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
	cRead := clientPkg.Client{PeerIds: peerIds, Transport: threadTransport}
	// 读写入数据
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		var value string
		s = cRead.Read(key, &value)
		if s != clientPkg.Ok {
			t.Errorf("Read test1 fail")
		}
	}

	// 读未写入数据
	for i := 5; i < 15; i++ {
		key := strconv.Itoa(i)
		var value string
		s = cRead.Read(key, &value)
		if s != clientPkg.NotFound {
			t.Errorf("Read test2 fail")
		}
	}
}
