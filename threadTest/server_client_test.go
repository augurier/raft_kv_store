package threadTest

import (
	"simple-kv-store/internal/client"
	"simple-kv-store/internal/nodes"
	"strconv"
	"testing"
	"time"
)

func TestServerClient(t *testing.T) {
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
	// client启动
	c := clientPkg.Client{PeerIds: peerIds, Transport: threadTransport}

	// 写入
	var s clientPkg.Status
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		s = c.Write(nodes.LogEntryCall{LogE: newlog})
		if s != clientPkg.Ok {
			t.Errorf("write test fail")
		}		
	}

	time.Sleep(time.Second) // 等待写入完毕
	// 读写入数据
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		var value string
		s = c.Read(key, &value)
		if s != clientPkg.Ok || value != "hello" {
			t.Errorf("Read test1 fail")
		}
	}

	// 读未写入数据
	for i := 10; i < 15; i++ {
		key := strconv.Itoa(i)
		var value string
		s = c.Read(key, &value)
		if s != clientPkg.NotFound {
			t.Errorf("Read test2 fail")
		}
	}
}
