package test

import (
	"fmt"
	"os/exec"
	"simple-kv-store/internal/client"
	"simple-kv-store/internal/nodes"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestNodeRestart(t *testing.T) {
	// 登记结点信息
	n := 5
	var clusters []string
	var peerIds []string
	addressMap := make(map[string]string)
	for i := 0; i < n; i++ {
		port := fmt.Sprintf("%d", uint16(9090)+uint16(i))
		addr := "127.0.0.1:" + port
		clusters = append(clusters, addr)
		addressMap[strconv.Itoa(i + 1)] = addr
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var cmds []*exec.Cmd
	for i := 0; i < n; i++ { 
		cmd := ExecuteNodeI(i, false, clusters)	
		cmds = append(cmds, cmd)		
	}

	// 通知所有进程结束
	defer func(){
		for _, cmd := range cmds {
			err := cmd.Process.Signal(syscall.SIGTERM)
			if err != nil {
				fmt.Println("Error sending signal:", err)
				return
			}
		}
	}()

	time.Sleep(time.Second) // 等待启动完毕
	// client启动, 连接任意节点
	transport := &nodes.HTTPTransport{NodeMap:  addressMap}
	cWrite := clientPkg.NewClient("0", peerIds, transport)

	// 写入
	var s clientPkg.Status
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		s := cWrite.Write(newlog)
		if s != clientPkg.Ok {
			t.Errorf("write test fail")
		}		
	}
	time.Sleep(time.Second) // 等待写入完毕

	// 模拟结点轮流崩溃
	for i := 0; i < n; i++ {
		err := cmds[i].Process.Signal(syscall.SIGTERM)
		if err != nil {
			fmt.Println("Error sending signal:", err)
			return
		}

		time.Sleep(time.Second)
		cmd := ExecuteNodeI(i, true, clusters)
		if cmd == nil {
			t.Errorf("recover test1 fail")
			return
		} else {
			cmds[i] = cmd
		}
		time.Sleep(time.Second) // 等待启动完毕
	}


	// client启动
	cRead := clientPkg.NewClient("0", peerIds, transport)
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
