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

func TestFollowerRestart(t *testing.T) {
	// 登记结点信息
	n := 3
	var clusters []string
	for i := 0; i < n; i++ {
		port := fmt.Sprintf("%d", uint16(9090)+uint16(i))
		addr := "127.0.0.1:" + port
		clusters = append(clusters, addr)
	}

	// 结点启动
	var cmds []*exec.Cmd
	for i := 0; i < n; i++ { 
		var cmd *exec.Cmd
		if i == 0 {
			cmd = ExecuteNodeI(i, true, clusters)
		} else {
			cmd = ExecuteNodeI(i, true, clusters)
		}
		
		if cmd == nil {
			return
		} else {
			cmds = append(cmds, cmd)
		}			
	}

	time.Sleep(time.Second) // 等待启动完毕
	// client启动, 连接leader
	cWrite := clientPkg.Client{Address: clusters[0], ServerId: "1"}

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
	// 模拟最后一个结点崩溃
	err := cmds[n - 1].Process.Signal(syscall.SIGTERM)
	if err != nil {
		fmt.Println("Error sending signal:", err)
		return
	}
	// 继续写入
	for i := 5; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		s := cWrite.Write(nodes.LogEntryCall{LogE: newlog, CallState: nodes.Normal})
		if s != clientPkg.Ok {
			t.Errorf("write test fail")
		}		
	}
	// 恢复结点
	cmd := ExecuteNodeI(n - 1, false, clusters)
	if cmd == nil {
		t.Errorf("recover test1 fail")
		return
	} else {
		cmds[n - 1] = cmd
	}
	time.Sleep(time.Second) // 等待启动完毕

	// client启动, 连接节点n-1(去读它的数据)
	cRead := clientPkg.Client{Address: clusters[n - 1], ServerId: "n"}
	// 读崩溃前写入数据
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

	// 通知进程结束
	for _, cmd := range cmds {
		err := cmd.Process.Signal(syscall.SIGTERM)
		if err != nil {
			fmt.Println("Error sending signal:", err)
			return
		}
	}

}
