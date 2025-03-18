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

func TestServerClient(t *testing.T) {
	// 登记结点信息
	n := 5
	var clusters []string
	for i := 0; i < n; i++ {
		port := fmt.Sprintf("%d", uint16(9090)+uint16(i))
		addr := "127.0.0.1:" + port
		clusters = append(clusters, addr)
	}

	// 结点启动
	var cmds []*exec.Cmd
	for i := 0; i < n; i++ {
		cmd := ExecuteNodeI(i, true, clusters)
		cmds = append(cmds, cmd)
	}

	time.Sleep(time.Second) // 等待启动完毕
	// client启动
	c := clientPkg.Client{Address: clusters}

	// 写入
	var s clientPkg.Status
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		s := c.Write(nodes.LogEntryCall{LogE: newlog, CallState: nodes.Normal})
		if s != clientPkg.Ok {
			t.Errorf("write test fail")
		}		
	}

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

	// 通知进程结束
	for _, cmd := range cmds {
		err := cmd.Process.Signal(syscall.SIGTERM)
		if err != nil {
			fmt.Println("Error sending signal:", err)
			return
		}
	}

}
