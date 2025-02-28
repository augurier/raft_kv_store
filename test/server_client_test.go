package test

import (
	"fmt"
	"os"
	"os/exec"
	"simple-kv-store/internal/client"
	"simple-kv-store/internal/logprovider"
	"simple-kv-store/internal/nodes"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

func TestServerClient(t *testing.T) {
	n := 5
	var clusters []string
	for i := 0; i < n; i++ {
		port := fmt.Sprintf("%d", uint16(9090)+uint16(i))
		addr := "127.0.0.1:" + port
		clusters = append(clusters, addr)
	}

	var cmds []*exec.Cmd
	for i := 0; i < n; i++ { 
		tmpClusters := append(clusters[:i], clusters[i+1:]...)
		port := fmt.Sprintf(":%d", uint16(9090)+uint16(i))

		var isleader string
		if i == 0 {
			isleader = "true"
		} else {
			isleader = "false"
		}
		cmd := exec.Command(
			"../main", 
			"-id", strconv.Itoa(i + 1), 
			"-port", port, 
			"-cluster", strings.Join(tmpClusters, ","), 
			"-isleader=" + isleader,
		)
		cmds = append(cmds, cmd)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	
		// 执行命令
		err := cmd.Start()
		if err != nil {
			fmt.Println("启动进程出错:", err)
			return
		}				
	}

	time.Sleep(time.Second)
	// client启动
	c := clientPkg.Client{Address: "127.0.0.1:9090", ServerId: "1"}
	s := c.Write(nodes.LogEntry{Key: "1", Value: "hello"})
	if s != clientPkg.Ok {
		t.Errorf("write test fail")
	}

	var value string
	s = c.Read("1", &value)
	if s != clientPkg.Ok {
		t.Errorf("Read test1 fail")
	}

	s = c.Read("2", &value)
	if s != clientPkg.NotFound {
		t.Errorf("Read test2 fail")
	}

	for _, cmd := range cmds {
		err := cmd.Process.Signal(syscall.SIGTERM)
		if err != nil {
			fmt.Println("Error sending signal:", err)
			return
		}
	}

}
