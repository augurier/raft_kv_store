package test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func ExecuteNodeI(i int, isRestart bool, clusters []string) *exec.Cmd {
	port := fmt.Sprintf(":%d", uint16(9090)+uint16(i))

	var isRestartStr string
	if isRestart {
		isRestartStr = "true"
	} else {
		isRestartStr = "false"
	}
	cmd := exec.Command(
		"../main", 
		"-id", strconv.Itoa(i + 1), 
		"-port", port, 
		"-cluster", strings.Join(clusters, ","), 
		"-isRestart=" + isRestartStr,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// 执行命令
	err := cmd.Start()
	if err != nil {
		fmt.Println("启动进程出错:", err)
		return nil
	}	
	return cmd
}