package test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func ExecuteNodeI(i int, isLeader bool, isNewDb bool, clusters []string) *exec.Cmd {
	tmpClusters := append(clusters[:i], clusters[i+1:]...)
	port := fmt.Sprintf(":%d", uint16(9090)+uint16(i))

	var isleader string
	if isLeader {
		isleader = "true"
	} else {
		isleader = "false"
	}
	var isnewdb string
	if isNewDb {
		isnewdb = "true"
	} else {
		isnewdb = "false"
	}
	cmd := exec.Command(
		"../main", 
		"-id", strconv.Itoa(i + 1), 
		"-port", port, 
		"-cluster", strings.Join(tmpClusters, ","), 
		"-isleader=" + isleader,
		"-isNewDb=" + isnewdb,
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