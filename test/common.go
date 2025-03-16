package test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func ExecuteNodeI(i int, isNewDb bool, clusters []string) *exec.Cmd {
	port := fmt.Sprintf(":%d", uint16(9090)+uint16(i))

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
		"-cluster", strings.Join(clusters, ","), 
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