package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"simple-kv-store/internal/logprovider"
	"simple-kv-store/internal/nodes"
	"strconv"
	"strings"
	"syscall"

	"go.uber.org/zap"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Info("i get a panic", zap.Any("panic error", err))
		}
	}()

	// 设置一个通道来捕获中断信号
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9092,127.0.0.1:9093", "comma sep")
	id := flag.String("id", "1", "node ID")
	pipe := flag.String("pipe", "", "input from scripts")
	isLeader := flag.Bool("isleader", false, "init node state")

	// 参数解析
	flag.Parse()
	clusters := strings.Split(*cluster, ",")
	idClusterPairs := make(map[string]string)
	idCnt := 1
	selfi, err := strconv.Atoi(*id)
	if err != nil {
		log.Error("figure id only")
	}
	for _, addr := range clusters {
		if idCnt == selfi {
			idCnt++ // 命令行cluster按id排序传入，记录时跳过自己的id，先保证所有节点互相记录的id一致
		}
		idClusterPairs[strconv.Itoa(idCnt)] = addr 
		idCnt++
	}
	node := nodes.Init(*id, idClusterPairs, *pipe)

	log.Info("id: " + *id + "节点开始监听: " + *port + "端口")

	// 监听rpc
	node.Rpc(*port)
	// 开启 raft
	nodes.Start(node, *isLeader)

	sig := <-sigs
	fmt.Println("接收到信号:", sig)

}
