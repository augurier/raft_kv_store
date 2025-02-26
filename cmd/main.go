package main

import (
	"flag"
	"simple-kv-store/internal/logprovider"
	"simple-kv-store/internal/nodes"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

var log, _ = logprovider.CreateDefaultZapLogger(zap.InfoLevel)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Info("i get a panic", zap.Any("panic error", err))
		}
	}()

	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9092,127.0.0.1:9093", "comma sep")
	id := flag.Int("id", 1, "node ID")
	pipe := flag.String("pipe", "", "input from scripts")
	isLeader := flag.Bool("isleader", false, "init node state")

	// 参数解析
	flag.Parse()
	clusters := strings.Split(*cluster, ",")
	node := nodes.Init(*id, clusters, *pipe)

	log.Info("id: " + strconv.Itoa(*id) + "节点开始监听: " + *port + "端口")

	// 监听rpc
	node.Rpc(*port)
	// 开启 raft
	nodes.Start(node, *isLeader)

	select {}
}
