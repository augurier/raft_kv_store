package main

import (
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
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
	cluster := flag.String("cluster", "127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093", "comma sep")
	id := flag.String("id", "1", "node ID")
	isRestart := flag.Bool("isRestart", true, "new test or restart")

	// 参数解析
	flag.Parse()
	clusters := strings.Split(*cluster, ",")
	idClusterPairs := make(map[string]string)
	idCnt := 1
	selfi, err := strconv.Atoi(*id)
	if err != nil {
		log.Fatal("figure id only")
	}
	for _, addr := range clusters {
		if idCnt == selfi {
			idCnt++ // 命令行cluster按id排序传入，记录时跳过自己的id，先保证所有节点互相记录的id一致
			continue
		}
		idClusterPairs[strconv.Itoa(idCnt)] = addr
		idCnt++
	}

	if *isRestart {	
		os.RemoveAll("storage/node" + *id + ".json")
	}

	// 创建每个结点自己的数据库。这里一开始理解上有些误区，状态机的状态恢复应该靠节点的持久化log，
	// 而用leveldb模拟状态机，造成了状态机本身的持久化，因此暂时通过删去旧db避免这一矛盾
	os.RemoveAll("leveldb/simple-kv-store" + *id)

	db, err := leveldb.OpenFile("leveldb/simple-kv-store"+*id, nil)
	if err != nil {
		log.Fatal("Failed to open database: ", zap.Error(err))
	}
	defer db.Close() // 确保数据库在使用完毕后关闭
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	// 打开或创建节点数据持久化文件
	storage := nodes.NewRaftStorage("storage/node" + *id + ".json")

	// 初始化
	node := nodes.Init(*id, idClusterPairs, db, storage, !*isRestart)

	log.Sugar().Infof("[%s]开始监听" + *port + "端口", *id)
	// 监听rpc
	node.Rpc(*port)
	// 开启 raft
	nodes.Start(node)

	sig := <-sigs
	fmt.Println("node_"+ *id +"接收到信号:", sig)

}
