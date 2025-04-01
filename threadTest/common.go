package threadTest

import (
	"fmt"
	"os"
	"simple-kv-store/internal/nodes"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func ExecuteNodeI(id string, isRestart bool, peerIds []string, threadTransport *nodes.ThreadTransport) (*nodes.Node, chan struct{}) {
	if !isRestart {
		os.RemoveAll("storage/node" + id)
	}

	os.RemoveAll("leveldb/simple-kv-store" + id)

	db, err := leveldb.OpenFile("leveldb/simple-kv-store" + id, nil)
	if err != nil {
		fmt.Println("Failed to open database: ", err)
	}

	// 打开或创建节点数据持久化文件
	storage := nodes.NewRaftStorage("storage/node" + id)

	var otherIds []string
	for _, ids := range peerIds {
		if ids != id {
			otherIds = append(otherIds, ids) // 删除目标元素
		}
	}
	// 初始化
	node, quitChan := nodes.InitThreadNode(id, otherIds, db, storage, isRestart, threadTransport)

	// 开启 raft
	go nodes.Start(node, quitChan)
	return node, quitChan
}

func CheckOneLeader(t *testing.T, nodeCollections []* nodes.Node) {
	cnt := 0
	for _, node := range nodeCollections {
		if node.State == nodes.Leader {
			cnt++
		}
	}
	if cnt != 1 {
		t.Errorf("实际有%d个leader(!=1)", cnt)
	}
}

func CheckZeroOrOneLeader(t *testing.T, nodeCollections []* nodes.Node) {
	cnt := 0
	for _, node := range nodeCollections {
		if node.State == nodes.Leader {
			cnt++
		}
	}
	if cnt > 1 {
		t.Errorf("实际有%d个leader(>1)", cnt)
	}
}

func CheckIsLeader(t *testing.T, node *nodes.Node) {
	if node.State != nodes.Leader {
		t.Errorf("[%s]不是leader", node.SelfId)
	}
}

func CheckTerm(t *testing.T, node *nodes.Node, targetTerm int) {
	if node.CurrTerm != targetTerm {
		t.Errorf("[%s]实际term=%d (!=%d)", node.SelfId, node.CurrTerm, targetTerm)
	}
}
