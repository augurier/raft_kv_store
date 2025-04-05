package threadTest

import (
	"fmt"
	"os"
	"simple-kv-store/internal/client"
	"simple-kv-store/internal/nodes"
	"strconv"
	"testing"
	"time"

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

func ExecuteStaticNodeI(id string, isRestart bool, peerIds []string, threadTransport *nodes.ThreadTransport) (*nodes.Node, chan struct{}) {
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
	// go nodes.Start(node, quitChan)
	return node, quitChan
}

func StopElectionReset(nodeCollections [] *nodes.Node, quitCollections []chan struct{}) {
	for i := 0; i < len(quitCollections); i++ {
		node := nodeCollections[i]
		quitChan := quitCollections[i]
		go func(node *nodes.Node, quitChan chan struct{}) {
			ticker := time.NewTicker(400 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-quitChan:
					return // 退出 goroutine

				case <-ticker.C:
					node.ResetElectionTimer() // 不主动触发选举
				}
			}
		}(node, quitChan)		
	}
}

func SendKvCall(kvCall *nodes.LogEntryCall, node *nodes.Node) {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	node.MaxLogId++
	logId := node.MaxLogId
	rLogE := nodes.RaftLogEntry{LogE: kvCall.LogE,LogId:  logId, Term: node.CurrTerm}
	node.Log = append(node.Log, rLogE)
	node.Storage.AppendLog(rLogE)
	// 广播给其它节点	
	node.BroadCastKV()
}

func ClientWriteLog(t *testing.T, startLogid int, endLogid int, cWrite *clientPkg.Client) {
	var s clientPkg.Status
	for i := startLogid; i < endLogid; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		s = cWrite.Write(newlog)
		if s != clientPkg.Ok {
			t.Errorf("write test fail")
		}		
	}
}

func FindLeader(t *testing.T, nodeCollections []* nodes.Node) (i int) {
	for i, node := range nodeCollections {
		if node.State == nodes.Leader {
			return i
		}
	}
	t.Errorf("系统目前没有leader")
	return 0
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

func CheckNoLeader(t *testing.T, nodeCollections []* nodes.Node) {
	cnt := 0
	for _, node := range nodeCollections {
		if node.State == nodes.Leader {
			cnt++
		}
	}
	if cnt != 0 {
		t.Errorf("实际有%d个leader(!=0)", cnt)
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

func CheckLogNum(t *testing.T, node *nodes.Node, targetnum int) {
	if len(node.Log) != targetnum {
		t.Errorf("[%s]实际logNum=%d (!=%d)", node.SelfId, len(node.Log), targetnum)
	}
}
