package threadTest

import (
	"fmt"
	"os"
	clientPkg "simple-kv-store/internal/client"
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

	// 创建临时目录用于 leveldb
	dbPath, err := os.MkdirTemp("", "simple-kv-store-"+id+"-")
	if err != nil {
		panic(fmt.Sprintf("无法创建临时数据库目录: %s", err))
	}

	// 创建临时目录用于 storage
	storagePath, err := os.MkdirTemp("", "raft-storage-"+id+"-")
	if err != nil {
		panic(fmt.Sprintf("无法创建临时存储目录: %s", err))
	}

	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to open database: %s", err))
	}

	// 初始化 Raft 存储
	storage := nodes.NewRaftStorage(storagePath)

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

	db, err := leveldb.OpenFile("leveldb/simple-kv-store"+id, nil)
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

func StopElectionReset(nodeCollections []*nodes.Node) {
	for i := 0; i < len(nodeCollections); i++ {
		node := nodeCollections[i]
		go func(node *nodes.Node) {
			ticker := time.NewTicker(400 * time.Millisecond)
			defer ticker.Stop()

			for {
				<-ticker.C
				node.ResetElectionTimer() // 不主动触发选举
			}
		}(node)
	}
}

func SendKvCall(kvCall *nodes.LogEntryCall, node *nodes.Node) {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	node.MaxLogId++
	logId := node.MaxLogId
	rLogE := nodes.RaftLogEntry{LogE: kvCall.LogE, LogId: logId, Term: node.CurrTerm}
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

func FindLeader(t *testing.T, nodeCollections []*nodes.Node) (i int) {
	for i, node := range nodeCollections {
		if node.State == nodes.Leader {
			return i
		}
	}
	t.Errorf("系统目前没有leader")
	t.FailNow()
	return 0
}

func CheckOneLeader(t *testing.T, nodeCollections []*nodes.Node) {
	cnt := 0
	for _, node := range nodeCollections {
		node.Mu.Lock()
		if node.State == nodes.Leader {
			cnt++
		}
		node.Mu.Unlock()
	}
	if cnt != 1 {
		t.Errorf("实际有%d个leader(!=1)", cnt)
		t.FailNow()
	}
}

func CheckNoLeader(t *testing.T, nodeCollections []*nodes.Node) {
	cnt := 0
	for _, node := range nodeCollections {
		node.Mu.Lock()
		if node.State == nodes.Leader {
			cnt++
		}
		node.Mu.Unlock()
	}
	if cnt != 0 {
		t.Errorf("实际有%d个leader(!=0)", cnt)
		t.FailNow()
	}
}

func CheckZeroOrOneLeader(t *testing.T, nodeCollections []*nodes.Node) {
	cnt := 0
	for _, node := range nodeCollections {
		node.Mu.Lock()
		if node.State == nodes.Leader {
			cnt++
		}
		node.Mu.Unlock()
	}
	if cnt > 1 {
		errmsg := fmt.Sprintf("%d个节点中，实际有%d个leader(>1)", len(nodeCollections), cnt)
		WriteFailLog(nodeCollections[0].SelfId, errmsg)
		t.Error(errmsg)
		t.FailNow()
	}
}

func CheckIsLeader(t *testing.T, node *nodes.Node) {
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if node.State != nodes.Leader {
		t.Errorf("[%s]不是leader", node.SelfId)
		t.FailNow()
	}
}

func CheckTerm(t *testing.T, node *nodes.Node, targetTerm int) {
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if node.CurrTerm != targetTerm {
		t.Errorf("[%s]实际term=%d (!=%d)", node.SelfId, node.CurrTerm, targetTerm)
		t.FailNow()
	}
}

func CheckLogNum(t *testing.T, node *nodes.Node, targetnum int) {
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if len(node.Log) != targetnum {
		t.Errorf("[%s]实际logNum=%d (!=%d)", node.SelfId, len(node.Log), targetnum)
		t.FailNow()
	}
}

func CheckSameLog(t *testing.T, nodeCollections []*nodes.Node) {
	nodeCollections[0].Mu.Lock()
	defer nodeCollections[0].Mu.Unlock()
	standard_node := nodeCollections[0]
	for i, node := range nodeCollections {
		if i != 0 {
			node.Mu.Lock()
			if len(node.Log) != len(standard_node.Log) {
				errmsg := fmt.Sprintf("[%s]和[%s]日志数量不一致", nodeCollections[0].SelfId, node.SelfId)
				WriteFailLog(node.SelfId, errmsg)
				t.Error(errmsg)
				t.FailNow()
			}

			for idx, log := range node.Log {
				standard_log := standard_node.Log[idx]
				if log.Term != standard_log.Term ||
					log.LogE.Key != standard_log.LogE.Key ||
					log.LogE.Value != standard_log.LogE.Value {
					errmsg := fmt.Sprintf("[1]和[%s]日志id%d不一致", node.SelfId, idx)
					WriteFailLog(node.SelfId, errmsg)
					t.Error(errmsg)
					t.FailNow()
				}
			}
			node.Mu.Unlock()
		}
	}
}

func CheckLeaderInvariant(t *testing.T, nodeCollections []*nodes.Node) {
	leaderCnt := make(map[int]bool)
	for _, node := range nodeCollections {
		node.Mu.Lock()
		if node.State == nodes.Leader {
			if _, exist := leaderCnt[node.CurrTerm]; exist {
				errmsg := fmt.Sprintf("在%d有多个leader(%s)", node.CurrTerm, node.SelfId)
				WriteFailLog(node.SelfId, errmsg)
				t.Error(errmsg)
			} else {
				leaderCnt[node.CurrTerm] = true
			}
		}
		node.Mu.Unlock()
	}
}

func CheckLogInvariant(t *testing.T, nodeCollections []*nodes.Node) {
	nodeCollections[0].Mu.Lock()
	defer nodeCollections[0].Mu.Unlock()
	standard_node := nodeCollections[0]
	standard_len := len(standard_node.Log)
	for i, node := range nodeCollections {
		if i != 0 {
			node.Mu.Lock()
			len2 := len(node.Log)
			var shorti int
			if len2 < standard_len {
				shorti = len2
			} else {
				shorti = standard_len
			}
			if shorti == 0 {
				node.Mu.Unlock()
				continue
			}

			alreadySame := false
			for i := shorti - 1; i >= 0; i-- {
				standard_log := standard_node.Log[i]
				log := node.Log[i]
				if alreadySame {
					if log.Term != standard_log.Term ||
						log.LogE.Key != standard_log.LogE.Key ||
						log.LogE.Value != standard_log.LogE.Value {
						errmsg := fmt.Sprintf("[%s]和[%s]日志id%d不一致", standard_node.SelfId, node.SelfId, i)
						WriteFailLog(node.SelfId, errmsg)
						t.Error(errmsg)
						t.FailNow()
					}
				} else {
					if log.Term == standard_log.Term &&
						log.LogE.Key == standard_log.LogE.Key &&
						log.LogE.Value == standard_log.LogE.Value {
						alreadySame = true
					}
				}
			}
			node.Mu.Unlock()
		}
	}
}

func WriteFailLog(name string, errmsg string) {
	f, _ := os.Create(name + ".log")
	fmt.Fprint(f, errmsg)
	f.Close()
}
