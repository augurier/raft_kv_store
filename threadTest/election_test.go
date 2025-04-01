package threadTest

import (
	"fmt"
	"os"
	"simple-kv-store/internal/nodes"
	"strconv"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

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

func TestInitElection(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport()
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections, quitCollections)

	// 通知所有node结束
	defer func(){
		for _, quitChan := range quitCollections {
			close(quitChan)
		}
	}()

	for i := 0; i < n; i++ {
		nodeCollections[i].State = nodes.Follower
	}

	nodeCollections[0].StartElection()
	time.Sleep(time.Second)

	CheckOneLeader(t, nodeCollections)
	CheckIsLeader(t, nodeCollections[0])
	CheckTerm(t, nodeCollections[0], 2)
}

func TestRepeatElection(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport()
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections, quitCollections)

	// 通知所有node结束
	defer func(){
		for _, quitChan := range quitCollections {
			close(quitChan)
		}
	}()

	for i := 0; i < n; i++ {
		nodeCollections[i].State = nodes.Follower
	}

	go nodeCollections[0].StartElection()
	go nodeCollections[0].StartElection()
	time.Sleep(time.Second)

	CheckOneLeader(t, nodeCollections)
	CheckIsLeader(t, nodeCollections[0])
	CheckTerm(t, nodeCollections[0], 3)
}

func TestBelowHalfCandidateElection(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport()
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections, quitCollections)

	// 通知所有node结束
	defer func(){
		for _, quitChan := range quitCollections {
			close(quitChan)
		}
	}()

	for i := 0; i < n; i++ {
		nodeCollections[i].State = nodes.Follower
	}

	go nodeCollections[0].StartElection()
	go nodeCollections[1].StartElection()
	time.Sleep(time.Second)

	CheckOneLeader(t, nodeCollections)
	for i := 0; i < n; i++ {
		CheckTerm(t, nodeCollections[i], 2)
	}
}

func TestOverHalfCandidateElection(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport()
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections, quitCollections)

	// 通知所有node结束
	defer func(){
		for _, quitChan := range quitCollections {
			close(quitChan)
		}
	}()

	for i := 0; i < n; i++ {
		nodeCollections[i].State = nodes.Follower
	}

	go nodeCollections[0].StartElection()
	go nodeCollections[1].StartElection()
	go nodeCollections[2].StartElection()	
	time.Sleep(time.Second)

	CheckZeroOrOneLeader(t, nodeCollections)
	for i := 0; i < n; i++ {
		CheckTerm(t, nodeCollections[i], 2)
	}
}