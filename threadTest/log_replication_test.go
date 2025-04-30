package threadTest

import (
	"simple-kv-store/internal/nodes"
	"strconv"
	"testing"
	"time"
)

func TestNormalReplication(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections)

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

	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		SendKvCall(&nodes.LogEntryCall{LogE: newlog}, nodeCollections[0])	
	}

	time.Sleep(time.Second)
	for i := 0; i < n; i++ {
		CheckLogNum(t, nodeCollections[i], 10)
	}
}

func TestParallelReplication(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections)

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

	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		go SendKvCall(&nodes.LogEntryCall{LogE: newlog}, nodeCollections[0])
		go nodeCollections[0].BroadCastKV()
	}

	time.Sleep(time.Second)
	for i := 0; i < n; i++ {
		CheckLogNum(t, nodeCollections[i], 10)
	}
}

func TestFollowerLagging(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections)

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
	close(quitCollections[1])
	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		go SendKvCall(&nodes.LogEntryCall{LogE: newlog}, nodeCollections[0])
	}

	node, q := ExecuteStaticNodeI("2", true, peerIds, threadTransport)
	quitCollections[1] = q
	nodeCollections[1] = node
	nodeCollections[1].State = nodes.Follower
	StopElectionReset(nodeCollections[1:2])
	nodeCollections[0].BroadCastKV()

	time.Sleep(time.Second)
	for i := 0; i < n; i++ {
		CheckLogNum(t, nodeCollections[i], 10)
	}
}

func TestFailLogAppendRpc(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	ctx := nodes.NewCtx()
	threadTransport := nodes.NewThreadTransport(ctx)
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections)

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
	
	for i := 0; i < n; i++ {
		ctx.SetBehavior("1", nodeCollections[i].SelfId, nodes.FailRpc, 0, 0)
	}
	
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		go SendKvCall(&nodes.LogEntryCall{LogE: newlog}, nodeCollections[0])
	}

	time.Sleep(time.Second)
	for i := 1; i < n; i++ {
		CheckLogNum(t, nodeCollections[i], 0)
	}
}

func TestRepeatLogAppendRpc(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	ctx := nodes.NewCtx()
	threadTransport := nodes.NewThreadTransport(ctx)
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections)

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
	
	for i := 0; i < n; i++ {
		ctx.SetBehavior("1", nodeCollections[i].SelfId, nodes.RetryRpc, 0, 2)
	}
	
	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		go SendKvCall(&nodes.LogEntryCall{LogE: newlog}, nodeCollections[0])
	}

	time.Sleep(time.Second)
	for i := 0; i < n; i++ {
		CheckLogNum(t, nodeCollections[i], 10)
	}
}

func TestDelayLogAppendRpc(t *testing.T) {
	n := 5
	var peerIds []string
	for i := 0; i < n; i++ {
		peerIds = append(peerIds, strconv.Itoa(i + 1))
	}

	// 结点启动
	var quitCollections []chan struct{}
	var nodeCollections []*nodes.Node
	ctx := nodes.NewCtx()
	threadTransport := nodes.NewThreadTransport(ctx)
	for i := 0; i < n; i++ {
		n, quitChan := ExecuteStaticNodeI(strconv.Itoa(i + 1), false, peerIds, threadTransport)
		quitCollections = append(quitCollections, quitChan)
		nodeCollections = append(nodeCollections, n)
	}
	StopElectionReset(nodeCollections)

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
	
	for i := 0; i < n; i++ {
		ctx.SetBehavior("1", nodeCollections[i].SelfId, nodes.DelayRpc, time.Second, 0)
	}
	
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		go SendKvCall(&nodes.LogEntryCall{LogE: newlog}, nodeCollections[0])
	}

	time.Sleep(time.Millisecond * 100)

	for i := 0; i < n; i++ {
		ctx.SetBehavior("1", nodeCollections[i].SelfId, nodes.NormalRpc, 0, 0)
	}
	for i := 5; i < 10; i++ {
		key := strconv.Itoa(i)
		newlog := nodes.LogEntry{Key: key, Value: "hello"}
		go SendKvCall(&nodes.LogEntryCall{LogE: newlog}, nodeCollections[0])
	}

	time.Sleep(time.Second * 2)
	for i := 0; i < n; i++ {
		CheckLogNum(t, nodeCollections[i], 10)
	}
}
