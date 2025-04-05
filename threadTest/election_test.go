package threadTest

import (
	"simple-kv-store/internal/nodes"
	"strconv"
	"testing"
	"time"
)

func TestInitElection(t *testing.T) {
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
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
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
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
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
	threadTransport := nodes.NewThreadTransport(nodes.NewCtx())
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

func TestRepeatVoteRpc(t *testing.T) {
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

	ctx.SetBehavior("1", "2", nodes.RetryRpc, 0, 2)
	nodeCollections[0].StartElection()
	time.Sleep(time.Second)

	CheckOneLeader(t, nodeCollections)
	CheckIsLeader(t, nodeCollections[0])
	CheckTerm(t, nodeCollections[0], 2)

	for i := 0; i < n; i++ {
		ctx.SetBehavior("1", nodeCollections[i].SelfId, nodes.RetryRpc, 0, 2)
		ctx.SetBehavior("2", nodeCollections[i].SelfId, nodes.RetryRpc, 0, 2)
	}

	go nodeCollections[0].StartElection()
	go nodeCollections[1].StartElection()
	time.Sleep(time.Second)

	CheckOneLeader(t, nodeCollections)
	CheckTerm(t, nodeCollections[0], 3)
}

func TestFailVoteRpc(t *testing.T) {
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

	ctx.SetBehavior("1", "2", nodes.FailRpc, 0, 0)
	nodeCollections[0].StartElection()
	time.Sleep(time.Second)

	CheckOneLeader(t, nodeCollections)
	CheckIsLeader(t, nodeCollections[0])
	CheckTerm(t, nodeCollections[0], 2)

	ctx.SetBehavior("1", "3", nodes.FailRpc, 0, 0)
	ctx.SetBehavior("1", "4", nodes.FailRpc, 0, 0)
	nodeCollections[0].StartElection()
	time.Sleep(time.Second)

	CheckNoLeader(t, nodeCollections)
	CheckTerm(t, nodeCollections[0], 3)
}

func TestDelayVoteRpc(t *testing.T) {
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
	StopElectionReset(nodeCollections, quitCollections)

	// 通知所有node结束
	defer func(){
		for _, quitChan := range quitCollections {
			close(quitChan)
		}
	}()

	for i := 0; i < n; i++ {
		nodeCollections[i].State = nodes.Follower
		ctx.SetBehavior("1", nodeCollections[i].SelfId, nodes.DelayRpc, time.Second, 0)
	}

	nodeCollections[0].StartElection()
	time.Sleep(2 * time.Second)

	CheckNoLeader(t, nodeCollections)
	for i := 0; i < n; i++ {
		CheckTerm(t, nodeCollections[i], 2)
	}

	for i := 0; i < n; i++ {
		nodeCollections[i].State = nodes.Follower
		ctx.SetBehavior("1", nodeCollections[i].SelfId, nodes.DelayRpc, 50 * time.Millisecond, 0)
	}

	nodeCollections[0].StartElection()
	time.Sleep(time.Second)

	CheckOneLeader(t, nodeCollections)
	for i := 0; i < n; i++ {
		CheckTerm(t, nodeCollections[i], 3)
	}
}
