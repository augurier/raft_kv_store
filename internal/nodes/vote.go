package nodes

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

type RequestVoteArgs struct {
	Term         int    // 候选人的当前任期
	CandidateId  string // 候选人 ID
	LastLogIndex int    // 候选人最后一条日志的索引
	LastLogTerm  int    // 候选人最后一条日志的任期
}

type RequestVoteReply struct {
    Term        int  // 当前节点的最新任期
    VoteGranted bool // 是否同意投票
}

func (n *Node) startElection() {
	n.mu.Lock()
    defer n.mu.Unlock()
    // 增加当前任期，转换为 Candidate
    n.currTerm++
    n.state = Candidate
    n.votedFor = n.selfId // 自己投自己
	n.storage.SetTermAndVote(n.currTerm, n.votedFor)

    log.Sugar().Infof("[%s] 开始选举，当前任期: %d", n.selfId, n.currTerm)

    // 重新设置选举超时，防止重复选举
    n.resetElectionTimer()

    // 构造 RequestVote 请求
	var lastLogIndex int
	var lastLogTerm int

	if len(n.log) == 0 {
		lastLogIndex = 0
		lastLogTerm = 0 // 论文中定义，空日志时 Term 设为 0
	} else {
		lastLogIndex = len(n.log) - 1
		lastLogTerm = n.log[lastLogIndex].Term
	}
    args := RequestVoteArgs{
        Term:        n.currTerm,
        CandidateId: n.selfId,
        LastLogIndex: lastLogIndex,
        LastLogTerm:  lastLogTerm,
    }

    // 并行向其他节点发送请求投票
    var mu sync.Mutex
    totalNodes := len(n.nodes)
    grantedVotes := 1 // 自己的票

    for _, peerId := range n.nodes {
		go func(peerId string) {
			reply := RequestVoteReply{}
			if n.sendRequestVote(peerId, &args, &reply) {
				mu.Lock()
				
				if reply.Term > n.currTerm {
					// 发现更高任期，回退为 Follower
					log.Sugar().Infof("[%s] 发现更高的 Term (%d)，回退为 Follower", n.selfId, reply.Term)
					n.currTerm = reply.Term
					n.state = Follower
					n.votedFor = ""
					n.storage.SetTermAndVote(n.currTerm, n.votedFor)
					n.resetElectionTimer()
					mu.Unlock()
					return
				}
	
				if reply.VoteGranted {
					grantedVotes++
				}
	
				if grantedVotes == totalNodes / 2 + 1 {
					n.state = Leader
					log.Sugar().Infof("[%s] 当选 Leader!", n.selfId)
					n.initLeaderState()
				}
	
				mu.Unlock()
			}
		}(peerId)
	}
	
	// 等待选举结果
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	if n.state == Candidate {
		log.Sugar().Infof("[%s] 选举超时，重新发起选举", n.selfId)
		// n.state = Follower 这里不修改，如果appendentries收到term合理的心跳，再变回follower
		n.resetElectionTimer()
	}
	mu.Unlock()
}

func (node *Node) sendRequestVote(peerId string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	log.Sugar().Infof("[%s] 请求 [%s] 投票", node.selfId, peerId)
	client, err := node.transport.DialHTTPWithTimeout("tcp", peerId)
	if err != nil {
		log.Error(node.selfId + "dialing [" + peerId + "] fail: ", zap.Error(err))
		return false
	}

	defer func(client ClientInterface) {
		err := client.Close()
		if err != nil {
			log.Error("client close err: ", zap.Error(err))
		}
	}(client)

	callErr := node.transport.CallWithTimeout(client, "Node.RequestVote", args, reply) // RPC
	if callErr != nil {
		log.Error(node.selfId + "calling [" + peerId + "] fail: ", zap.Error(callErr))
	}
    return callErr == nil
}

func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
    defer n.mu.Unlock()
    // 如果候选人的任期小于当前任期，则拒绝投票
    if args.Term < n.currTerm {
        reply.Term = n.currTerm
        reply.VoteGranted = false
        return nil
    }

    // 如果请求的 Term 更高，则更新当前 Term 并回退为 Follower
    if args.Term > n.currTerm {
        n.currTerm = args.Term
        n.state = Follower
        n.votedFor = ""
        n.resetElectionTimer() // 重新设置选举超时
    }

    // 检查是否已经投过票，且是否投给了同一个候选人
    if n.votedFor == "" || n.votedFor == args.CandidateId {
        // 检查日志是否足够新
		var lastLogIndex int
		var lastLogTerm int
	
		if len(n.log) == 0 {
			lastLogIndex = -1
			lastLogTerm = 0
		} else {
			lastLogIndex = len(n.log) - 1
			lastLogTerm = n.log[lastLogIndex].Term
		}

        if args.LastLogTerm > lastLogTerm || 
           (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
            // 够新就投票给候选人
            n.votedFor = args.CandidateId
			log.Sugar().Infof("在term(%s), [%s]投票给[%s]", strconv.Itoa(n.currTerm), n.selfId, n.votedFor)
            reply.VoteGranted = true
            n.resetElectionTimer()
        } else {
            reply.VoteGranted = false
        }
    } else {
        reply.VoteGranted = false
    }

	n.storage.SetTermAndVote(n.currTerm, n.votedFor)
    reply.Term = n.currTerm
	return nil
}

// follower 500-1000ms内没收到appendentries心跳，就变成candidate发起选举
func (node *Node) resetElectionTimer() {
	if node.electionTimer == nil {
		node.electionTimer = time.NewTimer(time.Duration(500+rand.Intn(500)) * time.Millisecond)
		go func() {
			for {
				<-node.electionTimer.C
				node.startElection()
			}
		}()
	} else {
		node.electionTimer.Stop()
		node.electionTimer.Reset(time.Duration(500+rand.Intn(500)) * time.Millisecond)
	}
}