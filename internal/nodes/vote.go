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

func (n *Node) StartElection() {
	n.Mu.Lock()
    defer n.Mu.Unlock()
    // 增加当前任期，转换为 Candidate
    n.CurrTerm++
    n.State = Candidate
    n.VotedFor = n.SelfId // 自己投自己
	n.Storage.SetTermAndVote(n.CurrTerm, n.VotedFor)

    log.Sugar().Infof("[%s] 开始选举，当前任期: %d", n.SelfId, n.CurrTerm)

    // 重新设置选举超时，防止重复选举
    n.ResetElectionTimer()

    // 构造 RequestVote 请求
	var lastLogIndex int
	var lastLogTerm int

	if len(n.Log) == 0 {
		lastLogIndex = 0
		lastLogTerm = 0 // 论文中定义，空日志时 Term 设为 0
	} else {
		lastLogIndex = len(n.Log) - 1
		lastLogTerm = n.Log[lastLogIndex].Term
	}
    args := RequestVoteArgs{
        Term:        n.CurrTerm,
        CandidateId: n.SelfId,
        LastLogIndex: lastLogIndex,
        LastLogTerm:  lastLogTerm,
    }

    // 并行向其他节点发送请求投票
    var Mu sync.Mutex
    totalNodes := len(n.Nodes)
    grantedVotes := 1 // 自己的票

    for _, peerId := range n.Nodes {
		go func(peerId string) {
			reply := RequestVoteReply{}
			if n.sendRequestVote(peerId, &args, &reply) {
				Mu.Lock()
				
				if reply.Term > n.CurrTerm {
					// 发现更高任期，回退为 Follower
					log.Sugar().Infof("[%s] 发现更高的 Term (%d)，回退为 Follower", n.SelfId, reply.Term)
					n.CurrTerm = reply.Term
					n.State = Follower
					n.VotedFor = ""
					n.Storage.SetTermAndVote(n.CurrTerm, n.VotedFor)
					n.ResetElectionTimer()
					Mu.Unlock()
					return
				}
	
				if reply.VoteGranted {
					grantedVotes++
				}
	
				if grantedVotes == totalNodes / 2 + 1 {
					n.State = Leader
					log.Sugar().Infof("[%s] 当选 Leader!", n.SelfId)
					n.initLeaderState()
				}
	
				Mu.Unlock()
			}
		}(peerId)
	}
	
	// 等待选举结果
	time.Sleep(300 * time.Millisecond)
	Mu.Lock()
	if n.State == Candidate {
		log.Sugar().Infof("[%s] 选举超时，等待后将重新发起选举", n.SelfId)
		// n.State = Follower 这里不修改，如果appendentries收到term合理的心跳，再变回follower
		n.ResetElectionTimer()
	}
	Mu.Unlock()
}

func (node *Node) sendRequestVote(peerId string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	log.Sugar().Infof("[%s] 请求 [%s] 投票", node.SelfId, peerId)
	client, err := node.Transport.DialHTTPWithTimeout("tcp", node.SelfId, peerId)
	if err != nil {
		log.Error("[" + node.SelfId + "]dialing [" + peerId + "] fail: ", zap.Error(err))
		return false
	}

	defer func(client ClientInterface) {
		err := client.Close()
		if err != nil {
			log.Error("client close err: ", zap.Error(err))
		}
	}(client)

	callErr := node.Transport.CallWithTimeout(client, "Node.RequestVote", args, reply) // RPC
	if callErr != nil {
		log.Error("[" + node.SelfId + "]calling [" + peerId + "] fail: ", zap.Error(callErr))
	}
    return callErr == nil
}

func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.Mu.Lock()
    defer n.Mu.Unlock()
    // 如果候选人的任期小于当前任期，则拒绝投票
    if args.Term < n.CurrTerm {
        reply.Term = n.CurrTerm
        reply.VoteGranted = false
        return nil
    }

    // 如果请求的 Term 更高，则更新当前 Term 并回退为 Follower
    if args.Term > n.CurrTerm {
        n.CurrTerm = args.Term
        n.State = Follower
        n.VotedFor = ""
        n.ResetElectionTimer() // 重新设置选举超时
    }

    // 检查是否已经投过票，且是否投给了同一个候选人
    if n.VotedFor == "" || n.VotedFor == args.CandidateId {
        // 检查日志是否足够新
		var lastLogIndex int
		var lastLogTerm int
	
		if len(n.Log) == 0 {
			lastLogIndex = -1
			lastLogTerm = 0
		} else {
			lastLogIndex = len(n.Log) - 1
			lastLogTerm = n.Log[lastLogIndex].Term
		}

        if args.LastLogTerm > lastLogTerm || 
           (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
            // 够新就投票给候选人
            n.VotedFor = args.CandidateId
			log.Sugar().Infof("在term(%s), [%s]投票给[%s]", strconv.Itoa(n.CurrTerm), n.SelfId, n.VotedFor)
            reply.VoteGranted = true
            n.ResetElectionTimer()
        } else {
            reply.VoteGranted = false
        }
    } else {
        reply.VoteGranted = false
    }

	n.Storage.SetTermAndVote(n.CurrTerm, n.VotedFor)
    reply.Term = n.CurrTerm
	return nil
}

// follower 150-300ms内没收到appendentries心跳，就变成candidate发起选举
func (node *Node) ResetElectionTimer() {
	if node.ElectionTimer == nil {
		node.ElectionTimer = time.NewTimer(node.RTTable.GetElectionTimeout())
		go func() {
			for {
				<-node.ElectionTimer.C
				node.StartElection()
			}
		}()
	} else {
		node.ElectionTimer.Stop()
		node.ElectionTimer.Reset(time.Duration(500+rand.Intn(500)) * time.Millisecond)
	}
}