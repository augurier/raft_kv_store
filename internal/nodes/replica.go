package nodes

import (
	"simple-kv-store/internal/logprovider"
	"sort"
	"strconv"
	"sync"

	"go.uber.org/zap"
)

type AppendEntriesArg struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Mu sync.Mutex
	Term int
	Success bool
}

// leader收到新内容要广播，以及心跳广播（同步自己的log)
func (node *Node) BroadCastKV() {
	log.Sugar().Infof("leader[%s]广播消息", node.SelfId)
	defer logprovider.DebugTraceback("broadcast")
	failCount := 0
	// 这里增加一个锁，防止并发修改成功计数
	var failMutex sync.Mutex
	// 遍历所有节点
	for _, id := range node.Nodes {
		go func(id string) {
			defer logprovider.DebugTraceback("send")
			node.sendKV(id, &failCount, &failMutex)
		}(id)
	}
}

func (node *Node) sendKV(peerId string, failCount *int, failMutex *sync.Mutex) {
	node.Mu.Lock()
	selfId := node.SelfId
	node.Mu.Unlock()

	client, err := node.Transport.DialHTTPWithTimeout("tcp", selfId, peerId)
	if err != nil {
		node.Mu.Lock()
		log.Error("[" + node.SelfId + "]dialling [" + peerId + "] fail: ", zap.Error(err))
		failMutex.Lock()
		*failCount++
		if *failCount == len(node.Nodes) / 2 + 1 { // 无法联系超过半数：自己有问题，降级
			node.LeaderId = ""
			node.State = Follower
			node.ResetElectionTimer()
		}
		failMutex.Unlock()
		node.Mu.Unlock()
		return
	}

	defer func(client ClientInterface) {
		err := client.Close()
		if err != nil {
			log.Error("client close err: ", zap.Error(err))
		}
	}(client)

	node.Mu.Lock()

	NextIndex := node.NextIndex[peerId]
	// log.Info("NextIndex " + strconv.Itoa(NextIndex))
	for {
		if NextIndex < 0 {
			log.Fatal("assert >= 0 here")
		}
	

		sendEntries := node.Log[NextIndex:]
		arg := AppendEntriesArg{
			Term: node.CurrTerm,
			PrevLogIndex: NextIndex - 1,
			Entries: sendEntries,
			LeaderCommit: node.CommitIndex,
			LeaderId: node.SelfId,
		}
		if arg.PrevLogIndex >= 0 {
			arg.PrevLogTerm = node.Log[arg.PrevLogIndex].Term
		}
		// 记录关键数据后解锁
		currTerm := node.CurrTerm
		currState := node.State
		MaxLogId := node.MaxLogId

		var appendReply AppendEntriesReply
		appendReply.Success = false			
		node.Mu.Unlock()

		callErr := node.Transport.CallWithTimeout(client, "Node.AppendEntries", &arg, &appendReply) // RPC

		node.Mu.Lock()
		if node.CurrTerm != currTerm || node.MaxLogId != MaxLogId || node.State != currState {
			node.Mu.Unlock()
			return
		}

		if callErr != nil {
			log.Error("[" + node.SelfId + "]calling [" + peerId + "] fail: ", zap.Error(callErr))
			failMutex.Lock()
			*failCount++
			if *failCount == len(node.Nodes) / 2 + 1 { // 无法联系超过半数：自己有问题，降级
				log.Info("term=" + strconv.Itoa(node.CurrTerm) + "的Leader[" + node.SelfId + "]无法联系到半数节点, 降级为 Follower")
				node.LeaderId = ""
				node.State = Follower
				node.ResetElectionTimer()
			}
			failMutex.Unlock()
			node.Mu.Unlock()
			return
		}

		appendReply.Mu.Lock()
		if appendReply.Term != node.CurrTerm {
			log.Sugar().Infof("term=%s的leader[%s]因为[%s]收到更高的term=%s, 转换为follower", 
					strconv.Itoa(node.CurrTerm), node.SelfId, peerId, strconv.Itoa(appendReply.Term))

			node.LeaderId = ""
			node.CurrTerm = appendReply.Term
			node.State = Follower
			node.VotedFor = ""
			node.Storage.SetTermAndVote(node.CurrTerm, node.VotedFor)
			node.ResetElectionTimer()
			appendReply.Mu.Unlock()
			node.Mu.Unlock()
			return
		}

		if appendReply.Success {
			appendReply.Mu.Unlock()
			break
		}

		appendReply.Mu.Unlock()
		NextIndex-- // 失败往前传一格
	}
	
	// 不变成follower情况下
	node.NextIndex[peerId] = node.MaxLogId + 1
	node.MatchIndex[peerId] = node.MaxLogId
	node.updateCommitIndex()
	node.Mu.Unlock()
}

func (node *Node) updateCommitIndex() {
	if node.Mu.TryLock() {
		log.Fatal("这里要保证有锁")
	}
	if node.IsFinish {
		return
	}

    totalNodes := len(node.Nodes)

    // 收集所有 MatchIndex 并排序
    MatchIndexes := make([]int, 0, totalNodes)
    for _, index := range node.MatchIndex {
        MatchIndexes = append(MatchIndexes, index)
    }
    sort.Ints(MatchIndexes) // 排序

    // 计算多数派 CommitIndex
    majorityIndex := MatchIndexes[totalNodes/2] // 取 N/2 位置上的索引（多数派）

    // 确保这个索引的日志条目属于当前 term，防止提交旧 term 的日志
    if majorityIndex > node.CommitIndex && majorityIndex < len(node.Log) && node.Log[majorityIndex].Term == node.CurrTerm {
        node.CommitIndex = majorityIndex
        log.Info("Leader[" + node.SelfId + "]更新 CommitIndex: " + strconv.Itoa(majorityIndex))
        
        // 应用日志到状态机
        node.applyCommittedLogs()
    }
}

// 应用日志到状态机
func (node *Node) applyCommittedLogs() {
    for node.LastApplied < node.CommitIndex {
        node.LastApplied++
        logEntry := node.Log[node.LastApplied]
        log.Sugar().Infof("[%s]应用日志到状态机: " + logEntry.print(), node.SelfId)
        err := node.Db.Put([]byte(logEntry.LogE.Key), []byte(logEntry.LogE.Value), nil)
		if err != nil {
			log.Error(node.SelfId + "应用状态机失败: ", zap.Error(err))
		}
    }
}

// RPC call
func (node *Node) AppendEntries(arg *AppendEntriesArg, reply *AppendEntriesReply) error {
	defer logprovider.DebugTraceback("append")

    node.Mu.Lock()	
    defer node.Mu.Unlock()	
	log.Sugar().Infof("[%s]在term=%d收到[%s]的AppendEntries", node.SelfId, node.CurrTerm, arg.LeaderId)


    // 如果 term 过期，拒绝接受日志
    if node.CurrTerm > arg.Term {
		reply.Mu.Lock()
		reply.Term = node.CurrTerm
		reply.Success = false
		reply.Mu.Unlock()
        return nil
    }
	
	node.LeaderId = arg.LeaderId  // 记录Leader
	
	// 如果term比自己高，或自己不是follower但收到相同term的心跳
	if node.CurrTerm < arg.Term || node.State != Follower {
		log.Sugar().Infof("[%s]发现更高 term(%s)", node.SelfId, strconv.Itoa(arg.Term))
		node.CurrTerm = arg.Term
		node.State = Follower
		node.VotedFor = ""
		// node.storage.SetTermAndVote(node.CurrTerm, node.VotedFor)
    }
	node.Storage.SetTermAndVote(node.CurrTerm, node.VotedFor)

    // 检查 prevLogIndex 是否有效
    if arg.PrevLogIndex >= len(node.Log) || (arg.PrevLogIndex >= 0 && node.Log[arg.PrevLogIndex].Term != arg.PrevLogTerm) {
		reply.Mu.Lock()
		reply.Term = node.CurrTerm
		reply.Success = false
		reply.Mu.Unlock()
        return nil
    }

    // 处理日志冲突（如果存在不同 term，则截断日志）
	idx := arg.PrevLogIndex + 1
	for i := idx; i < len(node.Log) && i-idx < len(arg.Entries); i++ {
		if node.Log[i].Term != arg.Entries[i-idx].Term {
			node.Log = node.Log[:idx]
			break
		}
	}
	// log.Info(strconv.Itoa(idx) + strconv.Itoa(len(node.Log)))

    // 追加新的日志条目
    for _, raftLogEntry := range arg.Entries {
		log.Sugar().Infof("[%s]写入：" + raftLogEntry.print(), node.SelfId)
        if idx < len(node.Log) {
            node.Log[idx] = raftLogEntry
        } else {
            node.Log = append(node.Log, raftLogEntry)
        }
        idx++
    }

	// 暴力持久化
	node.Storage.WriteLog(node.Log)

    // 更新 MaxLogId
    node.MaxLogId = len(node.Log) - 1

    // 更新 CommitIndex
    if arg.LeaderCommit < node.MaxLogId {
		node.CommitIndex = arg.LeaderCommit
	} else {
		node.CommitIndex = node.MaxLogId
	}

    // 提交已提交的日志
    node.applyCommittedLogs()

	// 在成功接受日志或心跳后，重置选举超时
	node.ResetElectionTimer()
	reply.Mu.Lock()
	reply.Term = node.CurrTerm
	reply.Success = true
	reply.Mu.Unlock()
    return nil
}