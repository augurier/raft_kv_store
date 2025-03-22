package nodes

import (
	"math/rand"
	"net/rpc"
	"sort"
	"strconv"
	"time"

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
	Term int
	Success bool
}

// leader收到新内容要广播，以及心跳广播（同步自己的log)
func (node *Node) BroadCastKV(callMode CallMode) {
	// 遍历所有节点
	for id := range node.nodes {
		go func(id string, kv CallMode) {
			node.sendKV(id, callMode)
		}(id, callMode)
	}
}

func (node *Node) sendKV(id string, callMode CallMode) {

	switch callMode {
	case Fail:
		log.Info("模拟发送失败")
		// 这么写向所有的node发送都失败，也可以随机数确定是否失败
	case Delay:
		log.Info("模拟发送延迟")
		// 随机延迟0-5ms
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)))
	default:
	}

	client, err := DialHTTPWithTimeout("tcp", node.nodes[id].address)
	if err != nil {
		log.Error("dialing: ", zap.Error(err))
		return
	}

	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Error("client close err: ", zap.Error(err))
		}
	}(client)

	node.mu.Lock()
    defer node.mu.Unlock()

	var appendReply AppendEntriesReply
	appendReply.Success = false
	nextIndex := node.nextIndex[id]
	// log.Info("nextindex " + strconv.Itoa(nextIndex))
	for (!appendReply.Success) {
		if nextIndex < 0 {
			log.Fatal("assert >= 0 here")
		}
		sendEntries := node.log[nextIndex:]
		arg := AppendEntriesArg{
			Term: node.currTerm,
			PrevLogIndex: nextIndex - 1,
			Entries: sendEntries,
			LeaderCommit: node.commitIndex,
			LeaderId: node.selfId,
		}
		if arg.PrevLogIndex >= 0 {
			arg.PrevLogTerm = node.log[arg.PrevLogIndex].Term
		}
		callErr := CallWithTimeout(client, "Node.AppendEntries", &arg, &appendReply) // RPC
		if callErr != nil {
			log.Error("dialing node_"+ id +"fail: ", zap.Error(callErr))
		}

		if appendReply.Term != node.currTerm {
			log.Info("Leader[" + node.selfId + "]收到更高的 term=" + strconv.Itoa(appendReply.Term) + "，转换为 Follower")
			node.currTerm = appendReply.Term
			node.state = Follower
			node.votedFor = ""
			node.storage.SetTermAndVote(node.currTerm, node.votedFor)
			node.resetElectionTimer()
			return
		}
		nextIndex-- // 失败往前传一格
	}
	
	// 不变成follower情况下
	node.nextIndex[id] = node.maxLogId + 1
	node.matchIndex[id] = node.maxLogId
	node.updateCommitIndex()
}

func (node *Node) updateCommitIndex() {
    totalNodes := len(node.nodes)

    // 收集所有 matchIndex 并排序
    matchIndexes := make([]int, 0, totalNodes)
    for _, index := range node.matchIndex {
        matchIndexes = append(matchIndexes, index)
    }
    sort.Ints(matchIndexes) // 排序

    // 计算多数派 commitIndex
    majorityIndex := matchIndexes[totalNodes/2] // 取 N/2 位置上的索引（多数派）

    // 确保这个索引的日志条目属于当前 term，防止提交旧 term 的日志
    if majorityIndex > node.commitIndex && majorityIndex < len(node.log) && node.log[majorityIndex].Term == node.currTerm {
        node.commitIndex = majorityIndex
        log.Info("Leader[" + node.selfId + "]更新 commitIndex: " + strconv.Itoa(majorityIndex))
        
        // 应用日志到状态机
        node.applyCommittedLogs()
    }
}

// 应用日志到状态机
func (node *Node) applyCommittedLogs() {
    for node.lastApplied < node.commitIndex {
        node.lastApplied++
        logEntry := node.log[node.lastApplied]
        log.Sugar().Infof("[%s]应用日志到状态机: " + logEntry.print(), node.selfId)
        err := node.db.Put([]byte(logEntry.LogE.Key), []byte(logEntry.LogE.Value), nil)
		if err != nil {
			log.Error(node.selfId + "应用状态机失败: ", zap.Error(err))
		}
    }
}

// RPC call
func (node *Node) AppendEntries(arg *AppendEntriesArg, reply *AppendEntriesReply) error {
	// start := time.Now()
	// defer func() {
	// 	log.Sugar().Infof("AppendEntries 处理时间: %v", time.Since(start))
	// }()
    node.mu.Lock()
    defer node.mu.Unlock()

    // 如果 term 过期，拒绝接受日志
    if node.currTerm > arg.Term {
        *reply = AppendEntriesReply{node.currTerm, false}
        return nil
    }
	
	node.leaderId = arg.LeaderId  // 记录Leader
	
	// 如果term比自己高，或自己不是follower但收到相同term的心跳
	if node.currTerm < arg.Term || node.state != Follower {
		log.Sugar().Infof("[%s]发现更高 term(%s)", node.selfId, strconv.Itoa(arg.Term))
		node.currTerm = arg.Term
		node.state = Follower
		node.votedFor = ""
		node.storage.SetTermAndVote(node.currTerm, node.votedFor)
    }
	node.storage.SetTermAndVote(node.currTerm, node.votedFor)

    // 检查 prevLogIndex 是否有效
    if arg.PrevLogIndex >= len(node.log) || (arg.PrevLogIndex >= 0 && node.log[arg.PrevLogIndex].Term != arg.PrevLogTerm) {
        *reply = AppendEntriesReply{node.currTerm, false}
        return nil
    }

    // 处理日志冲突（如果存在不同 term，则截断日志）
	idx := arg.PrevLogIndex + 1
	for i := idx; i < len(node.log) && i-idx < len(arg.Entries); i++ {
		if node.log[i].Term != arg.Entries[i-idx].Term {
			node.log = node.log[:idx]
			break
		}
	}
	// log.Info(strconv.Itoa(idx) + strconv.Itoa(len(node.log)))

    // 追加新的日志条目
    for _, raftLogEntry := range arg.Entries {
		log.Sugar().Infof("[%s]写入：" + raftLogEntry.print(), node.selfId)
        if idx < len(node.log) {
            node.log[idx] = raftLogEntry
        } else {
            node.log = append(node.log, raftLogEntry)
        }
        idx++
    }

	// 暴力持久化
	node.storage.WriteLog(node.log)

    // 更新 maxLogId
    node.maxLogId = len(node.log) - 1

    // 更新 commitIndex
    if arg.LeaderCommit < node.maxLogId {
		node.commitIndex = arg.LeaderCommit
	} else {
		node.commitIndex = node.maxLogId
	}

    // 提交已提交的日志
    node.applyCommittedLogs()

	// 在成功接受日志或心跳后，重置选举超时
	node.resetElectionTimer()
    *reply = AppendEntriesReply{node.currTerm, true}
    return nil
}